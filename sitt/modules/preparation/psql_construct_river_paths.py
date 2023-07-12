# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

import logging

import geopandas as gpd
import networkx as nx
import pandas as pd
import yaml
from shapely import LineString, MultiLineString, Point, wkb, get_parts, get_coordinates, prepare, destroy_prepared, \
    line_merge
from sqlalchemy import create_engine, Table, Column, select, literal_column, func, text

from sitt import Configuration, Context
from sitt.modules.preparation import PSQLBase

logger = logging.getLogger()


class PsqlConstructRiverPaths(PSQLBase):
    """Construct river paths from harbor hubs. This is a pretty complicated module, taking approximate median paths
     through rivers and creating connected paths and hubs from them."""

    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', rivers_table_name: str = 'topology.recrivers',
                 rivers_geom_col: str = 'geom', rivers_index_col: str = 'id', river_coerce_float: bool = True,
                 rivers_hub_a_id: str = 'hubaid', rivers_hub_b_id: str = 'hubbid',
                 hubs_table_name: str = 'topology.rechubs', hubs_geom_col: str = 'geom',
                 hubs_index_col: str = 'id', hubs_coerce_float: bool = True, hubs_harbor: str = 'harbor',
                 water_body_table_name: str = 'topology.water_body', water_body_index_col: str = 'id',
                 water_body_geom: str = 'geom', water_lines_table_name: str = 'topology.water_lines',
                 water_lines_geom: str = 'geom', crs_no: str = 4326, connection: str | None = None):
        # connection data - should be set/overwritten by config
        super().__init__(server, port, db, user, password, connection)
        self.water_body_table_name: str = water_body_table_name
        self.water_body_index_col: str = water_body_index_col
        self.water_body_geom: str = water_body_geom
        self.water_lines_table_name: str = water_lines_table_name
        self.water_lines_geom: str = water_lines_geom
        self.rivers_table_name: str = rivers_table_name
        self.rivers_geom_col: str = rivers_geom_col
        self.rivers_index_col: str = rivers_index_col
        self.rivers_coerce_float: bool = river_coerce_float
        self.rivers_hub_a_id: str = rivers_hub_a_id
        self.rivers_hub_b_id: str = rivers_hub_b_id
        self.hubs_table_name: str = hubs_table_name
        self.hubs_geom_col: str = hubs_geom_col
        self.hubs_index_col: str = hubs_index_col
        self.hubs_coerce_float: bool = hubs_coerce_float
        self.hubs_harbor: str = hubs_harbor
        self.crs_no: str = crs_no

    def run(self, config: Configuration, context: Context) -> Context:
        if self.skip:
            logger.info("Skipping PsqlConstructRiverPaths due to setting")
            return context

        logger.info(
            "Constructing river paths PostgreSQL: " + self._create_connection_string(for_printing=True))

        # create connection string and connect to db
        db_string: str = self._create_connection_string()
        self.conn = create_engine(db_string).connect()

        # get all the harbor hubs and create groups
        groups = self._get_harbor_groups()

        # construct river paths
        logger.info("Constructing paths for " + str(len(groups)) + " river(s)")
        paths = self._construct_river_paths(groups)

        # create GeoPandas data frame for river paths and add to context
        self._convert_river_paths_to_geopandas(paths, context)

        # close connection
        self.conn.close()

        return context

    def _get_harbor_groups(self) -> dict[int, dict[str, any]]:
        # get all harbors first
        hubs = self._get_all_harbors()

        table_parts = self.water_body_table_name.rpartition('.')
        t = Table(table_parts[2], self.metadata_obj, schema=table_parts[0])
        geom_col = Column(self.water_body_geom).label('geom')
        fields = [Column(self.water_body_index_col).label('id'), geom_col]
        groups = {}

        for idx, row in hubs.iterrows():
            # get the closest water body for this harbor
            s = select(*fields).select_from(t).limit(1).order_by(func.ST_DistanceSpheroid(geom_col, literal_column(
                "'SRID=" + str(self.crs_no) + ";" + str(row[self.water_body_geom]) + "'")))
            result = self.conn.execute(s).fetchone()

            if result is None:
                logger.warning(f"No water body found for harbor {idx}")
                continue

            # add to groups
            if result[0] not in groups:
                groups[result[0]] = {
                    'geom': wkb.loads(result[1]),  # shapely object
                    'hubs': []
                }
            geom = row[self.water_body_geom]
            prepare(geom)
            groups[result[0]]['hubs'].append({'id': idx, 'geom': geom})

        return groups

    def _get_all_harbors(self) -> gpd.GeoDataFrame:
        """
        Get list of all harbors.

        :return: GeoPandas GeoDataFrame with all harbors
        """
        # get hubs - create statement via sql alchemy
        table_parts = self.hubs_table_name.rpartition('.')
        t = Table(table_parts[2], self.metadata_obj, Column(self.hubs_harbor), schema=table_parts[0])
        fields = [Column(self.hubs_index_col).label('id'), Column(self.hubs_geom_col).label('geom')]
        s = select(*fields).select_from(t).where(t.c[self.hubs_harbor] == literal_column("'y'"))
        return gpd.GeoDataFrame.from_postgis(str(s.compile()), self.conn,
                                             geom_col='geom',
                                             index_col='id',
                                             coerce_float=self.hubs_coerce_float)

    def _construct_river_paths(self, groups: dict[int, dict[str, any]]) -> nx.Graph:
        """
        Construct river paths

        :param groups: Groups created by _get_harbor_groups
        :return: nx.Graph graph of all river paths
        """
        graph = nx.Graph()

        for idx, water_body in groups.items():
            # get single river path system
            graph = nx.compose(graph, self._construct_river_path(idx, water_body))

        return graph

    def _construct_river_path(self, idx: int, water_body: dict[str, any]) -> nx.Graph:
        """
        Create single river path

        :param idx: River index within database
        :param water_body: water body structure reperesenting river and hubs
        :return: nx.Graph single graph system for this river
        """

        logger.info(f"Getting approximate medial axis for river system {idx} - this can take a long time...")
        result = self.conn.execute(text(
            f"SELECT st_approximatemedialaxis({self.water_body_geom}) as axis from {self.water_body_table_name} where {self.water_body_index_col} = {idx}")).fetchone()
        ideal_axis = wkb.loads(result[0])

        # explode the multistring into single lines in order to create the graph
        lines: list = get_parts(ideal_axis).tolist()

        logger.info(
            f"Constructing river paths for river {idx} which has {len(water_body['hubs'])} hubs and {len(lines)}"
            " lines to consider.")

        # now create graph checking each line - we will not prepare the lines, because the geometry tests are
        # really simple after all
        graph = nx.Graph()

        for lix, line in enumerate(lines):
            if logger.level <= logging.DEBUG and lix % 100 == 0:
                logger.debug(f"{lix}/{len(lines)}...")

            line_points = get_coordinates(line)
            from_id = str(line_points[0][0]) + "_" + str(line_points[0][1])
            to_id = str(line_points[1][0]) + "_" + str(line_points[1][1])

            # create points
            from_p = Point(line_points[0][0], line_points[0][1], 0.)
            to_p = Point(line_points[1][0], line_points[1][1], 0.)
            prepare(from_p)
            prepare(to_p)

            # check for the closest points to hubs
            for hub in water_body['hubs']:
                d1 = hub['geom'].distance(from_p)
                d2 = hub['geom'].distance(to_p)

                if 'min_dist' not in hub:
                    if d1 < d2:
                        hub['min_dist'] = d1
                        hub['min_id'] = from_id
                    else:
                        hub['min_dist'] = d2
                        hub['min_id'] = to_id
                elif d1 < hub['min_dist']:
                    hub['min_dist'] = d1
                    hub['min_id'] = from_id
                elif d2 < hub['min_dist']:
                    hub['min_dist'] = d2
                    hub['min_id'] = to_id

            destroy_prepared(from_p)
            destroy_prepared(to_p)

            # apply graph data - nodes...
            if not graph.has_node(from_id):
                graph.add_node(from_id, geom=from_p, type='river')
            if not graph.has_node(to_id):
                graph.add_node(to_id, geom=to_p, type='river')
            # ... and edges
            graph.add_edge(from_id, to_id, geom=line, id=idx, length=line.length)

        # add hubs as nodes, add edge to the closest point in network
        # It would be cooler to connect hubs to the closest line in the graph, but this makes the calculation way
        # more complicated. TODO: Think about a way to do this.
        for hub in water_body['hubs']:
            destroy_prepared(hub['geom'])  # free memory
            graph.add_node(hub['id'], geom=hub['geom'], type='hub')

            # get closest hub
            connector = graph.nodes[hub['min_id']]
            geom = LineString([get_coordinates(connector['geom'])[0][0:2], get_coordinates(hub['geom'])[0][0:2]])
            graph.add_edge(hub['id'], hub['min_id'], geom=geom, idx=-1, length=geom.length)

        logger.info(
            f"Full graph has {len(graph.nodes)} nodes and {len(graph.edges)} edges - cleaning up and simplifying a"
            " bit...")

        nodes_included = set()

        # remove dangling nodes/edges
        graph = self.remove_dangling_edges(graph, [hub['id'] for hub in water_body['hubs']])

        # simplify our graph in order ro reduce complexity
        graph = self.compact_graph(graph)

        logger.info(
            f"Compacted graph has {len(graph.nodes)} nodes and {len(graph.edges)} edges - finding"
            " paths between hubs and constructing paths. This may take a while...")

        # now we construct the shortest paths between all hubs - this is all we need.
        for from_id in range(len(water_body['hubs']) - 1):
            for to_id in range(from_id + 1, len(water_body['hubs'])):
                from_hub = water_body['hubs'][from_id]
                to_hub = water_body['hubs'][to_id]

                logger.info(f"Calculating paths between {from_hub['id']} and {to_hub['id']}...")
                # by number of edges - should be ok
                for path in nx.all_shortest_paths(graph, from_hub['id'], to_hub['id']):
                    for node in path:
                        nodes_included.add(node)

                # by length
                for path in nx.all_shortest_paths(graph, from_hub['id'], to_hub['id'], weight='length'):
                    for node in path:
                        nodes_included.add(node)

                # Using both shortest path algorithms should yield multiple paths to try out - we do not know the
                # ideal one yet, so it is good to have a few alternatives.

        # clean graph
        for node in list(graph.nodes):
            if node not in nodes_included:
                graph.remove_node(node)

        logger.info(
            f"Finished path calculation for river {idx}. Boiled graph down to {len(graph.nodes)} nodes and {len(graph.edges)} edges.")

        return graph

    def remove_dangling_edges(self, g: nx.Graph, exclude_hubs: list[str]) -> nx.Graph:
        """
        Remove dangling edges in our river system
        :param g:
        :param exclude_hubs:
        :return:
        """
        counter = 0

        while counter < 100:
            nodes = [node for node, degree in g.degree() if degree == 1 and node not in exclude_hubs]
            if len(nodes) == 0:
                break
            g.remove_nodes_from(nodes)

            counter += 1

        return g

    def compact_graph(self, g: nx.Graph) -> nx.Graph:
        # inspired by https://stackoverflow.com/questions/68499507/reduce-number-of-nodes-edges-of-a-graph-in-nedworkx

        graph_edges = g.edges()
        graph_nodes = g.nodes()

        # create subgraph of all nodes with degree 2
        is_chain = [node for node, degree in g.degree() if degree == 2]
        chains = g.subgraph(is_chain)

        # contract connected components (which should be chains of variable length) into single node
        components = [chains.subgraph(c) for c in nx.components.connected_components(chains)]

        # initialise new graph with all nodes - we will delete edges from it
        h = g.copy()

        # get compactable paths and add these to the graph
        for component in components:
            # get the two endpoints of our subgraph
            end_points = [node for node, degree in component.degree() if degree < 2]
            if len(end_points) != 2:
                continue

            geom = None
            component_nodes = component.nodes()

            # create path - we are being lazy here, might be more efficient to traverse the graph using neighbors...
            path = nx.shortest_path(component, end_points[0], end_points[1])

            if len(path) <= 2:
                continue

            last_node = None
            for node in path:
                if last_node is not None:
                    # traverse pairs of the path and stitch our line
                    path_geom = graph_edges[last_node, node]['geom']
                    # change direction of line?
                    if graph_nodes[last_node]['geom'].coords[0] != path_geom.coords[0]:
                        path_geom = path_geom.reverse()

                    if geom is None:
                        geom = path_geom
                    else:
                        geom = line_merge(MultiLineString([geom, path_geom]))
                        if type(geom) != LineString:
                            raise Exception(f"Unexpected geometry type after line_merge: {type(geom)}")

                last_node = node

            # delete inner nodes
            h.remove_nodes_from(path[1:-1])

            # add new edge
            h.add_edge(end_points[0], end_points[1], geom=geom, length=geom.length)

        return h

    def _convert_river_paths_to_geopandas(self, paths: nx.Graph, context: Context) -> None:
        # first, add hubs
        index = []
        data_frame = {'geom': [], 'overnight': []}
        for col in context.raw_hubs.columns:
            data_frame[col] = []

        for hub in paths.nodes:
            if hub not in context.raw_hubs.index:
                index.append(hub)

                hdata = paths.nodes[hub]
                for col in data_frame:
                    if col == 'geom':
                        data_frame[col].append(hdata[col])
                    elif col == 'overnight':
                        if 'overnight' in hdata:
                            data_frame[col].append(hdata['overnight'])
                        else:
                            data_frame[col].append('n')
                    else:
                        data_frame[col].append(None)
        df = gpd.GeoDataFrame(data_frame, index=index)
        context.raw_hubs = gpd.GeoDataFrame(pd.concat([context.raw_hubs, df]))

        # now add rivers
        index = []
        data_frame = {'geom': [], 'hubaid': [], 'hubbid': []}
        for col in context.raw_rivers.columns:
            data_frame[col] = []

        for river in paths.edges:
            idx = str(river[0]) + "_" + str(river[1])
            index.append(idx)
            rdata = paths.edges[river]
            for col in data_frame:
                if col == 'geom':
                    data_frame[col].append(rdata[col])
                elif col == 'hubaid':
                    data_frame[col].append(river[0])
                elif col == 'hubbid':
                    data_frame[col].append(river[1])
                else:
                    data_frame[col].append(None)

        df = gpd.GeoDataFrame(data_frame, index=index)
        context.raw_rivers = gpd.GeoDataFrame(pd.concat([context.raw_rivers, df]))

        # update epsg, because we might have lost it
        context.raw_hubs = context.raw_hubs.set_geometry('geom').set_crs(epsg=self.crs_no)
        context.raw_rivers = context.raw_rivers.set_geometry('geom').set_crs(epsg=self.crs_no)

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'PsqlConstructRiverPaths'
