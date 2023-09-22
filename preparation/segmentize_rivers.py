# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Segment rivers and water bodies - this is stuff that will take a very long time to complete, so it should be
done in advance."""

import argparse
import sys
from urllib import parse

import igraph as ig
from geoalchemy2 import Geometry
from shapely import wkb, get_parts, prepare, destroy_prepared, \
    delaunay_triangles, contains, overlaps, intersection, STRtree, LineString, Polygon, relate_pattern, centroid
import shapely.ops as sp_ops
from sqlalchemy import create_engine, Table, Column, literal_column, insert, schema, MetaData, \
    Integer, Boolean, String, select, text
from pyproj import Transformer


def init():
    """Initialize database."""
    print("Initializing database from",
          _create_connection_string(args.server, args.database, args.user, args.password, args.port, for_printing=True))

    conn = create_engine(
        _create_connection_string(args.server, args.database, args.user, args.password, args.port)).connect()

    # ensure schemas
    if not conn.dialect.has_schema(conn, args.topology_schema):
        conn.execute(schema.CreateSchema(args.topology_schema))
        print("Created schema:", args.topology_schema)
    if not conn.dialect.has_schema(conn, args.wip_schema):
        conn.execute(schema.CreateSchema(args.wip_schema))
        print("Created schema:", args.wip_schema)

    # ensure tables
    _get_water_body_table()
    _get_parts_table()
    metadata_obj.create_all(bind=conn, checkfirst=True)
    conn.commit()


def segment_rivers_and_water_bodies():
    """Segment rivers and water bodies - actual segmentation, takes a long time to complete."""
    print("Segment rivers and water bodies - actual segmentation, takes a long time to complete.")

    # database stuff
    conn = create_engine(
        _create_connection_string(args.server, args.database, args.user, args.password, args.port)).connect()
    water_body_table = _get_water_body_table()
    parts_table = _get_parts_table()

    # read water body entries
    for body in conn.execute(water_body_table.select()):
        print("Segmenting water body", body[0])

        geom = wkb.loads(body[1].desc)
        prepare(geom)

        # split the water body into triangles
        parts = get_parts(delaunay_triangles(geom))
        total = len(parts)
        c = 0
        for part in parts:
            c += 1
            if contains(geom, part):
                stmt = insert(parts_table).values(
                    geom=literal_column("'SRID=" + str(args.crs_no) + ";" + str(part) + "'"), water_body_id=body[0],
                    is_river=body[2])
                compiled_stmt = stmt.compile(compile_kwargs={'literal_binds': True})
                conn.execute(stmt)
                print(c / total, compiled_stmt)
                conn.commit()
            elif overlaps(geom, part):
                pass
                sub_parts = get_parts(intersection(geom, part))
                for p in sub_parts:
                    if p.geom_type == 'Polygon':
                        stmt = insert(parts_table).values(
                            geom=literal_column("'SRID=" + str(args.crs_no) + ";" + str(p) + "'"),
                            water_body_id=body[0], is_river=body[2])
                        compiled_stmt = stmt.compile(compile_kwargs={'literal_binds': True})
                        conn.execute(stmt)
                        print(c / total, compiled_stmt)
                        conn.commit()

        destroy_prepared(geom)


def networks():
    """Create networks from water body triangles. This is the toughest part of the process."""
    print("Create networks from water body triangles. This is the toughest part of the process.")

    # database stuff
    conn = create_engine(
        _create_connection_string(args.server, args.database, args.user, args.password, args.port)).connect()
    water_body_table = _get_water_body_table()
    parts_table = _get_parts_table()

    nodes_table = Table("water_nodes", metadata_obj,
          Column("id", String(length=16), primary_key=True),
          Column("geom", Geometry('POINT')),
          Column("water_body_id", Integer, index=True),
          Column("is_river", Boolean),
          schema=args.wip_schema)

    edges_table = Table("water_edges", metadata_obj,
                        Column("id", String(length=32), primary_key=True),
                        Column("geom", Geometry('LINESTRING')),
                        Column("water_body_id", Integer, index=True),
                        Column("is_river", Boolean),
                        schema=args.wip_schema)

    metadata_obj.create_all(bind=conn, checkfirst=True)

    conn.execute(text("TRUNCATE TABLE " + args.wip_schema + ".water_nodes"))
    conn.execute(text("TRUNCATE TABLE " + args.wip_schema + ".water_edges"))
    conn.commit()

    transformer = Transformer.from_crs(args.crs_no, args.crs_to, always_xy=True)

    # read water body entries
    for body in conn.execute(water_body_table.select().where(water_body_table.c.id == 131)):
        print("Networking water body", body[0])

        # get all data
        tree = []
        stmt = select(parts_table.c.geom).select_from(parts_table).where(parts_table.c.water_body_id == body[0])
        for row in conn.execute(stmt):
            tree.append(wkb.loads(row[0].desc))

        # keeps entities already considered and still to consider
        already_considered = set()
        to_consider = set()
        to_consider.add(0)  # we will always consider the first entry

        # efficient tree tester for fast geometry functions
        tree = STRtree(tree)

        # graph
        g = ig.Graph()

        while len(to_consider) > 0:
            idx = to_consider.pop()
            already_considered.add(idx)
            entity = tree.geometries.take(idx)

            str_idx = _add_vertex(g, body[0], idx, entity)

            # get neighbors
            for id in tree.query(entity, 'touches'):
                if id in already_considered or not relate_pattern(entity, tree.geometries.take(id), '****1****'):
                    continue
                to_consider.add(id)

                # add vertex
                str_id = _add_vertex(g, body[0], id, tree.geometries.take(id))

                g.add_edge(str_idx, str_id)

        # compact graph
        # in a way, we do something similar to http://szhorvat.net/mathematica/IGDocumentation/#igsmoothen - but we
        # need to preserve the geometry
        # inspired by https://stackoverflow.com/questions/68499507/reduce-number-of-nodes-edges-of-a-graph-in-nedworkx

        # get all nodes that are simple connectors or end points (degree <= 2)
        is_chain = [vertex['name'] for vertex in g.vs if vertex.degree() <= 2]
        # ids of vertices that are not simple connectors or end points (degree > 2) - will be added to
        ids = [vertex['name'] for vertex in g.vs if vertex.degree() > 2]

        # find chain components - this is a list of single edge lists
        components = g.subgraph(is_chain).connected_components().subgraphs()

        # now we filter chains that are too short and add their names to ids list
        for component in components:
            if component.vcount() == 1:
                ids.append(component.vs[0]['name'])

        # create a copy of our graph - add connectors and dangling endpoints first
        tg = g.subgraph(ids)
        for e in tg.es:
            # direct connection
            source = tg.vs[e.source]
            target = tg.vs[e.target]
            e['name'] = source['name'] + '=' + target['name']
            e['geom'] = LineString([source['center'], target['center']])
            e['length'] = sp_ops.transform(transformer.transform, e['geom']).length

        # TODO: create components from created subgraph - we want to compact those later

        # walk chains and get endpoints for each component cluster
        for component in g.subgraph(is_chain).connected_components().subgraphs():
            if component.vcount() > 1:
                endpoints = [vertex['name'] for vertex in component.vs if vertex.degree() == 1]
                if len(endpoints) != 2:
                    print("fatal error: to many endpoints", endpoints)
                    sys.exit(-1)
                source = component.vs.find(name=endpoints[0])
                target = component.vs.find(name=endpoints[1])
                # add vertices
                tg.add_vertex(source['name'], geom=source['geom'], center=source['center'])
                tg.add_vertex(target['name'], geom=target['geom'], center=target['center'])
                # construct new edge from path
                [line, shape] = _merge_path(component, source['name'], target['name'])
                length = sp_ops.transform(transformer.transform, line).length
                tg.add_edge(endpoints[0], endpoints[1], geom=line, name=endpoints[0] + '=' + endpoints[1], length=length)
                # TODO: do something with the shape

                # connect new end points to connectors
                _complete_neighboring_connections(g, tg, source, transformer)
                _complete_neighboring_connections(g, tg, target, transformer)

        for v in tg.vs:
            stmt = insert(nodes_table).values(
                id=v['name'],
                geom=literal_column("'SRID=" + str(args.crs_no) + ";" + str(v['center']) + "'"), water_body_id=body[0],
                is_river=body[2])
            compiled_stmt = stmt.compile(compile_kwargs={'literal_binds': True})
            print(compiled_stmt)
            conn.execute(stmt)

        for e in tg.es:
            stmt = insert(edges_table).values(
                id=e['name'],
                geom=literal_column("'SRID=" + str(args.crs_no) + ";" + str(e['geom']) + "'"), water_body_id=body[0],
                is_river=body[2])
            compiled_stmt = stmt.compile(compile_kwargs={'literal_binds': True})
            print(compiled_stmt)
            conn.execute(stmt)

        conn.commit()

def _complete_neighboring_connections(g: ig.Graph, tg: ig.Graph, vertex: ig.Vertex, transformer: Transformer) -> None:
    for i in g.neighbors(vertex['name'], 'all'):
        neighbor = g.vs[i]
        # neighbor in target graph?
        try:
            tg.vs.find(name=neighbor['name'])
        except:
            continue

        # no exception - check if we have already added this edge
        try:
            tg.es.find(_source=neighbor['name'],_target=vertex['name'])
        except:
            # add new edge
            line = LineString([vertex['center'], neighbor['center']])
            length = sp_ops.transform(transformer.transform, line).length
            tg.add_edge(vertex['name'], neighbor['name'], geom=line, name=vertex['name'] + '=' + neighbor['name'],
                        length=length)

def _add_vertex(g: ig.Graph, water_body_id: int, idx: int, geom: object) -> str:
    """Add a vertex to the graph. if it does not exist yet. Returns the index of the vertex."""
    str_idx = str(water_body_id) + '-' + str(idx)
    try:
        g.vs.find(name=str_idx)
    except:
        center = centroid(geom)

        g.add_vertex(str_idx, geom=geom, center=center)

    return str_idx


def _merge_path(g: ig.Graph, source: str, target: str) -> [LineString, Polygon]:
    """merge a path from source to target in the graph into a single shape and edge"""
    points: list = []
    shape: Polygon | None = None
    for id in g.get_shortest_paths(source, target)[0]:
        vertex = g.vs[id]
        points.append(vertex['center'])
        if shape is None:
            shape = vertex['geom']
        else:
            shape = shape.union(vertex['geom'])

    return LineString(points), shape

def _create_connection_string(server: str, db: str, user: str, password: str, port: int, for_printing=False):
    """
    Create DB connection string

    :param for_printing: hide password, so connection can be printed
    """
    if for_printing:
        return 'postgresql://' + user + ':***@' + server + ':' + str(
            port) + '/' + db
    else:
        return 'postgresql://' + user + ':' + parse.quote_plus(password) + '@' + server + ':' + str(port) + '/' + db


def _get_water_body_table() -> Table:
    return Table(args.water_body_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry),
                 Column("is_river", Boolean),
                 schema=args.topology_schema)


def _get_parts_table() -> Table:
    return Table(args.parts_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry('POLYGON')),
                 Column("water_body_id", Integer, index=True),
                 Column("is_river", Boolean),
                 schema=args.wip_schema)


if __name__ == "__main__":
    """Segment rivers and water bodies - this is stuff that will take a very long time."""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Segment rivers and water bodies - this is stuff that will take a very long time to complete, so it should be done in advance.",
        exit_on_error=False)
    parser.add_argument('action', default='help', choices=['help', 'init', 'segment', 'networks'],
                        help='action to perform')

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')

    parser.add_argument('--topology-schema', dest='topology_schema', default='topology', type=str,
                        help='topology schema name')
    parser.add_argument('--wip-schema', dest='wip_schema', default='water_wip', type=str, help='water_wip schema name')
    parser.add_argument('--parts-table', dest='parts_table', default='parts', type=str, help='name of parts table')
    parser.add_argument('--water-body-table', dest='water_body_table', default='water_body', type=str,
                        help='name of water_body table')

    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection')
    parser.add_argument('--crs-to', dest='crs_to', default=32633, type=int, help='target projection (for approximation of lengths)')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # init meta data structure
    metadata_obj: MetaData = MetaData()

    # select action
    if args.action == 'help':
        parser.print_help()
    elif args.action == 'init':
        init()
    elif args.action == 'segment':
        segment_rivers_and_water_bodies()
    elif args.action == 'networks':
        networks()
