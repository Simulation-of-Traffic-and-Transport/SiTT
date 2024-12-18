# SPDX-FileCopyrightText: 2024-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Help define graph directions"""

import os.path as path

import igraph as ig
import shapefile
from shapely import LineString, Point, distance

from sitt import Configuration, Context
from sitt.modules.preparation import PSQLBase, CreateRoutes


def convert_graph_to_shapefile(graph: ig.Graph, output_path: str, shapefile_name: str) -> None:
    w = shapefile.Writer(target=path.join(output_path, shapefile_name), shapeType=shapefile.POLYLINE, autoBalance=True)
    w.field("name", "C")
    w.field("source", "C")
    w.field("target", "C")
    w.field("count_a_b", "N")
    w.field("count_b_a", "N")

    for e in graph.es:
        geom: LineString = e['geom']
        # which end is closer?
        if distance(Point(geom.coords[0]), e.source_vertex['geom']) > distance(Point(geom.coords[-1]), e.source_vertex['geom']):
            geom = geom.reverse()

        coords = list([c[0], c[1]] for c in list(coord for coord in geom.coords))
        w.line([coords])
        w.record(e["name"], e.source_vertex['name'], e.target_vertex['name'], e["count_a_b"], e["count_b_a"])

    w.close()


def _add_vertex(g: ig.Graph, attributes):
    try:
        g.vs.find(name=attributes['name'])
    except:
        g.add_vertices(1, attributes=attributes)


def _add_edge(g: ig.Graph, e: ig.Edge) -> ig.Edge:
    # add vertices, if they don't exist yet
    _add_vertex(g, e.source_vertex.attributes())
    _add_vertex(g, e.target_vertex.attributes())

    # add new edge
    attr: dict = e.attributes().copy()
    # delete "none" types
    attr = {k: v for k, v in attr.items() if v is not None}

    return g.add_edge(e.source_vertex['name'], e.target_vertex['name'], **attr)


class GraphLoader(PSQLBase):
    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', schema: str = 'sitt'):
        super().__init__(server, port, db, user, password, schema)

    def run(self, config: Configuration, context: Context) -> Context:
        pass


# --------------- settings ---------------
db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'
db_port = 5432
db_name = 'sitt'
db_schema = 'sitt'
crs_no = 4326

directions = [
    # S -> N:
    # Start: STR-REC-SE-TIM-001-2023_03_17; Stop: STR-REC-CRO-IUVA-001-2024_07_10
    # Start: STR-REC-CRO-MAG-001-2023_01_20; Stop: STR-REC-SE-KIRCHD-001-2024_12_04
    # Start: STR-REC-SE-VIR-001-2023_03_13; Stop: STR-REC-CRO-IUVA-001-2024_07_10
    # ('STR-REC-SE-TIM-001-2023_03_17', 'STR-REC-CRO-IUVA-001-2024_07_10'),
    # ('STR-REC-CRO-MAG-001-2023_01_20', 'STR-REC-SE-KIRCHD-001-2024_12_04'),
    # ('STR-REC-SE-VIR-001-2023_03_13', 'STR-REC-CRO-IUVA-001-2024_07_10'),
    # W -> O:
    # Start: STR-REC-SE-TEURN-001-22_11_24; Stop: STR-REC-FP-CEL-001-2023_05_28
    # Start: STR-REC-SE-TEURN-001-22_11_24; Stop: STR-REC-FP-LAV-001-2023_04_24
    ('STR-REC-SE-TEURN-001-22_11_24', 'STR-REC-FP-CEL-001-2023_05_28'),
    ('STR-REC-SE-TEURN-001-22_11_24', 'STR-REC-FP-LAV-001-2023_04_24'),
]

# ----------------------------------------

# create object to load graph from database
loader = GraphLoader(server=db_host, port=db_port, db=db_name, user=db_user, password=db_password,
                     schema=db_schema)
g: ig.Graph = loader.load_graph_from_database()

graphs: list[ig.Graph] = []

# create multiple graphs
for direction in directions:
    # create context and config
    context: Context = Context()
    context.graph = g.copy()

    # create configuration and set simulation start and end points
    config: Configuration = Configuration()
    config.simulation_start = direction[1]
    config.simulation_end = direction[0]

    # precalculate routes
    routes = CreateRoutes()
    context = routes.run(config, context)

    graphs.append(context.routes)

# create base directed graph
g: ig.Graph = graphs[0]
# create attribute
g.es['count_a_b'] = 1
g.es['count_b_a'] = 0

for i in range(1, len(graphs)):
    # try to find common edges
    for e in graphs[i].es:
        name = e['name']
        try:
            te = g.es.find(name=name)
            # check direction
            if e.source_vertex['name'] != te.source_vertex['name']:
                te['count_b_a'] += 1
            else:
                te['count_a_b'] += 1
        except:
            # edge does not exist in union graph, add it
            te = _add_edge(g, e)
            te['count_a_b'] = 1
            te['count_b_a'] = 0

convert_graph_to_shapefile(g, ".", "union_graph.shp")