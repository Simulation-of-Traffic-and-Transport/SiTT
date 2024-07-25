# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Example file to create sql from pickled graph.
"""

# create table water_wip.test_edges
# (
#     id          text                 not null
#         constraint test_edges_pk
#             primary key,
#     geom        geometry(LineString) not null,
#     min_width   double precision,
#     depth_m     double precision,
#     length      double precision,
#     slope       double precision,
#     flow_rate   double precision,
#     flow_from   text,
#     flow_to     text
# );

# create table water_wip.test_vertices
# (
#     id     text
#         constraint test_vertices_pk
#             primary key,
#     center        geometry(Point) not null,
#     min_width     double precision,
#     depth_m       double precision,
#     flow_from     text,
#     flow_to       text,
#     shape_height  double precision,
#     min_height    double precision,
#     max_height    double precision,
#     is_bump       boolean
# );


import pickle
import igraph as ig

# source graph
g: ig.Graph = pickle.load(open('graph_dump_1_calculated.pickle', 'rb'))

print(g.summary())

f = open("vertices.sql", "w")

for v in g.vs:
    flow_to = []
    flow_from = []
    for e in v.incident('all'):
        if e['flow_to'] is None:
            pass # WIP
        elif e['flow_to'] != v['name']:
            flow_to.append(e['flow_to'])
        else:
            if e.source_vertex['name']!= v['name']:
                flow_from.append(e.source_vertex['name'])
            else:
                flow_from.append(e.target_vertex['name'])

    flow_from = ",".join(flow_from)
    flow_to = ",".join(flow_to)
    min_width = v['min_width'] if v['min_width'] is not None else 0
    depth_m = v['depth_m'] if v['depth_m'] is not None else 0
    is_bump = True if v['is_bump'] is True else False

    print("INSERT INTO water_wip.test_vertices (id, center, min_width, depth_m, is_bump, flow_from, flow_to, shape_height, min_height, max_height) VALUES "
          f"('{v['name']}', ST_GeomFromText('{v['center'].wkt}'), {min_width}, {depth_m}, {is_bump}, '{flow_from}', '{flow_to}', {v['shape_height']}, {v['min_height']}, {v['max_height']});", file=f)


f = open("edges.sql", "w")

for e in g.es:
    depth_m = e['depth_m'] if e['depth_m'] is not None else 0
    length = e['length'] if e['length'] is not None else 0
    slope = e['slope'] if e['slope'] is not None else 0
    flow_rate = e['flow_rate'] if e['flow_rate'] is not None else 0
    flow_from = e.source_vertex['name'] if e['flow_to'] == e.target_vertex['name'] else e.target_vertex['name']

    print("INSERT INTO water_wip.test_edges (id, geom, min_width, depth_m, length, slope, flow_rate, flow_from, flow_to) VALUES "
          f"('{e['name']}', ST_GeomFromText('{e['geom'].wkt}'), {e['min_width']}, {depth_m}, {length}, {slope}, {flow_rate}, '{flow_from}', '{e['flow_to']}');", file=f)
