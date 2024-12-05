# SPDX-FileCopyrightText: 2024-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

"""
Graph compactor - quite powerful tool to simplify a graph while preserving the geometry.
"""

import sys
from zlib import crc32

import igraph as ig
from pyproj import Transformer
from shapely import LineString, Polygon, Point, centroid, intersection
from shapely.ops import transform


def compact_graph(g: ig.Graph, transformer: Transformer) -> ig.Graph:
    # compact graph
    # in a way, we do something similar to http://szhorvat.net/mathematica/IGDocumentation/#igsmoothen - but we
    # need to preserve the geometry
    # inspired by https://stackoverflow.com/questions/68499507/reduce-number-of-nodes-edges-of-a-graph-in-nedworkx

    # create a copy of our graph - add connectors first => connectors are all nodes with more than 2 degrees
    # tg is our target graph
    tg: ig.Graph = g.subgraph([vertex['name'] for vertex in g.vs if vertex.degree() > 2])
    # add data to edges in subgraph
    _update_edge_attributes_of_direct_neighbors(tg, transformer)

    # walk chain components - these all have degree <= 2
    # if 1 => end points
    # if 2 => simple connectors between two shapes
    # walk chains and get endpoints for each component cluster
    for component in g.subgraph(
            [vertex for vertex in g.vs if 0 < vertex.degree() <= 2]).connected_components().subgraphs():
        # Case 1: single endpoint without multiple neighbors
        if component.vcount() == 1:
            # find vertex in original graph and look for neighbors
            source = component.vs[0]
            neighbors = g.vs.find(name=source['name']).neighbors()
            # in the original graph, this point might be a single endpoint - or between two connectors
            if len(neighbors) == 1:
                # endpoint
                target = neighbors[0]
            elif len(neighbors) == 2:
                # connector
                source = neighbors[0]
                target = neighbors[1]
            else:
                print("fatal error: too many neighbors", source, neighbors)
                exit(-1)

            # add vertices
            _add_vertex(tg, source.attributes())
            _add_vertex(tg, target.attributes())

            # construct edge
            _create_compacted_line_data(g, tg, source['name'], target['name'], transformer)

        # Case 2: line of points - two endpoints
        else:
            names_to_exclude = [vertex['name'] for vertex in component.vs]
            endpoints = [vertex['name'] for vertex in component.vs if vertex.degree() == 1]
            if len(endpoints) != 2:
                print("fatal error: too many endpoints", endpoints)
                exit(-1)
            # source and target are the endpoints of the line
            source = component.vs.find(name=endpoints[0])['name']
            target = component.vs.find(name=endpoints[1])['name']

            # expand to connector points, so we can connect to points on the target graph
            source = _expand_point_list_with_outer_neighbors(g, component, source, excluded_names=names_to_exclude)
            target = _expand_point_list_with_outer_neighbors(g, component, target, excluded_names=names_to_exclude)

            # get segments of equal depth
            for segment in _get_segments(component, source, target):
                # add vertices
                _add_vertex(tg, component.vs.find(name=segment[0]).attributes())
                _add_vertex(tg, component.vs.find(name=segment[1]).attributes())

                # construct edge
                _create_compacted_line_data(component, tg, segment[0], segment[1], transformer)

    return tg


def _expand_point_list_with_outer_neighbors(og: ig.Graph, tg: ig.Graph, name: str, excluded_names: list[str]) -> str:
    """
    Expand the target graph with the outer neighbors of a vertex in the original graph, if applicable
    :param og: original graph
    :param tg: target graph
    :param name: name of vertex to find neighbors for
    :param excluded_names: list of vertex names to exclude
    :return:
    """
    neighbor = _get_outer_neighbor(og, name, excluded_names)
    if neighbor is not None:
        # add new vertex to the target graph
        tg.add_vertices(1, attributes=neighbor.attributes())
        # connect
        tg.add_edge(name, neighbor['name'])
        return neighbor['name']
    return name


def _get_outer_neighbor(tg: ig.Graph, name: str, excluded_names: list[str]) -> ig.Vertex | None:
    """
    Get the outer neighbor of a vertex in the graph. Neighbors must not be in the list of excluded_names.
    :param tg: graph
    :param name: name to look for
    :param excluded_names: list of vertex names to exclude
    :return:
    """
    neighbors = [vertex for vertex in tg.vs.find(name=name).neighbors() if
                 vertex['name'] not in excluded_names]
    if len(neighbors) == 1:
        return neighbors[0]
    if len(neighbors) > 1:
        print("fatal error: too many neighbors", name, neighbors)
        exit(-1)
    return None


def _add_vertex(g: ig.Graph, attributes):
    try:
        g.vs.find(name=attributes['name'])
    except:
        g.add_vertices(1, attributes=attributes)


def _get_segments(g: ig.Graph, source: str, target: str) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    last_vertex = None

    # get list of vertices along the shortest path
    path = g.get_shortest_path(source, target)
    for v_id in path:
        v = g.vs[v_id]
        if last_vertex is None:
            last_vertex = v
            continue

    # close
    last_node_name = g.vs[path[-1]]['name']
    if last_node_name != last_vertex['name']:
        segments.append((last_vertex['name'], last_node_name))

    return segments


def _update_edge_attributes_of_direct_neighbors(tg: ig.Graph, transformer: Transformer):
    """
    Update existing edge attributes of direct neighbors
    :param tg: target graph to update
    :param transformer: transformer
    :return:
    """
    for e in tg.es:
        # direct connection
        source = tg.vs[e.source]
        target = tg.vs[e.target]
        e['name'] = source['name'] + '=' + target['name']
        e['geom'] = LineString([source['geom'], target['geom']])


def _create_compacted_line_data(og: ig.Graph, tg: ig.Graph, source: str, target: str, transformer: Transformer):
    """merge a path from source to target in the graph into a single shape and edge"""

    points: list = []
    last_shape: Polygon | None = None

    shortest_path = og.get_shortest_path(source, target)

    # add center of first shape
    vertex = og.vs[shortest_path[0]]
    points.append(vertex['geom'])
    all_shapes: list[Polygon] = []

    for id in shortest_path:
        vertex = og.vs[id]

        # find common line of both shapes and take the center of it to get the new point
        if last_shape is not None:
            common_line = intersection(last_shape, vertex['geom'])
            # calculate the length of the common line - this is the width of river
            length = transform(transformer.transform, common_line).length
            center: Point = centroid(common_line)
            if center.is_empty:
                points.append(vertex['geom'])
            else:
                points.append(center)

            all_shapes.append(vertex['geom'])

        last_shape = vertex['geom']

    # add center of first and last shapes
    vertex = og.vs[shortest_path[-1]]
    points.append(vertex['geom'])
    geom = LineString(points)

    # create edge
    tg.add_edge(source, target, name=source + '=' + target + '-' + hex(crc32(geom.wkb)), geom=geom,
                length=transform(transformer.transform, geom).length, shapes=all_shapes)
