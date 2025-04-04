# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Convert Graph to directed graph using directions.
"""
import igraph as ig


def convert_graph_to_directed(graph: ig.Graph, direction: str, reverse: bool, from_hub: str, to_hub: str) -> ig.Graph:
    """
    Convert Graph to directed graph using directions.

    :param graph: input graph (undirected)
    :param direction: direction key (will be converted to lowercase for comparison)
    :param reverse: True if directions are reversed
    :param from_hub: start hub
    :param to_hub: end hub
    :return: directed graph
    """

    # create base target Graph
    g: ig.Graph = create_directed_graph(graph, direction, reverse)

    start_hub = g.vs.find(name=from_hub)
    print(start_hub.neighbors(mode="out"))

    # # print(g.vs.find(name=from_hub))
    # # print(g.vs.find(name=to_hub))
    #
    # for iter in g.bfsiter(g.vs.find(name=from_hub)):
    #     print(iter['name'])

    return g

def get_direction_value(value: int, reverse: bool) -> int:
    """
    Get direction value based on reverse flag.

    :param value: Original value
    :param reverse: True if reverse is enabled
    :return: New value
    """
    if not reverse:
        return value
    if value == 1:
        return -1
    if value == -1:
        return 1
    return value


def create_directed_graph(graph: ig.Graph, direction: str, reverse: bool) -> ig.Graph:
    """
    Create directed graph based on direction.

    :param graph: input graph (undirected)
    :param direction: direction key (lower case)
    :param reverse: True if directions are reversed
    :return: directed graph
    """

    # create target Graph
    g: ig.Graph = ig.Graph(directed=True)

    # convert direction to lower case for case-insensitive comparison
    direction = direction.lower()

    # get ids
    hub_ids, edge_ids = get_route_hub_and_edge_ids(graph, direction)

    # first, create hubs
    for hub in graph.vs.select(hub_ids):
        hub: ig.Vertex
        g.add_vertices(1, attributes=hub.attributes())

    for edge in graph.es.select(edge_ids):
        edge: ig.Edge
        if edge['name'] == 'STR-REC-ROA-TEURN-002-2024_01_22':
            print(edge)

        attrs = edge.attributes().copy()
        # set correct direction value
        direction_value = get_direction_value(attrs['directions'][direction], reverse)
        # delete directions attribute
        del attrs['directions']

        from_v = edge.source_vertex['name']
        to_v = edge.target_vertex['name']

        if direction_value == 1 or direction_value == 2:
            g.add_edges([(from_v, to_v)], attributes=attrs.copy())
        if direction_value == -1 or direction_value == 2:
            new_attrs = attrs.copy()
            new_attrs['reverse'] = True
            new_attrs['name'] = new_attrs['name'] + '_rev'
            g.add_edges([(to_v, from_v)], attributes=new_attrs)

    return g


def get_route_hub_and_edge_ids(graph: ig.Graph, direction: str) -> tuple[list, list]:
    """
    Get route hub and edge ids based on direction.

    :param graph: input graph
    :param direction: direction key (lower case)
    :return: list of route hub ids, list of edge ids
    """
    vertices: set[int] = set()
    edges: set[int] = set()

    # get vertices that have edges with any direction
    for es in graph.es:
        if 'directions' in es.attribute_names() and direction in es['directions'] and es['directions'][direction] != 0:
            # add hub id to vertex set
            vertices.add(es.source)
            vertices.add(es.target)
            # add edge id to edge set
            edges.add(es.index)

    return list(vertices), list(edges)



#
#
#         for es in context.graph.es:
#             if 'directions' in es.attribute_names() and route in es['directions'] and es['directions'][route] != 0:
#                 direction = es['directions'][route]
#                 if direction == 1:
#                     # direction as taken
#                     g.add_edge(es.source_vertex['name'], es.target_vertex['name'])
#                 elif direction == -1:
#                     # reverse direction
#                     g.add_edge(es.target_vertex['name'], es.source_vertex['name'])