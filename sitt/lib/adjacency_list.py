# SPDX-FileCopyrightText: 2024-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

import heapq as hq
from sys import float_info

import igraph as ig

"""
Create an adjacency list for a given undirected graph with distances from a given start node.
"""


def create_adjacency_list(g: ig.Graph, start_node_index: int, is_sorted: bool = False) -> list[list[tuple[int, int, float]]]:
    """
    Create an adjacency list for a given undirected graph.

    :param g: Undirected graph
    :param start_node_index: Start node index
    :param is_sorted: If True, sort adjacent nodes distance in ascending order. Default is False.
    :return: Adjacency list for the given graph. Each entry is an index plus a list of tuples (neighbor_index, edge_index, edge_length)
    """
    # does our model have target hubs?
    has_target_hubs = 'target_hub' in g.es.attribute_names()

    # each index will be a node index with list of tuples (neighbor_index, edge_index, edge_length)
    adj: list[list[tuple[int, int, float]]] = [[]] * g.vcount()
    for v in g.vs:
        adj_list = []
        for e in v.incident():
            idx = e.target if e.source == v.index else e.source

            # TODO: not quite correct yet
            # # do not consider edges that have target hubs set and are the wrong direction
            # if has_target_hubs and e['target_hub'] is not None and e['target_hub'] != '' and e['target_hub'] != v['name']:
            #     continue

            adj_list.append((idx, e.index, e['length_m']))
        # sort by length
        if is_sorted:
            adj_list.sort(key=lambda x: x[2])
        adj[v.index] = adj_list

    return adj


def create_shortest_path_data(g: ig.Graph, start_node: int | str | ig.Vertex, is_sorted: bool = False) -> tuple[
    dict[int, float], dict[int, int | None], dict[int, set], list[list[tuple[int, int, float]]]]:
    """
    Create the shortest path data for a given undirected graph with distances from a given start node.

    :param g: Undirected graph
    :param start_node: Start node index, name or vertex object
    :param is_sorted: If True, sort adjacent nodes distance in ascending order. Default is False.
    :return: Tuple of dictionaries (distances, sources, nodes_before, adj). distances is a dictionary of node index to
        the shortest distance from the start node to the given node. sources is a dictionary of node index to the node
        that we reached it through. nodes_before is a dictionary of node index to the nodes that came before it. The
        last return value is the adjacency list.
    """
    start_node_index = node_to_index(g, start_node)

    adj: list[list[tuple[int, int, float]]] = create_adjacency_list(g, start_node_index, is_sorted)

    # run Dijkstra's Algorithm - keep list of distances and sources
    distances = {}
    sources = {}
    nodes_before: dict[int, set] = {start_node_index: set()}
    heap: list[tuple[float, int]] = [(0., start_node_index)]

    while heap:
        dist, idx = hq.heappop(heap)
        if idx in distances:
            continue  # Already encountered before

        # We know that this is the first time we encounter node.
        #   As we pull nodes in order of increasing distance, this
        #   must be the node's shortest distance from the start node.
        distances[idx] = dist

        # we set some variables to determine the shortest neighbor to this node
        min_dist_to_neighbors = float_info.max
        shortest_neighbor = None

        # Add neighbors to heap
        for neighbor_idx, _, edge_length in adj[idx]:
            if neighbor_idx not in nodes_before:
                nodes_before[neighbor_idx] = set(nodes_before[idx])
                nodes_before[neighbor_idx].add(idx)
            else:
                nodes_before[neighbor_idx].update(nodes_before[idx])
                nodes_before[neighbor_idx].add(idx)

            if neighbor_idx not in distances:  # only if not encountered before
                neighbor_distance = dist + edge_length
                hq.heappush(heap, (neighbor_distance, neighbor_idx))
            else:
                neighbor_distance = distances[neighbor_idx]

            # update the shortest neighbor to this node
            if neighbor_distance < min_dist_to_neighbors:
                min_dist_to_neighbors = neighbor_distance
                shortest_neighbor = neighbor_idx
        # now we also now the shortest neighbor to this node, we can later check the shortest path
        sources[idx] = shortest_neighbor

    return distances, sources, nodes_before, adj

def get_shortest_path(g: ig.Graph, start_node: int | str | ig.Vertex, end_node: int | str | ig.Vertex) -> list[int]:
    """
    Get the shortest path from the start node to the end node.

    :param g: Undirected graph
    :param start_node: Start node index, name or vertex object
    :param end_node: End node index, name or vertex object
    :return: List of vertices in the shortest path from start to end.
    """
    start_node_index = node_to_index(g, start_node)
    end_node_index = node_to_index(g, end_node)

    # create the shortest path data and do not consider certain edges (with targets hubs, these are directed)
    distances, sources, _, adj = create_shortest_path_data(g, start_node_index)

    if end_node_index not in distances:
        return []  # no path from start to end

    path = []
    current_node = end_node_index
    while current_node != start_node_index:
        source = sources[current_node]

        shortest_edge = None
        shortest_dist = float_info.max
        for neighbor_idx, edge_idx, edge_length in adj[current_node]:
            if neighbor_idx == source and edge_length < shortest_dist:
                shortest_edge = edge_idx
                shortest_dist = edge_length
                # TODO: not quite correct yet

        path.append(shortest_edge)

        current_node = source
    path.reverse()

    return path

def node_to_index(g: ig.Graph, node: int | str | ig.Vertex) -> int:
    # convert start_node to index
    if type(node) == ig.Vertex:
        return node.index
    elif type(node) == str:
        return g.vs.find(name=node).index
    elif type(node) == int:
        return node
    else:
        raise ValueError("Invalid node type")
