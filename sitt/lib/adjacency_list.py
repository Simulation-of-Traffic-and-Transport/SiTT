# SPDX-FileCopyrightText: 2024-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

import heapq as hq
from sys import float_info
from typing import Tuple, Dict

import igraph as ig

"""
Create an adjacency list for a given undirected graph with distances from a given start node.
"""


def create_adjacency_list(g: ig.Graph, start_node: int | str | ig.Vertex, is_sorted: bool = False) -> tuple[
    dict[int, float], dict[int, int | None], dict[int, set]]:
    """
    Create an adjacency list for a given undirected graph with distances from a given start node.

    :param g: Undirected graph
    :param start_node: Start node index, name or vertex object
    :param is_sorted: If True, sort adjacent nodes distance in ascending order. Default is False.
    :return: Tuple of dictionaries (distances, sources, nodes_before). distances is a dictionary of node index to
        the shortest distance from the start node to the given node. sources is a dictionary of node index to the node
        that we reached it through. nodes_before is a dictionary of node index to the nodes that came before it.
    """
    # convert start_node to index
    if type(start_node) == ig.Vertex:
        start_node_index = start_node.index
    elif type(start_node) == str:
        start_node_index = g.vs.find(name=start_node).index
    elif type(start_node) == int:
        start_node_index = start_node
    else:
        raise ValueError("Invalid start_node type")

    # each index will be a node index with list of tuples (neighbor_index, edge_index, edge_length)
    adj: list[list[tuple[int, int, float]]] = [[]] * g.vcount()
    for v in g.vs:
        adj_list = []
        for e in v.incident():
            idx = e.target if e.source == v.index else e.source
            adj_list.append((idx, e.index, e['length_m']))
        # sort by length
        if is_sorted:
            adj_list.sort(key=lambda x: x[2])
        adj[v.index] = adj_list

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

    return distances, sources, nodes_before
