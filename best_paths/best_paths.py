# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Read pickled graph."""

import time
import pickle
import random
from pyproj import Transformer
import shapely.ops as sp_ops
import igraph as ig

if __name__ == "__main__":
    """Read pickled graph."""

    # good graphs for testing: start node: 62036 end node: 149105

    with open('graph_dump.pickle', 'rb') as f:
        g: ig.Graph = pickle.load(f)
        print(g.summary())

        print(g.vs["center"][0])
        print(g.vs["geom"][0])
        print(g.vs["name"][0])

        transformer = Transformer.from_crs(4326, 32633, always_xy=True)
        for edge in g.es:
            start = sp_ops.transform(transformer.transform, edge.source_vertex["center"])
            end = sp_ops.transform(transformer.transform, edge.target_vertex["center"])
            distance = start.distance(end)
            edge["distance"] = distance

        vertex_count = len(g.vs)

        start_node = input(f"Start node (index between 0-{vertex_count - 1} or random for a random point:")
        if start_node == "random":
            start_node = random.randint(0, vertex_count - 1)
        else:
            start_node = int(start_node)

        end_node = input(f"End node (index between 0-{vertex_count - 1} or random for a random point:")
        if end_node == "random":
            end_node = random.randint(0, vertex_count - 1)
        else:
            end_node = int(end_node)

        print(f"start node: {start_node}")
        print(f"end node: {end_node}")

        start = time.time()
        result = g.get_k_shortest_paths(start_node, end_node, k=5, weights=g.es["distance"], output="epath")
        end = time.time()

        found_paths = len(result)
        if found_paths > 0:
            print(f"Found {found_paths} best paths:")
            print(f"The operation took {(end - start):.2f} seconds.")

            for i in range(0, found_paths):
                path = result[i]
                distance = 0
                for edge in path:
                    distance += g.es[edge]["distance"]

                print(f"Path {i} distance is: {distance}")
