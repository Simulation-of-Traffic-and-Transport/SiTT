# SPDX-FileCopyrightText: 2024-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

import igraph as ig
import shapefile
import shapely
import os.path as path

"""
Graph to shapefile conversion.
"""


def convert_graph_to_shapefile(graph: ig.Graph, output_path: str, shapefile_name: str) -> None:
    w = shapefile.Writer(target=path.join(output_path, shapefile_name), shapeType=shapefile.POLYLINE, autoBalance=True)
    w.field("name", "C")

    for e in graph.es:
        geom: shapely.LineString = e['geom']
        if shapely.is_ccw(geom):  # need to be clockwise
            geom = geom.reverse()

        coords = list([c[0], c[1]] for c in list(coord for coord in geom.coords))
        w.line([coords])
        w.record(e["name"])

    w.close()
