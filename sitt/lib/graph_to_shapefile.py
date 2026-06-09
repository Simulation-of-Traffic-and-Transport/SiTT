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
    """
    Converts a graph with geographical data into a shapefile format, saving it to the specified location. The graph's
    edges must have geometries defined as LineString objects and names assigned to properly create the shapefile.

    :param graph: A graph object with edges containing geographical data (geometry and name).
    :type graph: igraph.Graph
    :param output_path: The directory where the shapefile will be saved.
    :type output_path: str
    :param shapefile_name: The name of the shapefile to be created.
    :type shapefile_name: str
    :return: None
    """
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
