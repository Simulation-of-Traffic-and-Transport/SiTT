# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Debug loaded paths and hubs"""
import logging
from zlib import crc32

import igraph as ig
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import yaml
from shapely.geometry import GeometryCollection

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class DebugDisplayPathsAndHubs(PreparationInterface):
    def __init__(self, draw_network: bool = True, show_network: bool = True, save_network: bool = False,
                 save_network_name: str = 'network', save_network_type: str = 'png', display_routes: bool = True,
                 start: str | None = None, end: str | None = None, show_graphs: bool = True, save_graphs: bool = False,
                 save_graphs_names: str = 'possible_routes', save_graphs_type: str = 'png',
                 save_graphs_number: int = 5):
        super().__init__()
        self.draw_network: bool = draw_network
        """draw the network graph"""
        self.show_network: bool = show_network
        """Plot graph to stdout"""
        self.save_network: bool = save_network
        """Save network to disk"""
        self.save_network_name: str = save_network_name
        self.save_network_type: str = save_network_type
        """possible values are eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff"""
        self.display_routes: bool = display_routes
        """Calculate example routes"""
        self.start: str | None = start
        """start hub id of example route"""
        self.end: str | None = end
        """end hub id of example route"""
        self.show_graphs: bool = show_graphs
        """plot routes to stdout"""
        self.save_graphs: bool = save_graphs
        """Save all graphs to disk"""
        self.save_graphs_names: str = save_graphs_names
        self.save_graphs_type: str = save_graphs_type
        """possible values are eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff"""
        self.save_graphs_number: int = save_graphs_number
        """number of graphs to save/show"""

    def run(self, config: Configuration, context: Context) -> Context:
        if context.graph:
            logger.info("Displaying paths and hubs")

            # draw complete network
            if self.draw_network:
                logger.info("Drawing network.")

                # coordinates for nodes
                v_x = []
                v_y = []
                geometries = []

                for vs in context.graph.vs:
                    v_x.append(vs['geom'].coords[0][0])
                    v_y.append(vs['geom'].coords[0][1])

                for es in context.graph.es:
                    geometries.append(es['geom'])

                _, ax = plt.subplots()

                lines = gpd.GeoSeries(GeometryCollection(geometries))
                lines.plot(ax=ax, color='coral')

                p = gpd.GeoSeries(gpd.points_from_xy(x=v_x, y=v_y))
                p.plot(ax=ax)

                if self.show_graphs:
                    plt.show()

                if self.save_network:
                    plt.savefig('%s.%s' % (self.save_network_name, self.save_network_type),
                                bbox_inches='tight', dpi=150)

            # draw single legs from a to b
            if self.display_routes and self.start and self.end:
                counter = 0
                paths = []
                for p in context.graph.get_k_shortest_paths(self.start, to=self.end, k=self.save_graphs_number,
                                                            weights='length_m', mode='all', output='epath'):
                    paths.append(p)
                if logger.level <= logging.INFO:
                    logger.info("Drawing %d route(s) from %s to %s.", len(paths), self.start, self.end)

                for path in paths:
                    # for plotting the height profile
                    profile_legs: np.darray = np.zeros((1,))  # first leg point starts at point 0
                    profile_height: np.darray | None = None
                    total_length = 0.0
                    geometries = []

                    # get single legs
                    for edge_id in path:
                        # third entry in tuple is the id of the vertex
                        edge = context.graph.es[edge_id]
                        total_length += edge['length_m']

                        # add leg points
                        my_legs = edge['legs']

                        offset = len(profile_legs)
                        my_len = len(my_legs)
                        profile_legs = np.append(profile_legs, np.zeros((my_len,)))
                        for i in range(0, my_len):
                            profile_legs[offset + i] = profile_legs[offset + i - 1] + my_legs[i]

                        # add heights
                        my_coords = edge['geom'].coords
                        geom_len = len(my_coords)
                        my_heights = np.zeros((geom_len,))
                        for i in range(0, geom_len, 1):
                            my_heights[i] = my_coords[i][2]

                        if profile_height is not None:
                            # delete first height
                            profile_height = np.append(profile_height, np.delete(my_heights, 0))
                        else:
                            profile_height = my_heights

                        geometries.append(edge['geom'])

                    counter += 1

                    # geographic view of route
                    fig, (col1, col2) = plt.subplots(2, 1)
                    fig.tight_layout(pad=2)
                    p = gpd.GeoSeries(GeometryCollection(geometries))
                    p.plot(ax=col1)
                    col1.set_title("%s to %s, Route %d" % (self.start, self.end, counter))

                    # height profile of route
                    plt.plot(profile_legs, profile_height,
                             color='red')
                    plt.title("Height Profile")
                    plt.xlabel("Distance (m, total = %f m)" % total_length)
                    plt.ylabel("Height (m)")

                    if self.show_graphs:
                        plt.show()

                    if self.save_graphs:
                        fig.savefig('%s_%d.%s' % (self.save_graphs_names, counter, self.save_graphs_type),
                                    bbox_inches='tight', dpi=150)

        else:
            logger.info("Skipping display of paths and hubs - no data.")

        exit()
        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'DebugDisplayPathsAndHubs'
