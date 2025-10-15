# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Debug loaded paths and hubs"""
import logging

import geopandas as gpd
import matplotlib.pyplot as plt
import shapefile
import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class DebugDisplayPathsAndHubs(PreparationInterface):
    def __init__(self, draw_network: bool = True, show_network: bool = True, save_network: bool = False,
                 save_network_name: str = 'network', save_network_type: str = 'png', display_routes: bool = True,
                 save_shapefile: bool = False, save_shapefile_name: str = 'network') -> None:
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
        self.save_shapefile: bool = save_shapefile
        """save network as shapefile"""
        self.save_shapefile_name: str = save_shapefile_name
        """Name of the shapefile to save"""

    def run(self, config: Configuration, context: Context) -> Context:
        # convert route to lower case for case-insensitive comparison
        route = config.simulation_route.lower()

        if context.graph and self.draw_network:
            logger.info("Displaying paths and hubs")

            vertices: set[str] = set()

            fig, ax = plt.subplots()

            for es in context.graph.es:
                if 'directions' in es.attribute_names() and route in es['directions'] and es['directions'][route] != 0:
                    direction = es['directions'][route]

                    s = context.graph.vs.find(name=es['from']) # we can trust this value
                    t = context.graph.vs.find(name=es['to']) # we can trust this value

                    vertices.add(s.index)
                    vertices.add(t.index)

                    arrow_style = '->'
                    if direction == -1:
                        arrow_style = '<-'
                    if direction == 2:
                        arrow_style = '<->'

                    ax.annotate(text='', xytext=(s['geom'].x, s['geom'].y), xy=(t['geom'].x, t['geom'].y), arrowprops=dict(arrowstyle=arrow_style, shrinkB=0, shrinkA=0, mutation_scale=4))

            # coordinates for nodes
            v_x = []
            v_y = []

            for vs in context.graph.vs.select(list(vertices)):
                v_x.append(vs['geom'].coords[0][0])
                v_y.append(vs['geom'].coords[0][1])

            p = gpd.GeoSeries(gpd.points_from_xy(x=v_x, y=v_y))
            p.plot(ax=ax,markersize=2)
            plt.title(config.simulation_route)

            if self.show_network:
                plt.show()

            if self.save_network:
                fig.savefig('%s.%s' % (self.save_network_name, self.save_network_type),
                            bbox_inches='tight', dpi=300)

        if context.graph and self.save_shapefile:
            logger.info("Saving network as shapefile")

            w = shapefile.Writer(target=self.save_shapefile_name, shapeType=shapefile.POLYLINE,
                                 autoBalance=True)
            w.field("name", "C")
            w.field("from", "C")
            w.field("to", "C")

            for es in context.graph.es:
                if 'directions' in es.attribute_names() and route in es['directions'] and es['directions'][route] != 0:
                    direction = es['directions'][route]
                    # reversed route?
                    if config.simulation_route_reverse:
                        if direction == 1:
                            direction = -1
                        elif direction == -1:
                            direction = 1

                    s = context.graph.vs.find(name=es['from']) # we can trust this value
                    t = context.graph.vs.find(name=es['to']) # we can trust this value

                    if direction == 1 or direction == 2:
                        w.line([[[s['geom'].x, s['geom'].y], [t['geom'].x, t['geom'].y]]])
                        w.record(es['name'], s['name'], t['name'])
                    if direction == -1 or direction == 2:
                        w.line([[[t['geom'].x, t['geom'].y], [s['geom'].x, s['geom'].y]]])
                        w.record(es['name'] + '_rev', t['name'], s['name'])

            w.close()

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'DebugDisplayPathsAndHubs'
