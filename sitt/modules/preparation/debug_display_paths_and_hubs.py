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
    def __init__(self, route: str | None = None, draw_network: bool = True, show_network: bool = True, save_network: bool = False,
                 save_network_name: str = 'network', save_network_type: str = 'png', display_routes: bool = True,
                 start: str | None = None, end: str | None = None, save_shapefile: bool = False,
                 save_shapefile_name: str = 'network'):
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
        """Route to be considered - required"""
        self.route: str | None = route
        """end hub id of example route"""
        self.save_shapefile: bool = save_shapefile
        """save network as shapefile"""
        self.save_shapefile_name: str = save_shapefile_name
        """Name of the shapefile to save"""

    def run(self, config: Configuration, context: Context) -> Context:
        if self.route is None:
            logger.error("No route specified.")
            return context

        # convert route to lower case for case-insensitive comparison
        route = self.route.lower()

        if context.graph and self.draw_network:
            logger.info("Displaying paths and hubs")

            vertices: set[str] = set()

            fig, ax = plt.subplots()

            for es in context.graph.es:
                if 'directions' in es.attribute_names() and route in es['directions'] and es['directions'][route] != 0:
                    dir = es['directions'][route]

                    s = context.graph.vs.find(name=es['from']) # we can trust this value
                    t = context.graph.vs.find(name=es['to']) # we can trust this value

                    vertices.add(s.index)
                    vertices.add(t.index)

                    arrow_style = '->'
                    if dir == -1:
                        arrow_style = '<-'
                    if dir == 2:
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
            plt.title(self.route)

            if self.show_network:
                plt.show()

            if self.save_network:
                fig.savefig('%s.%s' % (self.save_network_name, self.save_network_type),
                            bbox_inches='tight', dpi=300)

        # ### TODO
        if context.graph and self.save_shapefile:
            logger.info("Saving network as shapefile")

            w = shapefile.Writer(target=self.save_shapefile_name, shapeType=shapefile.POLYLINE,
                                 autoBalance=True)
            w.field("name", "C")
            w.field("from", "C")
            w.field("to", "C")

            for es in context.graph.es:
                if 'directions' in es.attribute_names() and route in es['directions'] and es['directions'][route] != 0:
                    dir = es['directions'][route]

                    s = context.graph.vs.find(name=es['from']) # we can trust this value
                    t = context.graph.vs.find(name=es['to']) # we can trust this value

                    if dir == 1 or dir == 2:
                        w.line([[[s['geom'].x, s['geom'].y], [t['geom'].x, t['geom'].y]]])
                        w.record(es['name'], s['name'], t['name'])
                    if dir == -1 or dir == 2:
                        w.line([[[t['geom'].x, t['geom'].y], [s['geom'].x, s['geom'].y]]])
                        w.record(es['name'] + '_rev', t['name'], s['name'])

            w.close()

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'DebugDisplayPathsAndHubs'
