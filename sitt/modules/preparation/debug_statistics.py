# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Print statistics about a given data set"""
import logging

import numpy as np
import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()

class DebugStatistics(PreparationInterface):
    def run(self, config: Configuration, context: Context) -> Context:
        print(f"Statistics: {config.simulation_route}")
        print(f"============{'='*len(config.simulation_route)}\n")

        #### Number of nodes/edges
        print(f"Hubs:            {len(context.routes.vs)}")
        print(f"Overnight hubs:  {len(context.routes.vs.select(overnight=True))}")
        print(f"Edges:           {len(context.routes.es)}")
        print(f"Road edges:      {len(context.routes.es.select(type='road'))}")
        print(f"River edges:     {len(context.routes.es.select(type='river'))}")
        print(f"Lake edges:      {len(context.routes.es.select(type='lake'))}")

        #### Lengths
        print()
        print(f"Total length:    {np.sum(context.routes.es['length_m'])/1000.:.2f} km")
        print(f"Road length:     {np.sum(context.routes.es.select(type='road')['length_m'])/1000.:.2f} km")
        print(f"River length:    {np.sum(context.routes.es.select(type='river')['length_m'])/1000.:.2f} km")
        print(f"Lake length:     {np.sum(context.routes.es.select(type='lake')['length_m'])/1000.:.2f} km")

        #### Slopes
        print()
        # merge all slopes into one flat array
        slopes = np.hstack(context.routes.es['slopes']).flatten()
        # get all points in a flat array
        points = np.array([point for geo in context.routes.es['geom'] for point in geo.coords])

        # replace None with 0
        slopes[slopes == None] = 0.

        idx_max = slopes.argmax()
        max_slope_up = slopes[idx_max]*100
        point_max_slope_up = points[idx_max]
        if max_slope_up < 0.:
            print("Max slope up:    none")
        else:
            print(f"Max slope up:    {max_slope_up:.2f} % at <{point_max_slope_up[0]} {point_max_slope_up[1]}>")
        idx_min = slopes.argmin()
        max_slope_down = slopes[idx_min]*-100
        point_max_slope_down = points[idx_min]
        if max_slope_down < 0.:
            print("Max slope down:  none")
        else:
            print(f"Max slope down:  {max_slope_down:.2f} % at <{point_max_slope_down[0]} {point_max_slope_down[1]}>")

        up_m = np.array(context.routes.es['up_m'])
        down_m = np.array(context.routes.es['down_m'])
        up_m[up_m == None] = 0.
        down_m[down_m == None] = 0.

        print(f"Total up:        {np.sum(up_m):.0f} m")
        print(f"Total down:      {np.sum(down_m):.0f} m")

        #### Shortest paths
        print()
        print("Route lengths:")
        distances = context.routes.distances(source=config.simulation_starts, target=config.simulation_ends, weights='length_m', mode='out')
        for s_idx, start in enumerate(config.simulation_starts):
            for e_idx, end in enumerate(config.simulation_ends):
                print(f"{start} → {end}: {distances[s_idx][e_idx]/1000.:.2f} km")

        furthest_points = context.routes.farthest_points(weights='length_m')
        p1 = context.routes.vs[furthest_points[0]]['name']
        p2 = context.routes.vs[furthest_points[1]]['name']
        print(f"Maximum distance in graph: {furthest_points[2]/1000.:.2f} km between {p1} and {p2}")

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'DebugStatistics'
