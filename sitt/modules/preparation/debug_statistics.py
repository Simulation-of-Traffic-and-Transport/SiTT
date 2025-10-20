# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Print statistics about a given data set"""
import logging
import csv

import numpy as np
import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()

class DebugStatistics(PreparationInterface):
    def __init__(self, save: bool = False, filename: str = 'statistics.csv', append: bool = False) -> None:
        super().__init__()
        self.save: bool = save
        """Save statistics to a csv file?"""
        self.filename: str = filename
        """Filename for the csv file"""
        self.append: bool = append
        """Append to existing file?"""

    def run(self, config: Configuration, context: Context) -> Context:
        writer = None
        if self.save:
            f = open(self.filename, 'a' if self.append else 'w', newline='')
            writer = csv.writer(f)

        print(f"Statistics: {config.simulation_route}")
        print(f"============{'='*len(config.simulation_route)}\n")

        #### Number of nodes/edges
        no_hubs = len(context.routes.vs)
        overnight_hubs = len(context.routes.vs.select(overnight=True))
        no_edges = len(context.routes.es)
        road_edges = len(context.routes.es.select(type='road'))
        river_edges = len(context.routes.es.select(type='river'))
        lake_edges = len(context.routes.es.select(type='lake'))

        print(f"Hubs:            {no_hubs}")
        print(f"Overnight hubs:  {overnight_hubs}")
        print(f"Edges:           {no_edges}")
        print(f"Road edges:      {road_edges}")
        print(f"River edges:     {river_edges}")
        print(f"Lake edges:      {lake_edges}")

        #### Lengths
        total_length = np.sum(context.routes.es['length_m'])/1000.
        road_length = np.sum(context.routes.es.select(type='road')['length_m'])/1000.
        river_length = np.sum(context.routes.es.select(type='river')['length_m'])/1000.
        lake_length = np.sum(context.routes.es.select(type='lake')['length_m'])/1000.

        print()
        print(f"Total length:    {total_length:.2f} km")
        print(f"Road length:     {road_length:.2f} km")
        print(f"River length:    {river_length:.2f} km")
        print(f"Lake length:     {lake_length:.2f} km")

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
        up_m = np.sum(up_m)
        down_m = np.sum(down_m)

        print(f"Total up:        {up_m:.0f} m")
        print(f"Total down:      {down_m:.0f} m")

        #### Shortest paths
        print()
        print("Route lengths:")
        distances = context.routes.distances(source=config.simulation_starts, target=config.simulation_ends, weights='length_m', mode='out')
        shortest_len = np.min(distances)
        longest_len = np.max(distances)
        shortest_start = None
        shortest_end = None
        longest_start = None
        longest_end = None
        for s_idx, start in enumerate(config.simulation_starts):
            for e_idx, end in enumerate(config.simulation_ends):
                print(f"{start} → {end}: {distances[s_idx][e_idx]/1000.:.2f} km")
                if distances[s_idx][e_idx] == shortest_len:
                    shortest_start = start
                    shortest_end = end
                if distances[s_idx][e_idx] == longest_len:
                    longest_start = start
                    longest_end = end

        furthest_points = context.routes.farthest_points(weights='length_m')
        p1 = context.routes.vs[furthest_points[0]]['name']
        p2 = context.routes.vs[furthest_points[1]]['name']
        print(f"Maximum distance in graph: {furthest_points[2]/1000.:.2f} km between {p1} and {p2}")

        if writer is not None:
            writer.writerow([config.simulation_route, no_hubs, overnight_hubs, no_edges, road_edges, river_edges,
                             lake_edges, total_length, road_length, river_length, lake_length, max_slope_up,
                             point_max_slope_up[0], point_max_slope_up[1], max_slope_down, point_max_slope_down[0],
                             point_max_slope_down[1], up_m, down_m, np.min(distances)/1000., shortest_start,
                             shortest_end, longest_len/1000., longest_start, longest_end])
            f.close()

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'DebugStatistics'
