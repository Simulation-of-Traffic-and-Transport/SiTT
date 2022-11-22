# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Prepare raw roads and hubs anc precalculate then into a graph"""
import logging

import networkx as nx
import numpy as np
import pandas as pd
import shapely.ops as sp_ops
import yaml
from pyproj import Transformer
from shapely.geometry import LineString

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class CalculateRoadsAndHubs(PreparationInterface):
    """Prepare raw roads and hubs anc precalculate then into a graph"""

    def __init__(self, crs_from: str = 'EPSG:4326', crs_to: str = 'EPSG:32633', always_xy: bool = True,
                 length_including_heights: bool = False):
        super().__init__()
        self.crs_from: str = crs_from
        self.crs_to: str = crs_to
        self.always_xy: bool = always_xy
        self.length_including_heights: bool = length_including_heights

    def run(self, config: Configuration, context: Context) -> Context:
        logger.info("Preparing graph...")

        # prepare data to be read into graph - this will precalculate leg lengths and so on
        context = self.prepare_data(config, context)

        # create actual graph
        context = self.calculate_graph(config, context)

        return context

    def prepare_data(self, config: Configuration, context: Context) -> Context:
        """
        Prepare/normalize raw data - precalculate distances of roads

        :param config: configuration
        :param context: context object
        :return: updated context
        """
        transformer = Transformer.from_crs(self.crs_from, self.crs_to, always_xy=self.always_xy)

        if context.raw_roads is not None and len(context.raw_roads) > 0:
            roads = {
                'length_m': {},
                'legs': {},
                'slopes': {},
                'geom': {},
                'hubaid': {},
                'hubbid': {},
                'source': {},
                'target': {},
                'uid': {}  # TODO: remove
            }

            for idx, row in context.raw_roads.iterrows():
                line = LineString(row.geom)

                # Calculate single legs
                length = 0.0
                legs = []  # in m
                slopes = []  # in degrees

                last_coord = None
                for coord in line.coords:
                    if last_coord is not None:
                        # distance calculation for each leg
                        leg = sp_ops.transform(transformer.transform, LineString([last_coord, coord]))
                        leg_length = leg.length

                        # asc/desc
                        diff = last_coord[2] - coord[2]

                        # add height to length calculation
                        if self.length_including_heights:
                            leg_length = np.sqrt([leg_length * leg_length + diff * diff])[0]

                        # logger.info("%f, %f", diff, leg_length)
                        if leg_length > 0:
                            slope = np.degrees(np.arctan(diff / leg_length))
                        else:
                            slope = 0.0

                        legs.append(leg_length)
                        slopes.append(slope)
                        length += leg_length

                    last_coord = coord

                # recreate roads
                roads['length_m'][idx] = length
                roads['legs'][idx] = np.array(legs, dtype=np.float64)
                roads['slopes'][idx] = np.array(slopes, dtype=np.float64)
                roads['geom'][idx] = row.geom
                roads['hubaid'][idx] = row.hubaid
                roads['hubbid'][idx] = row.hubbid
                # these attributes are in preparation of calculate_graph below
                roads['source'][idx] = row.hubaid
                roads['target'][idx] = row.hubbid
                roads['uid'][idx] = idx

                context.raw_roads = pd.DataFrame(roads)

        return context

    def calculate_graph(self, config: Configuration, context: Context):
        # create roads as multigraph because they can be traversed in both ways
        g: nx.MultiGraph = nx.from_pandas_edgelist(context.raw_roads, edge_key='uid',
                                                   create_using=nx.MultiGraph, edge_attr=True)

        # add hub data, too
        if context.raw_hubs is not None and len(context.raw_hubs) > 0:
            for idx, row in context.raw_hubs.iterrows():
                g.add_node(idx, **row)

        context.graph = g

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CalculateRoadsAndHubs"
