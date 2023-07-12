# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Prepare raw paths and hubs anc precalculate then into a graph. This is the step before actual route creation.
Run this before CreateRoutes to create a graph of prepared routes."""
import logging

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import shapely.ops as sp_ops
import yaml
from pyproj import Transformer
from shapely.geometry import LineString

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class CalculatePathsAndHubs(PreparationInterface):
    """
    Prepare raw paths and hubs anc precalculate then into a graph. This is the step before actual route creation.
    Run this before CreateRoutes to create a graph of prepared routes.
    """

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
        path = self.prepare_data(config, context)

        # create actual graph
        context = self.calculate_graph(path, config, context)

        return context

    def prepare_data(self, config: Configuration, context: Context) -> pd.DataFrame:
        """
        Prepare/normalize raw data - precalculate distances of roads and rivers

        :param config: configuration
        :param context: context object
        :return: updated context
        """
        transformer = Transformer.from_crs(self.crs_from, self.crs_to, always_xy=self.always_xy)

        # define path data
        path = {
            'length_m': {},
            'legs': {},
            'slopes': {},
            'geom': {},
            'hubaid': {},
            'hubbid': {},
            'source': {},
            'target': {},
            'type': {},
            'uid': {}
        }

        self._prepare_data_for_path(path, context.raw_roads, transformer)
        self._prepare_data_for_path(path, context.raw_rivers, transformer, path_type="river")

        return pd.DataFrame(path)

    def _prepare_data_for_path(self, path: dict, field: gpd.geodataframe.GeoDataFrame,
                               transformer: Transformer, path_type: str = "road") -> None:
        """
        Helper function for the actual work done above (prepare_data)

        :param path: object containing data
        :param field: field, either context.raw_roads or context.raw_rivers
        :param transformer: transformer defined above
        :param path_type: road or river
        :return: None
        """
        if field is not None and len(field) > 0:
            for idx, row in field.iterrows():
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

                # recreate paths
                path['length_m'][idx] = length
                path['legs'][idx] = np.array(legs, dtype=np.float64)
                path['slopes'][idx] = np.array(slopes, dtype=np.float64)
                path['geom'][idx] = row.geom
                path['hubaid'][idx] = row.hubaid
                path['hubbid'][idx] = row.hubbid
                # these attributes are in preparation of calculate_graph below
                path['source'][idx] = row.hubaid
                path['target'][idx] = row.hubbid
                path['type'][idx] = path_type
                path['uid'][idx] = idx

    def calculate_graph(self, path: pd.DataFrame, config: Configuration, context: Context):
        # create paths as multigraph because all can be traversed in both ways (at least in theory)
        g: nx.MultiGraph = nx.from_pandas_edgelist(path, edge_key='uid',
                                                     create_using=nx.MultiGraph, edge_attr=True)

        # add hub data, too
        if context.raw_hubs is not None and len(context.raw_hubs) > 0:
            for idx, row in context.raw_hubs.iterrows():
                g.add_node(idx, **row)

        # set frozen graph to prevent changes
        context.graph = nx.freeze(g)

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CalculatePathsAndHubs"
