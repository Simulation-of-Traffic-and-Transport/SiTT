# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Prepare raw paths and hubs anc precalculate then into a graph. This is the step before actual route creation.
Run this before CreateRoutes to create a graph of prepared routes."""
import logging

import geopandas as gpd
import igraph as ig
import numpy as np
import pandas as pd
import shapely.ops as sp_ops
import yaml
from pyproj import Transformer
from shapely.geometry import LineString, Point

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
        # transient data
        self.raw_river_hubs = []

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
            'source': {},
            'target': {},
            'length_m': {},
            'legs': {},
            'slopes': {},
            'geom': {},
            'type': {},
            'name': {}
        }
        # TODO: river width
        # TODO: river base speed

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
        river_hubs_added = set()

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
                path['source'][idx] = row.hubaid
                path['target'][idx] = row.hubbid
                path['length_m'][idx] = length
                path['legs'][idx] = np.array(legs, dtype=np.float64)
                path['slopes'][idx] = np.array(slopes, dtype=np.float64)
                path['geom'][idx] = row.geom
                path['type'][idx] = path_type
                path['type'][idx] = path_type
                path['name'][idx] = idx

                # prepare river hubs, might be needed later
                if path_type == "river":
                    if row.hubaid not in river_hubs_added:
                        self.raw_river_hubs.append({'name': row.hubaid, 'geom': Point(row.geom.coords[0]),
                                                    'overnight': 'n', 'harbor': 'n', 'water_node': 'y'})
                        river_hubs_added.add(row.hubaid)

                    if row.hubbid not in river_hubs_added:
                        self.raw_river_hubs.append({'name': row.hubbid, 'geom': Point(row.geom.coords[-1]),
                                                    'overnight': 'n', 'harbor': 'n', 'water_node': 'y'})
                        river_hubs_added.add(row.hubbid)

    def calculate_graph(self, path: pd.DataFrame, config: Configuration, context: Context):
        # create paths from dataframe
        g: ig.Graph = ig.Graph.TupleList(path.itertuples(index=False), directed=False, edge_attrs=['length_m', 'legs',
                                                                                                   'slopes', 'geom',
                                                                                                   'type', 'name'])
        # add hub data, too
        if context.raw_hubs is not None and len(context.raw_hubs) > 0:
            cols = context.raw_hubs.columns.tolist()

            for idx, row in context.raw_hubs.iterrows():
                vertex = g.vs.find(name=idx)
                c = 0
                for el in row.array:
                    vertex[cols[c]] = el
                    c += 1

        # add river hubs, if there is no data
        for hub in self.raw_river_hubs:
            vertex: ig.Vertex = g.vs.find(name=hub['name'])
            if vertex['geom'] is None:  # only update, if there is no geometry
                vertex.update_attributes(**hub)

        context.graph = g

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CalculatePathsAndHubs"
