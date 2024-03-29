# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Read paths and hubs from PostgreSQL database

Example configuration:
preparation:
  - class: PsqlReadPathsAndHubs
    module: preparation_modules.psql_read_paths_and_hubs
    args:
      server: !Env "${PSQL_SERVER}"
      port: !Env ${PSQL_PORT}
      db: !Env "${PSQL_DB}"
      user: !Env "${PSQL_USER}"
      password: !Env "${PSQL_PASSWORD}"
      roads_table_name: topology.recroads
      roads_geom_col: geom
      roads_index_col: recroadid
      roads_coerce_float: true
      roads_hub_a_id: hubaid
      roads_hub_b_id: hubbid
      rivers_table_name: topology.recrivers
      rivers_geom_col: geom
      rivers_index_col: recriverid
      rivers_coerce_float: true
      rivers_hub_a_id: hubaid
      rivers_hub_b_id: hubbid
      hubs_table_name: topology.rechubs
      hubs_geom_col: geom
      hubs_index_col: rechubid
      hubs_coerce_float: true
      hubs_overnight: overnight
      hubs_extra_fields:
        - hubtypeid
        - storage
        - interchange
        - market
      strategy: merge

"""
import logging
from typing import List

import geopandas as gpd
import pandas as pd
import yaml
from sqlalchemy import create_engine, Table, Column, select

from sitt import Configuration, Context
from sitt.modules.preparation import PSQLBase

logger = logging.getLogger()


class PsqlReadPathsAndHubs(PSQLBase):
    """Read paths and hubs from PostgreSQL database"""

    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', roads_table_name: str = 'topology.recroads', roads_geom_col: str = 'geom',
                 roads_index_col: str = 'id', roads_coerce_float: bool = True, roads_hub_a_id: str = 'hubaid',
                 roads_hub_b_id: str = 'hubbid', rivers_table_name: str = 'topology.recrivers',
                 rivers_geom_col: str = 'geom', rivers_index_col: str = 'id', river_coerce_float: bool = True,
                 rivers_hub_a_id: str = 'hubaid', rivers_hub_b_id: str = 'hubbid', rivers_width_m: str = 'width_m',
                 hubs_table_name: str = 'topology.rechubs', hubs_geom_col: str = 'geom',
                 hubs_index_col: str = 'id', hubs_coerce_float: bool = True, hubs_overnight: str = 'overnight',
                 hubs_extra_fields: List[str] = [], strategy: str = 'merge', connection: str | None = None):
        # connection data - should be set/overwritten by config
        super().__init__(server, port, db, user, password, connection)
        # db data - where to query from
        self.roads_table_name: str = roads_table_name
        self.roads_geom_col: str = roads_geom_col
        self.roads_index_col: str = roads_index_col
        self.roads_coerce_float: bool = roads_coerce_float
        self.roads_hub_a_id: str = roads_hub_a_id
        self.roads_hub_b_id: str = roads_hub_b_id
        self.rivers_table_name: str = rivers_table_name
        self.rivers_geom_col: str = rivers_geom_col
        self.rivers_index_col: str = rivers_index_col
        self.rivers_coerce_float: bool = river_coerce_float
        self.rivers_hub_a_id: str = rivers_hub_a_id
        self.rivers_hub_b_id: str = rivers_hub_b_id
        self.rivers_width_m: str = rivers_width_m
        self.hubs_table_name: str = hubs_table_name
        self.hubs_geom_col: str = hubs_geom_col
        self.hubs_index_col: str = hubs_index_col
        self.hubs_coerce_float: bool = hubs_coerce_float
        self.hubs_overnight: str = hubs_overnight
        self.hubs_extra_fields: List[str] = hubs_extra_fields
        self.strategy: str = strategy
        """merge or overwrite"""

    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info(
                "Reading roads, rivers and hubs from PostgreSQL: " + self._create_connection_string(for_printing=True))

        # create connection string and connect to db
        db_string: str = self._create_connection_string()
        self.conn = create_engine(db_string).connect()

        # get roads - create statement via sql alchemy
        table_parts = self.roads_table_name.rpartition('.')
        t = Table(table_parts[2], self.metadata_obj, schema=table_parts[0])
        geom_col = Column(self.roads_geom_col).label('geom')
        s = select(Column(self.roads_index_col).label('id'), geom_col,
                   Column(self.roads_hub_a_id).label('hubaid'),
                   Column(self.roads_hub_b_id).label('hubbid')).where(geom_col.is_not(None)).select_from(t)
        raw_roads = gpd.GeoDataFrame.from_postgis(str(s.compile()),
                                                  self.conn, geom_col='geom',
                                                  index_col='id',
                                                  coerce_float=self.roads_coerce_float)

        logger.info('Read %d road(s) from PostgreSQL', len(raw_roads))

        # for idx, row in geoms.iterrows():
        #    print(idx)

        # get rivers - create statement via sql alchemy
        table_parts = self.rivers_table_name.rpartition('.')
        t = Table(table_parts[2], self.metadata_obj, schema=table_parts[0])
        geom_col = Column(self.rivers_geom_col).label('geom')
        s = select(Column(self.rivers_index_col).label('id'), geom_col,
                   Column(self.rivers_hub_a_id).label('hubaid'),
                   Column(self.rivers_hub_b_id).label('hubbid'),
                   Column(self.rivers_width_m).label('width_m')).where(geom_col.is_not(None)).select_from(t)

        raw_rivers = gpd.GeoDataFrame.from_postgis(str(s.compile()),
                                                   self.conn, geom_col='geom',
                                                   index_col='id',
                                                   coerce_float=self.rivers_coerce_float)
        logger.info('Read %d rivers(s) from PostgreSQL', len(raw_rivers))

        # for idx, row in geoms.iterrows():
        #    print(idx)

        # get hubs - create statement via sql alchemy
        table_parts = self.hubs_table_name.rpartition('.')
        t = Table(table_parts[2], self.metadata_obj, schema=table_parts[0])
        fields = [Column(self.hubs_index_col).label('id'), Column(self.hubs_geom_col).label('geom'),
                  Column(self.hubs_overnight).label('overnight')]
        for field in self.hubs_extra_fields:
            fields.append(Column(field))
        s = select(*fields).select_from(t)
        raw_hubs = gpd.GeoDataFrame.from_postgis(str(s.compile()), self.conn,
                                                 geom_col='geom',
                                                 index_col='id',
                                                 coerce_float=self.hubs_coerce_float)

        logger.info('Read %d hub(s) from PostgreSQL', len(raw_hubs))

        # close connection
        self.conn.close()

        return self._merge_or_overwrite(context, raw_roads, raw_rivers, raw_hubs)

    def _merge_or_overwrite(self, context: Context, raw_roads: gpd.geodataframe.GeoDataFrame,
                            raw_rivers: gpd.geodataframe.GeoDataFrame,
                            raw_hubs: gpd.geodataframe.GeoDataFrame) -> Context:
        if self.strategy == 'overwrite':
            return self._overwrite(context, raw_roads, raw_rivers, raw_hubs)
        if self.strategy == 'merge':
            return self._merge(context, raw_roads, raw_rivers, raw_hubs)
        logger.warning("unknown strategy %s, defaulting to \"merge\"", self.strategy)
        return self._merge(context, raw_roads, raw_rivers, raw_hubs)

    def _overwrite(self, context: Context, raw_roads: gpd.geodataframe.GeoDataFrame,
                   raw_rivers: gpd.geodataframe.GeoDataFrame,
                   raw_hubs: gpd.geodataframe.GeoDataFrame) -> Context:
        context.raw_roads = raw_roads
        context.raw_rivers = raw_rivers
        context.raw_hubs = raw_hubs
        return context

    def _merge(self, context: Context, raw_roads: gpd.geodataframe.GeoDataFrame,
               raw_rivers: gpd.geodataframe.GeoDataFrame,
               raw_hubs: gpd.geodataframe.GeoDataFrame) -> Context:
        if context.raw_roads is None:
            context.raw_roads = raw_roads
        else:
            context.raw_roads = pd.concat([context.raw_roads, raw_roads], copy=False).drop_duplicates()

        if context.raw_rivers is None:
            context.raw_rivers = raw_rivers
        else:
            context.raw_rivers = pd.concat([context.raw_rivers, raw_rivers], copy=False).drop_duplicates()

        # let pandas do the bulk of work
        context.raw_hubs = pd.concat([context.raw_hubs, raw_hubs], copy=False).drop_duplicates()
        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'PsqlReadPathsAndHubs'
