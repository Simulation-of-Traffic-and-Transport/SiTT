"""
Read roads and hubs from PostgreSQL database

Example configuration:
preparation:
  - class: PsqlReadRoadsAndHubs
    module: preparation_modules.psql_read_roads_and_hubs
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
import urllib.parse

import geopandas as gp
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import table, column, select

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class PsqlReadRoadsAndHubs(PreparationInterface):
    """Read roads and hubs from PostgreSQL database"""

    def __init__(self):
        # connection data - should be set/overwritten by config
        super().__init__()
        self.server: str = 'localhost'
        self.port: int = 5432
        self.db: str = 'sitt'
        self.user: str = 'postgres'
        self.password: str = 'postgres'
        # db data - where to query from
        self.roads_table_name = 'topology.recroads'
        self.roads_geom_col = 'geom'
        self.roads_index_col = 'id'
        self.roads_coerce_float = True
        self.roads_hub_a_id = 'hubaid'
        self.roads_hub_b_id = 'hubbid'
        self.hubs_table_name = 'topology.rechubs'
        self.hubs_geom_col = 'geom'
        self.hubs_index_col = 'id'
        self.hubs_coerce_float = True
        self.hubs_overnight = 'overnight'
        self.hubs_extra_fields = []
        self.strategy = 'merge'
        """merge or overwrite"""
        # runtime settings
        self.connection: str | None = None

    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info(
                "Reading roads and hubs from PostgreSQL: " + self._create_connection_string(for_printing=True))

        # create connection string and connect to db
        db_string: str = self._create_connection_string()
        conn = create_engine(db_string)

        # get roads - create statement via sql alchemy
        table_parts = self.roads_table_name.rpartition('.')
        t = table(table_parts[2], schema=table_parts[0])
        geom_col = column(self.roads_geom_col).label('geom')
        s = select(column(self.roads_index_col).label('id'), geom_col,
                   column(self.roads_hub_a_id).label('hubaid'),
                   column(self.roads_hub_b_id).label('hubbid')).where(geom_col.is_not(None)).select_from(t)
        raw_roads = gp.GeoDataFrame.from_postgis(s.compile(),
                                                         conn, geom_col='geom',
                                                         index_col='id',
                                                         coerce_float=self.roads_coerce_float)

        logger.info('Read %d road(s) from PostgreSQL', len(raw_roads))

        # for idx, row in geoms.iterrows():
        #    print(idx)

        # get hubs - create statement via sql alchemy
        table_parts = self.hubs_table_name.rpartition('.')
        t = table(table_parts[2], schema=table_parts[0])
        fields = [column(self.hubs_index_col).label('id'), column(self.hubs_geom_col).label('geom'),
                   column(self.hubs_overnight).label('overnight')]
        for field in self.hubs_extra_fields:
            fields.append(column(field))
        s = select(fields).select_from(t)
        raw_hubs = gp.GeoDataFrame.from_postgis(s.compile(), conn,
                                             geom_col='geom',
                                             index_col='id',
                                             coerce_float=self.hubs_coerce_float)

        logger.info('Read %d hub(s) from PostgreSQL', len(raw_hubs))

        return self._merge_or_overwrite(context, raw_roads, raw_hubs)

        return context

    def _merge_or_overwrite(self, context: Context, raw_roads: gp.geodataframe.GeoDataFrame, raw_hubs: gp.geodataframe.GeoDataFrame) -> Context:
        if self.strategy == 'overwrite':
            return self._overwrite(context, raw_roads, raw_hubs)
        if self.strategy == 'merge':
            return self._merge(context, raw_roads, raw_hubs)
        logger.warning("unknown strategy %s, defaulting to \"merge\"", self.strategy)
        return self._merge(context, raw_roads, raw_hubs)

    def _overwrite(self, context: Context, raw_roads: gp.geodataframe.GeoDataFrame, raw_hubs: gp.geodataframe.GeoDataFrame) -> Context:
        context.raw_roads = raw_roads
        context.raw_hubs = raw_hubs
        return context

    def _merge(self, context: Context, raw_roads: gp.geodataframe.GeoDataFrame, raw_hubs: gp.geodataframe.GeoDataFrame) -> Context:
        # new entry?
        if context.raw_roads is None:
            return self._overwrite(context, raw_roads, raw_hubs)

        # let pandas do the bulk of work
        context.raw_roads = pd.concat([context.raw_roads, raw_roads], copy=False).drop_duplicates()
        context.raw_hubs = pd.concat([context.raw_hubs, raw_hubs], copy=False).drop_duplicates()
        return context

    def _create_connection_string(self, for_printing=False):
        """
        Create DB connection string

        :param for_printing: hide password, so connection can be printed
        """
        if for_printing:
            return 'postgresql://' + self.user + ':***@' + self.server + ':' + str(
                self.port) + '/' + self.db
        else:
            return 'postgresql://' + self.user + ':' + urllib.parse.quote_plus(
                self.password) + '@' + self.server + ':' + str(
                self.port) + '/' + self.db

    def __str__(self):
        return 'PsqlReadRoadsAndHubs'
