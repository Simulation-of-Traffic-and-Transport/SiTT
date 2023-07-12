# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Take river graph data and calculate widths of rivers based on shapes in a database. Can be run right after
PsqlConstructRiverPaths."""

import logging
import sys

import yaml
from shapely import LineString, MultiLineString, Point, wkb, get_parts, get_coordinates, prepare, destroy_prepared, \
    line_merge
from sqlalchemy import create_engine, Table, Column, select, literal_column, func, text

from sitt import Configuration, Context
from sitt.modules.preparation import PSQLBase

logger = logging.getLogger()


class PsqlCalculateRiverWidths(PSQLBase):
    """Take river graph data and calculate widths of rivers based on shapes in a database. Can be run right after
    PsqlConstructRiverPaths."""

    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', water_lines_table_name: str = 'topology.water_lines',
                 water_lines_geom: str = 'geom', crs_no: str = 4326, connection: str | None = None):
        """Initialize the PSQL model."""
        super().__init__(server, port, db, user, password, connection)
        self.water_lines_table_name: str = water_lines_table_name
        self.water_lines_geom: str = water_lines_geom
        self.crs_no: str = crs_no

    def run(self, config: Configuration, context: Context) -> Context:
        if self.skip:
            logger.info("Skipping PsqlCalculateRiverWidths due to setting")
            return context

        logger.info(
            "Calculating river widths using PostgreSQL: " + self._create_connection_string(for_printing=True))

        # create connection string and connect to db
        db_string: str = self._create_connection_string()
        self.conn = create_engine(db_string).connect()

        table_parts = self.water_lines_table_name.rpartition('.')
        t = Table(table_parts[2], self.metadata_obj, schema=table_parts[0])
        geom_col = Column(self.water_lines_geom).label('geom')

        # calculate width of river paths
        for idx, river in context.raw_rivers.iterrows():
            coords = river.geom.coords
            # check for harbors and delete coordinates at beginning or end of the river sequence, because harbors will
            # dilute the true width of the river
            if self._is_harbor(river.hubaid, context):
                coords = coords[1:]
            if self._is_harbor(river.hubbid, context):
                coords = coords[0:-1]
            if len(coords) == 0:
                print("TODO: empty river sequence")
                continue
            if len(coords) == 1:
                geom = Point(coords)
            else:
                geom = LineString(coords)

            # let postgis do the actual calculation
            field = func.ST_DistanceSpheroid(geom_col,
                                             literal_column("'SRID=" + str(self.crs_no) + ";" + str(geom) + "'"))
            s = select(field).select_from(t).limit(1).order_by(field)
            result = self.conn.execute(s).fetchone()

            # approximate minimum width of river = closest distance * 2 (since we have median axes)
            context.raw_rivers.loc[idx, ['width_m']] = result[0]*2

        # close connection
        self.conn.close()

        return context

    def _connected_harbors(self, river, context: Context) -> list[str]:
        harbors = []

        if self._is_harbor(river.hubaid, context):
            harbors.append(str(river.hubaid))
        if self._is_harbor(river.hubbid, context):
            harbors.append(str(river.hubbid))

        return harbors

    def _is_harbor(self, hubid, context: Context) -> bool:
        """return true if hub id is a harbor in raw_hubs context"""
        try:
            return str(context.raw_hubs.loc[[hubid]].harbor.values[0]) == 'y'
        except KeyError:
            return False

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'PsqlCalculateRiverWidths'
