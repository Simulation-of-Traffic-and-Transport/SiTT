# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Save existing/updated paths and hubs to PostgreSQL database

Example configuration:
preparation:
  - class: PsqlSavePathsAndHubs
    module: preparation_modules.psql_save_paths_and_hubs
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

"""
import logging
from typing import List

import yaml
from sqlalchemy import create_engine, String, Table, Column, text, select, update, insert, Float

from sitt import Configuration, Context
from sitt.modules.preparation import PSQLBase

logger = logging.getLogger()


class PsqlSavePathsAndHubs(PSQLBase):
    """Save existing/updated paths and hubs to PostgreSQL database"""

    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', roads_table_name: str = 'topology.recroads', roads_geom_col: str = 'geom',
                 roads_index_col: str = 'id', roads_coerce_float: bool = True, roads_hub_a_id: str = 'hubaid',
                 roads_hub_b_id: str = 'hubbid', rivers_table_name: str = 'topology.recrivers',
                 rivers_geom_col: str = 'geom', rivers_index_col: str = 'id', river_coerce_float: bool = True,
                 rivers_hub_a_id: str = 'hubaid', rivers_hub_b_id: str = 'hubbid', rivers_width_m: str = 'width_m',
                 hubs_table_name: str = 'topology.rechubs', hubs_geom_col: str = 'geom',
                 hubs_index_col: str = 'id', hubs_coerce_float: bool = True, hubs_overnight: str = 'overnight',
                 hubs_extra_fields: List[str] = [], crs_no: str = 4326, connection: str | None = None):
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
        self.crs_no: str = crs_no
        """merge or overwrite"""

    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info(
                "Saving paths and hubs to PostgreSQL: " + self._create_connection_string(for_printing=True))

        # create connection string and connect to db
        db_string: str = self._create_connection_string()
        self.conn = create_engine(db_string).connect()

        # update roads
        if context.raw_roads is not None and len(context.raw_roads) > 0:
            updated = 0
            inserted = 0

            table_parts = self.roads_table_name.rpartition('.')
            idx_col = Column(self.roads_index_col)
            t = Table(table_parts[2], self.metadata_obj, idx_col, Column(self.roads_geom_col),
                      Column(self.roads_hub_a_id), Column(self.roads_hub_b_id), schema=table_parts[0])

            for idx, row in context.raw_roads.iterrows():
                data = {
                    t.c[self.roads_geom_col]: text(
                        String().literal_processor(dialect=self.conn.dialect)(
                            value="SRID=" + str(self.crs_no) + ";" + str(row.geom))),
                    t.c[self.roads_hub_a_id]: text(
                        String().literal_processor(dialect=self.conn.dialect)(value=str(row.hubaid))),
                    t.c[self.roads_hub_b_id]: text(
                        String().literal_processor(dialect=self.conn.dialect)(value=str(row.hubbid)))
                }

                # exists?
                s = select(idx_col).select_from(t).where(t.c[self.roads_index_col] == idx)

                # insert or update
                if self.conn.execute(s).rowcount == 0:
                    data[t.c[self.roads_index_col]] = text(
                        String().literal_processor(dialect=self.conn.dialect)(value=str(idx)))
                    stmt = insert(t).values(data)
                    inserted += 1
                else:
                    stmt = update(t).where(t.c[self.roads_index_col] == idx).values(data)
                    updated += 1
                self.conn.execute(stmt.compile(compile_kwargs={"literal_binds": True}))

            self.conn.commit()
            if logger.level <= logging.INFO:
                logger.info(f"Roads: {updated} updated, {inserted} inserted")

        # update rivers
        if context.raw_rivers is not None and len(context.raw_rivers) > 0:
            updated = 0
            inserted = 0

            table_parts = self.rivers_table_name.rpartition('.')
            idx_col = Column(self.rivers_index_col)
            t = Table(table_parts[2], self.metadata_obj, idx_col, Column(self.rivers_geom_col),
                      Column(self.rivers_hub_a_id), Column(self.rivers_hub_b_id), Column(self.rivers_width_m, Float),
                      schema=table_parts[0])

            for idx, row in context.raw_rivers.iterrows():
                data = {
                    t.c[self.rivers_geom_col]: text(
                        String().literal_processor(dialect=self.conn.dialect)(
                            value="SRID=" + str(self.crs_no) + ";" + str(row.geom))),
                    t.c[self.rivers_hub_a_id]: text(
                        String().literal_processor(dialect=self.conn.dialect)(value=str(row.hubaid))),
                    t.c[self.rivers_hub_b_id]: text(
                        String().literal_processor(dialect=self.conn.dialect)(value=str(row.hubbid))),
                    t.c[self.rivers_width_m]: row.width_m
                }

                # exists?
                s = select(idx_col).select_from(t).where(t.c[self.rivers_index_col] == idx)
                if self.conn.execute(s).rowcount == 0:
                    data[t.c[self.rivers_index_col]] = text(
                        String().literal_processor(dialect=self.conn.dialect)(value=str(idx)))
                    stmt = insert(t).values(data)
                    inserted += 1
                else:
                    stmt = update(t).where(t.c[self.rivers_index_col] == idx).values(data)
                    updated += 1
                self.conn.execute(stmt.compile(compile_kwargs={"literal_binds": True}))

            self.conn.commit()
            if logger.level <= logging.INFO:
                logger.info(f"Rivers: {updated} updated, {inserted} inserted")

        # update hubs
        if context.raw_hubs is not None and len(context.raw_hubs) > 0:
            updated = 0
            inserted = 0

            table_parts = self.hubs_table_name.rpartition('.')
            idx_col = Column(self.hubs_index_col)
            fields = [idx_col, Column(self.hubs_geom_col), Column(self.hubs_overnight)]
            for field in self.hubs_extra_fields:
                fields.append(Column(field))
            t = Table(table_parts[2], self.metadata_obj, *fields, schema=table_parts[0])

            for idx, row in context.raw_hubs.iterrows():
                data = {
                    t.c[self.hubs_geom_col]: text(
                        String().literal_processor(dialect=self.conn.dialect)(
                            value="SRID=" + str(self.crs_no) + ";" + str(row.geom))),
                    t.c[self.hubs_overnight]: text(
                        String().literal_processor(dialect=self.conn.dialect)(value=str(row.overnight) or '')),
                }
                for field in self.hubs_extra_fields:
                    if field in row and row[field] is not None:
                        v = row[field]
                    else:
                        v = ''
                    data[t.c[field]] = text(String().literal_processor(dialect=self.conn.dialect)(value=str(v)))

                # exists?
                s = select(idx_col).select_from(t).where(t.c[self.hubs_index_col] == idx)

                # insert or update
                if self.conn.execute(s).rowcount == 0:
                    data[t.c[self.hubs_index_col]] = text(
                        String().literal_processor(dialect=self.conn.dialect)(value=str(idx)))
                    stmt = insert(t).values(data)
                    inserted += 1
                else:
                    stmt = update(t).where(t.c[self.hubs_index_col] == idx).values(data)
                    updated += 1
                self.conn.execute(stmt.compile(compile_kwargs={"literal_binds": True}))

            self.conn.commit()
            if logger.level <= logging.INFO:
                logger.info(f"Hubs: {updated} updated, {inserted} inserted")

        # close connection
        self.conn.close()

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'PsqlSavePathsAndHubs'
