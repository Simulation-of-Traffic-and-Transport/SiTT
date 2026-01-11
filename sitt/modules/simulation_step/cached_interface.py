# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
import json
from abc import ABC
from urllib import parse

from sqlalchemy import create_engine, Connection, MetaData, Column, Table, String, Float, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from sitt import SimulationStepInterface


class CachedInterface(SimulationStepInterface, ABC):
    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', schema: str = 'sitt', connection: str | None = None):
        super().__init__()
        # connection data - should be set/overwritten by config
        self.server = server
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.schema = schema
        # runtime settings
        self.connection: str | None = connection # this is used to automatically load the connection from the config
        self.conn: Connection | None = None
        self.metadata_obj: MetaData = MetaData(schema=self.schema)
        self.is_initialized = False


    def _create_connection_string(self, for_printing=False):
        """
        Create DB connection string

        :param for_printing: hide password, so connection can be printed
        """
        if for_printing:
            return 'postgresql://' + self.user + ':***@' + self.server + ':' + str(
                self.port) + '/' + self.db + ' (schema:' + self.schema + ')'
        else:
            return 'postgresql://' + self.user + ':' + parse.quote_plus(
                self.password) + '@' + self.server + ':' + str(
                self.port) + '/' + self.db

    def get_connection(self) -> Connection:
        """
        Load or initialize the connection to the database.
        """
        if self.conn is None or self.conn.closed:
            # create connection string and connect to db
            db_string: str = self._create_connection_string()
            self.conn = create_engine(db_string).connect()

        return self.conn

    def get_step_cache_table(self) -> Table:
        schema_key = self.schema + '.step_cache'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        id_col = Column('id', String, primary_key=True) # hash
        config_key_col = Column('config_key', String, nullable=False, index=True, default='default') # for bulk deletes
        generator_id_col = Column('generator', String, nullable=False, index=True) # for bulk deletes
        time_taken_col = Column('time_taken', Float, nullable=False)
        time_for_legs_col = Column('time_for_legs', String, nullable=False)

        return Table("step_cache", self.metadata_obj, id_col, config_key_col, generator_id_col, time_taken_col, time_for_legs_col, schema=self.schema)

    def _initialize(self):
        if self.is_initialized:
            return

        # initialize table data
        conn = self.get_connection()

        self.get_step_cache_table()
        self.metadata_obj.create_all(conn)

        conn.commit() # commit the table creation

        self.is_initialized = True

    def _save_to_cache(self, cache_key: str, config_key: str, generator_id: str, time_taken: float, time_for_legs: list[float]):
        stmt = pg_insert(self.get_step_cache_table()).values(id=cache_key, config_key=config_key, generator=generator_id, time_taken=time_taken, time_for_legs=json.dumps(time_for_legs)).on_conflict_do_nothing()
        self.conn.execute(stmt)
        self.conn.commit()

    def _load_from_cache(self, cache_key: str, config_key: str) -> tuple[bool, float, str]:
        stmt = select(self.get_step_cache_table()).where(self.get_step_cache_table().c.id == cache_key,
                 self.get_step_cache_table().c.config_key == config_key)

        result = self.conn.execute(stmt).fetchone()
        if result is None:
            return False, 0., ''

        return True, result.time_taken, result.time_for_legs
