"""
Save existing/updated roads and hubs to PostgreSQL database

Example configuration:
preparation:
  - class: PsqlSaveRoadsAndHubs
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

"""
import logging
import urllib.parse

from sqlalchemy import create_engine, String
from sqlalchemy.sql import table, column, text

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class PsqlSaveRoadsAndHubs(PreparationInterface):
    """Save existing/updated roads and hubs to PostgreSQL database"""

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
                "Saving roads and hubs to PostgreSQL: " + self._create_connection_string(for_printing=True))

        # create connection string and connect to db
        db_string: str = self._create_connection_string()
        conn = create_engine(db_string)

        # update roads
        if context.raw_roads is not None and len(context.raw_roads) > 0:
            table_parts = self.roads_table_name.rpartition('.')
            t = table(table_parts[2], column(self.roads_index_col), column(self.roads_geom_col),
                      column(self.roads_hub_a_id), column(self.roads_hub_b_id), schema=table_parts[0])

            for idx, row in context.raw_roads.iterrows():
                stmt = t.update() \
                    .ordered_values(
                    (t.c[self.roads_geom_col],
                     text(String('').literal_processor(dialect=conn.dialect)(value=str(row.geom)))),
                    (t.c[self.roads_hub_a_id],
                     text(String('').literal_processor(dialect=conn.dialect)(value=row.hubaid))),
                    (t.c[self.roads_hub_b_id],
                     text(String('').literal_processor(dialect=conn.dialect)(value=row.hubbid))),
                ) \
                    .where(t.c[self.roads_index_col] == idx)
                conn.execute(stmt.compile(compile_kwargs={"literal_binds": True}))

        # update hubs
        if context.raw_hubs is not None and len(context.raw_hubs) > 0:
            table_parts = self.hubs_table_name.rpartition('.')
            fields = [column(self.hubs_index_col), column(self.hubs_geom_col), column(self.hubs_overnight)]
            for field in self.hubs_extra_fields:
                fields.append(column(field))
            t = table(table_parts[2], *fields)

            for idx, row in context.raw_hubs.iterrows():
                values = [(t.c[self.hubs_geom_col],
                           text(String('').literal_processor(dialect=conn.dialect)(value=str(row.geom)))), (
                          t.c[self.hubs_overnight],
                          text(String('').literal_processor(dialect=conn.dialect)(value=row.overnight)))]
                for field in self.hubs_extra_fields:
                    values.append((t.c[field], text(String('').literal_processor(dialect=conn.dialect)(value=row[field]))))

                stmt = t.update().ordered_values(*values) \
                    .where(t.c[self.hubs_index_col] == idx)
                conn.execute(stmt.compile(compile_kwargs={"literal_binds": True}))

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
