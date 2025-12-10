# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Persists all agents after each simulation day to PostgresSQL.
"""
import copy
import logging
from urllib import parse

from sqlalchemy import create_engine, Connection, MetaData, Column, Table, String, Float, Boolean, Integer, Sequence, \
    Date, TIMESTAMP, insert, func, ForeignKey, ForeignKeyConstraint, Index, ARRAY, update
from sqlalchemy.dialects.postgresql import JSONB

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults

logger = logging.getLogger()


class PersistAgentsAfterDay(SimulationDayHookInterface):
    """
    Persists all agents after each simulation day to PostgresSQL.
    """

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
        self.current_simulation_id: int = 0

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

    def get_hubs_table(self) -> Table:
        """Get the hubs (vertices) table."""
        schema_key = self.schema + '.hubs'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        id_col = Column('id', String, primary_key=True)
        # geom_col = Column('geom', Geography('POINTZ'))
        # data_col = Column('data', JSONB)
        return Table("hubs", self.metadata_obj, id_col, schema=self.schema)

    def get_edges_table(self) -> Table:
        """Get the edges table."""
        schema_key = self.schema + '.edges'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        id_col = Column('id', String, primary_key=True)
        # geom_col = Column('geom', Geography('LINESTRINGZ'))
        # hub_id_a = Column('hub_id_a', String)
        # hub_id_b = Column('hub_id_b', String)
        # edge_type = Column('type', Enum('road', 'river', 'lake'))
        # data_col = Column('data', JSONB)
        # directions_col = Column('directions', JSONB)
        return Table("edges", self.metadata_obj, id_col, schema=self.schema)

    def get_sim_table(self) -> Table:
        schema_key = self.schema + '.simulation'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        id_col = Column('id', Integer, Sequence('simulation_id_seq', schema=self.schema), primary_key=True)
        route_col = Column('route', String, nullable=False, index=True)
        start_date_col = Column('start_date', Date, nullable=True, index=True)
        start_ts = Column('start_ts', TIMESTAMP, server_default=func.now(), index=True)
        end_ts = Column('end_ts', TIMESTAMP, index=True)

        return Table("simulation", self.metadata_obj, id_col, route_col, start_date_col, start_ts, end_ts, schema=self.schema)

    def get_sim_agent_table(self) -> Table:
        schema_key = self.schema + '.sim_agent'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        simulation_col = Column('simulation_id', Integer, ForeignKey('simulation.id'), nullable=False, primary_key=True)
        uid_col = Column('uid', String, primary_key=True)
        min_dt_col = Column('min_dt', Float)
        max_dt_col = Column('max_dt', Float)
        day = Column('day', Integer, index=True)
        start_hub = Column('start_hub', String, index=True)
        end_hub = Column('end_hub', String, index=True)
        is_finished_col = Column('is_finished', Boolean, index=True)
        is_cancelled_col = Column('is_cancelled', Boolean, index=True)
        additional_data_col = Column('additional_data', JSONB)

        Index('idx_sim_agent_min_max_dt', min_dt_col, max_dt_col)
        return Table("sim_agent", self.metadata_obj, simulation_col, uid_col, min_dt_col, max_dt_col, day, start_hub, end_hub, is_finished_col, is_cancelled_col, additional_data_col, schema=self.schema)

    def get_sim_agent_hub_table(self) -> Table:
        schema_key = self.schema + '.sim_agent_hub'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        simulation_col = Column('simulation_id', Integer, nullable=False, primary_key=True)
        sim_agent_col = Column('agent_id', String, nullable=False, primary_key=True)
        hub_id_col = Column('hub_id', String, primary_key=True)
        oder_col = Column('sorting', Integer, index=True)
        min_dt_col = Column('min_dt', Float)
        max_dt_col = Column('max_dt', Float)
        additional_data_col = Column('additional_data', JSONB)

        Index('idx_sim_agent_hub_min_max_dt', min_dt_col, max_dt_col)
        ForeignKey('sim_agent_hub_sim_agent_id_fkey', ForeignKeyConstraint(columns=('simulation.id', 'sim_agent.uid', 'hubs.id'), refcolumns=(simulation_col, sim_agent_col, hub_id_col)))
        return Table("sim_agent_hub", self.metadata_obj, simulation_col, sim_agent_col, hub_id_col, oder_col, min_dt_col, max_dt_col, additional_data_col, schema=self.schema)

    def get_sim_agent_route_table(self) -> Table:
        schema_key = self.schema + '.sim_agent_route'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        simulation_col = Column('simulation_id', Integer, nullable=False, primary_key=True)
        sim_agent_col = Column('agent_id', String, nullable=False, primary_key=True)
        route_id_col = Column('route_id', String, nullable=False, primary_key=True)
        order_col = Column('sorting', Integer, index=True)
        min_dt_col = Column('min_dt', Float)
        max_dt_col = Column('max_dt', Float)
        leg_times = Column('leg_times', ARRAY(Float))
        additional_data_col = Column('additional_data', JSONB)

        Index('idx_sim_agent_route_min_max_dt', min_dt_col, max_dt_col)
        ForeignKey('sim_agent_route_sim_agent_id_fkey', ForeignKeyConstraint(columns=('simulation.id', 'sim_agent.uid', 'edges.id'), refcolumns=(simulation_col, sim_agent_col, route_id_col)))
        return Table("sim_agent_route", self.metadata_obj, simulation_col, sim_agent_col, route_id_col, order_col, min_dt_col, max_dt_col, leg_times, additional_data_col, schema=self.schema)


    def _initialize(self, config: Configuration):
        # initialize table data
        conn = self.get_connection()

        self.get_hubs_table()
        self.get_edges_table()
        self.sim_table = self.get_sim_table()
        self.agent_table = self.get_sim_agent_table()
        self.agent_hub_table = self.get_sim_agent_hub_table()
        self.agent_route_table = self.get_sim_agent_route_table()

        self.metadata_obj.create_all(conn)

        result = self.conn.execute(insert(self.sim_table).values(route=config.simulation_route, start_date=config.start_date))

        # set simulation id
        self.current_simulation_id = result.inserted_primary_key[0]

        logger.info(f"Created simulation with id {self.current_simulation_id}")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        if self.skip:
            return agents_finished_for_today

        # initialize by creating simulation id
        if self.current_simulation_id == 0:
            self._initialize(config)

        # do not add duplicate agents on the same hub - so we use a set to keep track of signatures of agents
        agents_per_hub_signatures = set()

        count = 0
        for agent in agents_finished_for_today:
            start_hub, end_hub, min_dt, max_dt = agent.get_start_end()

            # create signature of an agent to check for duplicates
            signature = agent.get_start_end()
            if signature in agents_per_hub_signatures:
                continue
            agents_per_hub_signatures.add(signature)

            additional_data = copy.deepcopy(agent.additional_data)

            if agent.is_cancelled and agent.state.last_coordinate_after_stop:
                additional_data['last_coordinate_after_stop'] = agent.state.last_coordinate_after_stop

            # ignore agents that have the same start and end hubs
            if start_hub == end_hub:
                continue

            count += 1

            # create entry in sim_agent table
            self.conn.execute(
                insert(self.agent_table).values(simulation_id=self.current_simulation_id, uid=agent.uid, min_dt=min_dt, max_dt=max_dt, day=current_day, start_hub=start_hub, end_hub=end_hub, is_finished=agent.is_finished, is_cancelled=agent.is_cancelled, additional_data=additional_data))

            for d in agents_finished_for_today[0].iterate_routes():
                if d['type'] == 'edge':
                    additional_data = {}
                    if d['rest']:
                        additional_data['rest'] = d['rest']
                    self.conn.execute(
                        insert(self.agent_route_table).values(simulation_id=self.current_simulation_id, agent_id=agent.uid, route_id=d['uid'], sorting=d['idx'], min_dt=d['legs'][0], max_dt=d['legs'][-1], leg_times=d['legs'], additional_data=additional_data))
                else:
                    additional_data = {}
                    if d['rest']:
                        additional_data['rest'] = d['rest']
                    self.conn.execute(
                        insert(self.agent_hub_table).values(simulation_id=self.current_simulation_id, agent_id=agent.uid, hub_id=d['uid'], sorting=d['idx'], min_dt=d['arrival'], max_dt=d['departure'], additional_data=additional_data))

        self.conn.commit()

        logger.info(f"Persisted {count} agents to the database for day {current_day}.")

        return agents_finished_for_today

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context, current_day: int) -> None:
        # set simulation to finished
        self.conn.execute(update(self.sim_table).values(end_ts=func.now()).where(self.sim_table.c.id == self.current_simulation_id))
        self.conn.commit()

