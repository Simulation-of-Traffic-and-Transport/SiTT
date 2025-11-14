# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Prune the agent list to reduce the number of agents to only unique ones, merging route data for duplicates.
"""
import copy
from urllib import parse

from geoalchemy2 import Geography
from sqlalchemy import create_engine, Connection, MetaData, Column, Table, String, Float, Boolean, Integer, Sequence, \
    Date, TIMESTAMP, insert, func, ForeignKey, ForeignKeyConstraint, Index, ARRAY, update
from sqlalchemy.dialects.postgresql import JSONB

from sitt import SimulationDayHookInterface, Configuration, Context, Agent


class PersistAgentsAfterDay(SimulationDayHookInterface):
    """
    Prune the agent list to reduce the number of agents to only unique ones, merging route data for duplicates.
    """

    def __init__(self, server: str = 'localhost',
                 port: int = 5432, db: str = 'sitt', user: str = 'postgres', password: str = 'postgres',
                 schema: str = 'sitt', connection: str | None = None):
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

        id_col = Column('id', Integer, Sequence('simulation_id_seq'), primary_key=True)
        ts_route_col = Column('route', String, nullable=False, index=True)
        ts_start_date_col = Column('start_date', Date, nullable=True, index=True)
        ts_col = Column('ts', TIMESTAMP, server_default=func.now(), index=True)
        is_finshed_col = Column('is_finished', Boolean, default=False, nullable=False, index=True)

        return Table("simulation", self.metadata_obj, id_col, ts_route_col, ts_start_date_col, ts_col, is_finshed_col, schema=self.schema)

    def get_sim_agent_table(self) -> Table:
        schema_key = self.schema + '.sim_agent'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        simulation_col = Column('simulation_id', Integer, ForeignKey('simulation.id'), nullable=False, primary_key=True)
        uid_col = Column('uid', String, primary_key=True)
        min_dt_col = Column('min_dt', Float)
        max_dt_col = Column('max_dt', Float)
        is_finshed_col = Column('is_finished', Boolean, index=True)
        is_cancelled_col = Column('is_cancelled', Boolean, index=True)
        additional_data_col = Column('additional_data', JSONB)

        Index('idx_sim_agent_min_max_dt', min_dt_col, max_dt_col)
        return Table("sim_agent", self.metadata_obj, simulation_col, uid_col, min_dt_col, max_dt_col, is_finshed_col, is_cancelled_col, additional_data_col, schema=self.schema)

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
        leg_times = Column('leg_times', ARRAY(Integer))
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

    def run(self, config: Configuration, context: Context, agents: list[Agent],
                            agents_finished_for_today: list[Agent], current_day: int) -> list[Agent]:
        # initialize by creating simulation id
        if self.current_simulation_id == 0:
            self._initialize(config)

        for agent in agents_finished_for_today:
            min_dt = None
            max_dt = None

            additional_data = copy.deepcopy(agent.additional_data)

            # only if we have route times
            if len(agent.route_times):
                # get route times for first and last routes
                min_dt = agent.route_times[agent.route[1]][0]
                max_dt = agent.route_times[agent.route[-2]][-1]

            if agent.is_cancelled and agent.state.last_coordinate_after_stop:
                print("TODO: agent.state.last_coordinate_after_stop")
                exit(0)

            # create entry in sim_agent table
            self.conn.execute(
                insert(self.agent_table).values(simulation_id=self.current_simulation_id, uid=agent.uid, min_dt=min_dt, max_dt=max_dt, is_finished=agent.is_finished, is_cancelled=agent.is_cancelled, additional_data=additional_data))

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

        return agents_finished_for_today

    def finish_simulation(self, config: Configuration, context: Context, current_day: int) -> None:
        # set simulation to finished
        self.conn.execute(update(self.sim_table).values(is_finished=True).where(self.sim_table.c.id == self.current_simulation_id))
        self.conn.commit()

