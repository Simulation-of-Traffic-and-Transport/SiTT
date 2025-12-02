# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This preparation will add a certain padding to the agent's start and stop time.
"""
import logging
import datetime as dt
import math
import os
import sqlite3

from shapely import LineString, force_2d
from dateutil import tz
from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults
from timezonefinder import TimezoneFinder
from suntime import Sun

logger = logging.getLogger()

class PersistAgentsToSpatialite(SimulationDayHookInterface):
    def __init__(self):
        super().__init__()
        self.filename = None
        self.con = None
        self.min_time = dt.datetime.now()

    def _initialize(self, config: Configuration):
        # set min time
        self.min_time = dt.datetime.combine(config.start_date, dt.datetime.min.time())

        # create filename
        self.filename = f"simulation_{config.simulation_route}_{config.start_date}.sqlite"

        # remove old database if it exists
        if os.path.exists(self.filename):
            os.remove(self.filename)

        db = sqlite3.connect(self.filename)
        db.enable_load_extension(True)
        db.execute("SELECT load_extension('mod_spatialite')")
        db.execute("SELECT InitSpatialMetadata(1)")
        # create agent table
        db.execute("CREATE TABLE agent (id TEXT PRIMARY KEY, start_hub TEXT, end_hub TEXT, day TIMESTAMP, start_time TIMESTAMP, end_time TIMESTAMP, is_finished BOOL DEFAULT 0, is_cancelled BOOL DEFAULT 0, cancel_reason TEXT DEFAULT NULL, stops TEXT DEFAULT NULL, hubs TEXT, edges TEXT, last_coordinate TEXT DEFAULT NULL, end_coordinate TEXT, complete_route TEXT)")
        db.execute(
            "SELECT AddGeometryColumn('agent', 'geom', 4326, 'LINESTRING', 'XY')"
        )
        db.execute(
            "SELECT CreateSpatialIndex('agent', 'geom');"
        )
        # create route table
        db.execute("CREATE TABLE route (id TEXT PRIMARY KEY, start_hub TEXT, end_hub TEXT, type TEXT, attempted INTEGER DEFAULT 0, succeeded INTEGER DEFAULT 0)")
        db.execute(
            "SELECT AddGeometryColumn('route', 'geom', 4326, 'LINESTRING', 'XY')"
        )
        db.execute(
            "SELECT CreateSpatialIndex('route', 'geom');"
        )
        self.con = db

        logger.info(f"Saving agents to Spatialite database named {self.filename}")

    def _initialize_routes(self, context: Context):
        # create route entries for each route
        for e in context.routes.es:
            self.con.execute("INSERT INTO route (id, start_hub, end_hub, type, geom) VALUES (?,?,?,?,GeomFromText(?,4326))", (e['name'], e.source_vertex['name'], e.target_vertex['name'], e['type'], force_2d(e['geom']).wkt))

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        if self.skip:
            return agents_finished_for_today

        # initialize by creating spatialite connection
        if self.con is None:
            self._initialize(config)
            self._initialize_routes(context)

        for agent in agents_finished_for_today:
            self._persist_agent(agent, context)

        return agents_finished_for_today

    def finish_simulation(self, config: Configuration, context: Context, current_day: int) -> None:
        # create indexes
        self.con.execute("CREATE INDEX idx_agent_start_hub ON agent (start_hub);")
        self.con.execute("CREATE INDEX idx_agent_end_hub ON agent (end_hub);")
        self.con.execute("CREATE INDEX idx_agent_day ON agent (day);")
        self.con.execute("CREATE INDEX idx_agent_start_time ON agent (start_time);")
        self.con.execute("CREATE INDEX idx_agent_end_time ON agent (end_time);")
        self.con.execute("CREATE INDEX idx_agent_is_finished ON agent (is_finished);")
        self.con.execute("CREATE INDEX idx_agent_is_cancelled ON agent (is_cancelled);")

        self.con.execute("CREATE INDEX idx_route_attempted ON route (attempted);")
        self.con.execute("CREATE INDEX idx_route_succeeded ON route (succeeded);")

        self.con.commit()
        self.con.close()

        logger.info(f"Saved agents to Spatialite database named {self.filename}")

    def _persist_agent(self, agent: Agent, context: Context):
        # get route/geometry
        route = self._merge_route(agent.route, agent.route_reversed, context)
        # get whole route
        route_before_stop = self._merge_route(agent.route_before_traceback, agent.route_reversed_before_traceback, context, is_attempt=True)

        # not traced back...
        if route_before_stop.is_empty:
            route_before_stop = route
            #route = LineString() # empty shape

        # get start/end time
        start_hub, end_hub, start_delta, end_delta = agent.get_start_end()

        start_time = self.min_time + dt.timedelta(hours=start_delta)
        end_time = self.min_time + dt.timedelta(hours=end_delta)
        day = start_time.replace(hour=12, minute=0, second=0)

        last_coordinate = 'NULL'
        if agent.state.last_coordinate_after_stop:
            last_coordinate = f"'POINT({agent.state.last_coordinate_after_stop[0]} {agent.state.last_coordinate_after_stop[1]})'"

        if len(route.coords) > 0:
            p = route.coords[-1]
            end_coordinate = f"'POINT({p[0]} {p[1]})'"
        else:
            p = context.routes.vs.find(name=agent.route[0])['geom']
            end_coordinate = f"'POINT({p.x} {p.y})'"

        if end_coordinate == last_coordinate:
            last_coordinate = 'NULL'

        hubs = ','.join(agent.route[::2])
        edges = ','.join(agent.route[1::2])

        self.con.execute(f"INSERT INTO agent (id, start_hub, end_hub, day, start_time, end_time, is_finished, is_cancelled, cancel_reason, last_coordinate, end_coordinate, stops, hubs, edges, complete_route, geom) VALUES (?,?,?,?,?,?,?,?,?,{last_coordinate},{end_coordinate},?,?,?,?,GeomFromText(?,4326))", (agent.uid, start_hub, end_hub, day, start_time, end_time, agent.is_finished, agent.is_cancelled, agent.cancel_reason, str(agent.rest_history), hubs, edges, route.wkt, route_before_stop.wkt))

    def _merge_route(self, route: list[str], route_reversed: list[bool], context: Context, is_attempt = False) -> LineString | None:
        coordinates = []

        for idx, route_id in enumerate(route[1::2]):
            # get route
            route = context.routes.es.find(name=route_id)
            # get coordinates
            coords = force_2d(route['geom']).coords
            if route_reversed[idx]:
                coords = reversed(coords)
            # join coordinates
            coords = list(coords)
            if len(coordinates) > 0 and coordinates[-1] == coords[0]:
                # last coordinate is equal to first coordinate, remove it
                coordinates.pop()
            coordinates.extend(coords)
            # increment route counter
            self._increment_route_counter(route_id, is_attempt)

        return LineString(coordinates)

    def _increment_route_counter(self, route_id: str, is_attempt = False):
        # update counter(s)
        self.con.execute(f"UPDATE route SET attempted = attempted + 1 WHERE id = ?", (route_id,))
        if not is_attempt:
            self.con.execute(f"UPDATE route SET succeeded = succeeded + 1 WHERE id = ?", (route_id,))
