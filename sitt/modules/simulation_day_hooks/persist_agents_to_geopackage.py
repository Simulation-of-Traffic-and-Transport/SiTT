# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Persist agents' routes to a GeoPackage file/database. We will save each day separately, so it is easier to
comprehend the data.
"""
import datetime as dt
import logging
import os
import shutil

import fiona
from shapely import LineString, force_2d

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults

logger = logging.getLogger()

class PersistAgentsToGeoPackage(SimulationDayHookInterface):
    def __init__(self, delete_existing_folder: bool = True):
        super().__init__()
        self.delete_existing_folder: bool = delete_existing_folder
        """Delete existing folder before running."""
        self.basename: str | None = None
        self.folder: str | None = None
        self.file: fiona.Collection | None = None
        self.min_time: dt.datetime = dt.datetime.now()
        self.route_origins: dict = {}
        """Keep track of routes of agents."""

    def _initialize(self, config: Configuration):
        # set min time
        self.min_time = dt.datetime.combine(config.start_date, dt.datetime.min.time())

        # create folder name
        start_date = config.start_date.strftime('%Y-%m-%d')
        self.basename = f"{config.simulation_route}_{start_date}"
        self.folder = f"simulation_{self.basename}"

        # remove old data if it exists
        if self.delete_existing_folder and os.path.exists(self.folder):
            shutil.rmtree(self.folder)

        # create folder
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)

        filename = os.path.join(self.folder, f"{self.basename}_agents.gpkg")
        self.file = fiona.open(filename, 'w', driver='GPKG', layer='agents', crs='EPSG:4326',
                                schema={'geometry': 'LineString',
                                        'properties': {'id': 'str', 'type': 'str', 'start_hub': 'str', 'end_hub': 'str',
                                                       'day': 'int', 'start_time': 'datetime', 'end_time': 'datetime',
                                                       'is_finished': 'bool', 'stops': 'str', 'hubs': 'str',
                                                       'edges': 'str'}})

        logger.info(f"Saving agent data to {filename}.")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        if self.skip:
            return agents_finished_for_today

        # initialize output
        if self.folder is None:
            self._initialize(config)

        self._persist_agents(agents_finished_for_today, config, context, current_day)

        return agents_finished_for_today

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context,
                          current_day: int) -> None:
        self.file.close()

    def _persist_agents(self, agents: list[Agent], config: Configuration, context: Context, current_day: int):
        # aggregate the agents and save their data into a GeoPackage file
        agent_data = []

        for agent in agents:
            # ignore cancelled agents
            if agent.is_cancelled:
                continue

            # define finished status
            is_finished = agent.is_finished
            if not is_finished:
                if config.simulation_ends and agent.this_hub in config.simulation_ends:
                    is_finished = True

            # add to data
            agent_data.append(self._get_agent_data(context, agent, current_day, is_finished))

        self.file.writerecords(agent_data)

    def _get_agent_data(self, context: Context, agent: Agent, current_day: int, is_finished: bool):
        coordinates = []
        for i, route_key in enumerate(agent.route[1::2]):
            coords = list(force_2d(context.routes.es.find(name=route_key)['geom']).coords)
            if agent.route_reversed[i]:
                coords = list(reversed(coords))
            if len(coordinates) > 0 and coordinates[-1] == coords[0]:
                # the last coordinate is equal to the first coordinate, remove it
                coordinates.pop()
            coordinates.extend(coords)

        start_hub, end_hub, start_delta, end_delta = agent.get_start_end()
        start_time = self.min_time + dt.timedelta(hours=start_delta)
        end_time = self.min_time + dt.timedelta(hours=end_delta)

        return {'geometry': LineString(coordinates), 'properties': {
            'id': agent.uid,
            'type': agent.type_signature,
            'start_hub': start_hub,
            'end_hub': end_hub,
            'day': current_day,
            'start_time': start_time,
            'end_time': end_time,
            'is_finished': is_finished,
            'stops': str(agent.rest_history),
            'hubs': ','.join(agent.route[::2]),
            'edges': ','.join(agent.route[1::2]),
        }}
