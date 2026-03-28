# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Persist agents' routes to a GeoPackage file/database. We will save each day separately, so it is easier to
comprehend the data.
"""
import csv
import datetime as dt
import logging
import math
import os
import shutil

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults

logger = logging.getLogger()

class PersistAgentsToCSV(SimulationDayHookInterface):
    def __init__(self, delete_existing_folder: bool = True):
        super().__init__()
        self.delete_existing_folder: bool = delete_existing_folder
        """Delete existing folder before running."""
        self.basename: str | None = None
        self.folder: str | None = None
        self.min_time: dt.datetime = dt.datetime.now()
        self.csv_file = None
        self.csv_writer = None
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
        os.mkdir(self.folder)

        csv_filename_routes = os.path.join(self.folder, f"{self.basename}_finished.csv")
        self.csv_file = open(csv_filename_routes, 'w', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(
            ['ID', 'Last Transport Type', 'Variant Paths', 'Length (hrs)', 'Arrival Day', 'Arrival Hour',
             'Start Hubs', 'Start Times', 'End Hub', 'End Time', 'Overnight Hubs'])

        logger.info(f"Saving CSV data to {csv_filename_routes}")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        if self.skip:
            return agents_finished_for_today

        # initialize output
        if self.folder is None:
            self._initialize(config)

        self._persist_agents(agents_finished_for_today, config, current_day)

        return agents_finished_for_today

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context,
                          current_day: int) -> None:
        self.csv_file.close()

    def _persist_agents(self, agents: list[Agent], config: Configuration, current_day: int):
        # save finished agents to CSV
        for agent in agents:
            # ignore cancelled agents
            if agent.is_cancelled:
                continue

            # define finished status
            is_finished = agent.is_finished
            if not is_finished:
                if config.simulation_ends and agent.this_hub in config.simulation_ends:
                    is_finished = True

            # persist to route graph
            # node = self._save_to_route_graph(agent)
            node = self._save_route_origins(agent)

            # save to CSV, if finished
            if is_finished:
                self._persist_agent(config, agent, node, current_day)

    def _save_route_origins(self, agent: Agent) -> dict:
        start_hub, _, start_delta, end_delta = agent.get_start_end()

        start_time = self.min_time + dt.timedelta(hours=start_delta)
        end_time = self.min_time + dt.timedelta(hours=end_delta)

        start_hubs = set()
        overnight_hubs = set()
        overnight_hubs.add(agent.route[0])
        start_times = set()
        min_time = start_time
        count = 0

        for pid in agent.parents:
            parent = self.route_origins[pid]
            min_time = min(min_time, parent['min_time'])
            start_hubs.update(parent['start_hubs'])
            start_times.update(parent['start_times'])
            overnight_hubs.update(parent['overnight_hubs'])
            count += parent['count']

        # start?
        if len(start_hubs) == 0:
            start_hubs.add(start_hub)
            start_times.add(start_time.strftime('%Y-%m-%d %H:%M'))
        if count == 0:
            count = 1

        node = {
            'start_time': start_time,
            'end_time': end_time,
            'start_hub': start_hub,
            'overnight_hubs': overnight_hubs,
            'min_time': min_time,
            'start_hubs': start_hubs,
            'start_times': start_times,
            'count': count,
        }


        self.route_origins[agent.uid] = node

        return node

    def _persist_agent(self, config: Configuration, agent: Agent, node: dict, current_day: int):
        last_means_of_transport = agent.type_signature if agent.type_signature is not None else ''
        time_taken = int((node['end_time'] - node['min_time']).total_seconds() / 3600)  # convert to hours
        current_hour = math.floor(agent.current_time % 24)

        self.csv_writer.writerow([agent.uid, last_means_of_transport, node['count'], time_taken, current_day,
                                  current_hour, ', '.join(list(node['start_hubs'])),
                                  ', '.join(list(node['start_times'])),
                                  agent.this_hub, node['end_time'].strftime('%Y-%m-%d %H:%M'),
                                  ', '.join(list(node['overnight_hubs']))])
