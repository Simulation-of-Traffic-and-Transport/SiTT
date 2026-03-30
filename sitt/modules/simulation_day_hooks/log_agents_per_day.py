# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This preparation will add a certain padding to the agent's start and stop time.
"""
import csv
import datetime as dt
import logging
import math
import os
import shutil

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults

logger = logging.getLogger()

class LogAgentsPerDay(SimulationDayHookInterface):
    """
    This preparation will add a certain padding to the agent's start and stop time.
    It uses the timezonefinder library to get the timezone of the agent's hub.
    """

    def __init__(self):
        super().__init__()
        self.basename: str | None = None
        self.folder: str | None = None
        self.min_time: dt.datetime = dt.datetime.now()
        self.csv_file = None
        self.csv_writer = None

    def _initialize(self, config: Configuration):
        # set min time
        self.min_time = dt.datetime.combine(config.start_date, dt.datetime.min.time())

        # create folder name
        start_date = config.start_date.strftime('%Y-%m-%d')
        self.basename = f"{config.simulation_route}_{start_date}"
        self.folder = f"simulation_{self.basename}"

        # create folder
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)

        csv_filename_routes = os.path.join(self.folder, f"{self.basename}_start_of_day.csv")
        self.csv_file = open(csv_filename_routes, 'w', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(
            ['ID', 'Transport Type', 'Day', 'Start Hour', 'Start Minute', 'Hub', 'via', 'To', 'Forced Route'])

        logger.info(f"Saving CSV data to {csv_filename_routes}")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        if self.skip:
            return agents_finished_for_today

        # initialize output
        if self.folder is None:
            self._initialize(config)

        for agent in agents:
            hour = math.floor(agent.current_time)
            minute = math.floor((agent.current_time - hour) * 60)

            self.csv_writer.writerow([
                agent.uid,
                agent.type_signature if agent.type_signature is not None else '',
                current_day,
                hour,
                minute,
                agent.this_hub,
                agent.route_key,
                agent.next_hub,
                ", ".join(agent.forced_route),
            ])

        return agents

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context, current_day: int) -> None:
        self.csv_file.close()