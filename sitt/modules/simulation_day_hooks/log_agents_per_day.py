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
    Handles logging of agent activities per day during the simulation.

    This class implements the SimulationDayHookInterface to manage logging of
    agents' start-of-day activities into a CSV file. It organizes data into
    simulation-specific folders and ensures proper formatting of the output.
    Primarily used to analyze agent behavior on a day-to-day basis within the
    simulation framework.

    :ivar basename: The base name for the folder and files created to store
        simulation logs. Typically contains the simulation route and date
        information.
    :type basename: str | None
    :ivar folder: The name of the folder where simulation logs will be stored.
    :type folder: str | None
    :ivar min_time: The minimum time threshold for logging based on the
        simulation's start date.
    :type min_time: datetime.datetime
    """

    def __init__(self):
        super().__init__()
        self.basename: str | None = None
        self.folder: str | None = None
        self.min_time: dt.datetime = dt.datetime.now()
        self.csv_file = None
        self.csv_writer = None

    def _initialize(self, config: Configuration):
        """
        Initializes the simulation environment based on the provided configuration.

        This method sets up the simulation by defining the minimum time, creating a
        specific folder structure for saving simulation outputs, and preparing a CSV file to
        store initial route data. Additionally, it logs the location where the CSV data will be saved.

        :param config: The Configuration object containing simulation settings including the
            start date and simulation route.
        :type config: Configuration
        :return: None
        """
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
            ['ID', 'Transport Type', 'Day', 'Start Hour', 'Start Minute', 'Hub', 'via', 'To'])

        logger.info(f"Saving CSV data to {csv_filename_routes}")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        """
        Executes the main logic of the function, iterating through a list of agents to record their details
        to a CSV file if the operation is not marked as skippable. Initializes the output folder structure
        if it has not been initialized yet.

        :param config: The configuration object containing necessary simulation settings.
        :type config: Configuration
        :param context: Context object that holds the current state of the simulation environment.
        :type context: Context
        :param agents: A list of agent instances involved in the simulation.
        :type agents: list[Agent]
        :param agents_finished_for_today: A list of agent instances that have completed their tasks for the current day.
        :type agents_finished_for_today: list[Agent]
        :param results: Aggregated results of the simulation up to the current state.
        :type results: SetOfResults
        :param current_day: The current simulation day as an integer.
        :type current_day: int
        :return: A list of agent instances that have finished their tasks for the current day.
        :rtype: list[Agent]
        """
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
                agent.transport_type if agent.transport_type is not None else '',
                current_day,
                hour,
                minute,
                agent.this_hub,
                agent.route_key,
                agent.next_hub,
            ])

        return agents

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context, current_day: int) -> None:
        """
        Closes the CSV file used for simulation data storage to ensure all resources
        are properly released after the simulation has concluded.

        :param results: A set of results generated during the simulation process.
        :param config: The configuration object containing all simulation parameters.
        :param context: The context in which the simulation was executed, providing
            necessary runtime information.
        :param current_day: The current day of the simulation when this method is
            invoked.
        :return: None
        """
        self.csv_file.close()