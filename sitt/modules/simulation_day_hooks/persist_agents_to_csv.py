# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Persist agents' routes to a CSV file.
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
    """
    Persist agents' routes to a CSV file.
    """
    def __init__(self, delete_existing_folder: bool = True):
        """
        Initialize the PersistAgentsToCSV hook with configuration options.

        This constructor sets up the initial state for the CSV persistence system,
        including whether to delete existing output folders and initializing internal
        data structures for tracking agent routes and CSV file handles.

        Args:
            delete_existing_folder (bool, optional): If True, any existing output folder
                with the same name will be deleted before creating a new one. If False,
                existing folders are preserved. Defaults to True.

        Returns:
            None
        """
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
        """
        Initialize the CSV persistence system by setting up the output folder and CSV file.

        This method creates the necessary directory structure for storing simulation results,
        optionally removes existing data, and initializes the CSV file with appropriate headers
        for tracking finished agents.

        Args:
            config (Configuration): The simulation configuration object containing start_date,
                simulation_route, and other settings needed to set up the output structure.

        Returns:
            None
        """
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

        csv_filename_routes = os.path.join(self.folder, f"{self.basename}_finished.csv")
        self.csv_file = open(csv_filename_routes, 'w', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(
            ['ID', 'Last Transport Type', 'Variant Paths', 'Length (hrs)', 'Arrival Day', 'Arrival Hour',
             'Start Hubs', 'Start Times', 'End Hub', 'End Time', 'Overnight Hubs'])

        logger.info(f"Saving CSV data to {csv_filename_routes}")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        """
        Execute the daily hook to persist finished agents to CSV.

        This method is called at the end of each simulation day to process and save
        information about agents that have completed their routes. It initializes the
        output system on first run and persists agent data to the CSV file.

        Args:
            config (Configuration): The simulation configuration object containing settings
                and parameters for the current simulation run.
            context (Context): The simulation context providing access to shared state and
                resources during the simulation.
            agents (list[Agent]): The complete list of all agents currently active in the
                simulation.
            agents_finished_for_today (list[Agent]): The list of agents that have finished
                their routes or reached their destination on the current simulation day.
            results (SetOfResults): The collection of simulation results accumulated so far.
            current_day (int): The current day number in the simulation (0-indexed).

        Returns:
            list[Agent]: The same list of agents_finished_for_today that was passed in,
                allowing for potential chaining with other hooks.
        """
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
        """
        Process and persist finished agents to CSV file.

        This method iterates through a list of agents, determines their completion status,
        saves their route origin information, and persists completed agents to the CSV file.
        Cancelled agents are skipped during processing.

        Args:
            agents (list[Agent]): The list of agents to process and potentially persist.
                Each agent contains route information, completion status, and other
                simulation data.
            config (Configuration): The simulation configuration object containing settings
                such as simulation_ends which defines valid endpoint hubs for determining
                if an agent has finished.
            current_day (int): The current day number in the simulation (0-indexed), used
                for recording when agents complete their routes.

        Returns:
            None
        """
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
        """
        Track and aggregate route origin information for an agent and its parent agents.

        This method collects and consolidates route information from an agent and all its
        parent agents (if any) to build a comprehensive view of the agent's journey origins.
        It tracks start hubs, start times, overnight stops, and timing deltas, aggregating
        data from the entire lineage of parent agents. The resulting information is stored
        in the route_origins dictionary for later retrieval.

        Args:
            agent (Agent): The agent whose route origin information should be tracked.
                The agent must have route information including start/end hubs, timing
                deltas, and may have parent agent references that need to be aggregated.

        Returns:
            dict: A dictionary containing aggregated route origin information with the
                following keys:
                - 'start_delta' (float): The earliest start time delta in hours from min_time
                - 'end_delta' (float): The end time delta in hours from min_time
                - 'start_time' (datetime): The calculated start datetime
                - 'end_time' (datetime): The calculated end datetime
                - 'start_hub' (str): The starting hub for this agent
                - 'overnight_hubs' (set): All hubs where overnight stops occurred
                - 'min_time' (datetime): The earliest start time across all parent agents
                - 'start_hubs' (set): All starting hubs from this agent and its parents
                - 'start_times' (set): All start times formatted as 'YYYY-MM-DD HH:MM'
                - 'count' (int): The total count of route variants from parent agents
        """
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
            start_delta = min(start_delta, parent['start_delta'])
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
            'start_delta': start_delta,
            'end_delta': end_delta,
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
        """
        Write a single finished agent's route information to the CSV file.

        This method extracts relevant information from the agent and its aggregated route
        node data, formats it appropriately, and writes a row to the CSV file containing
        the agent's complete journey details including timing, hubs visited, and route
        variant information.

        Args:
            config (Configuration): The simulation configuration object containing settings
                and parameters for the current simulation run.
            agent (Agent): The agent whose information should be persisted. Must be a
                finished agent with complete route information including uid, type_signature,
                current_time, and this_hub attributes.
            node (dict): A dictionary containing aggregated route origin information for
                the agent, including 'start_delta', 'end_delta', 'end_time', 'count',
                'start_hubs', 'start_times', and 'overnight_hubs' keys.
            current_day (int): The current day number in the simulation (0-indexed) when
                the agent finished its route.

        Returns:
            None
        """
        last_means_of_transport = agent.type_signature if agent.type_signature is not None else ''
        time_taken = int(node['end_delta'] - node['start_delta'])  # convert to hours
        current_hour = math.floor(agent.current_time % 24)

        self.csv_writer.writerow([agent.uid, last_means_of_transport, node['count'], time_taken, current_day,
                                  current_hour, ', '.join(list(node['start_hubs'])),
                                  ', '.join(list(node['start_times'])),
                                  agent.this_hub, node['end_time'].strftime('%Y-%m-%d %H:%M'),
                                  ', '.join(list(node['overnight_hubs']))])
