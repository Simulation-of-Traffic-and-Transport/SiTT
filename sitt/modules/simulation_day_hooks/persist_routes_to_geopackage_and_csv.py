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

import fiona
from shapely import LineString, MultiLineString, force_2d

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults

logger = logging.getLogger()

class PersistRoutesToGeoPackageAndCSV(SimulationDayHookInterface):
    """
    A simulation day hook that persists agent routes to GeoPackage and CSV files.

    This class implements the SimulationDayHookInterface to save agent route data at the end
    of each simulation day. It creates output files in GeoPackage format for spatial data and
    CSV format for tabular data, tracking route information including hubs visited, edges
    traversed, transport types used, and timing information.

    Attributes:
        delete_existing_folder (bool): Flag to delete existing output folder before running.
        basename (str | None): Base name for output files, derived from simulation configuration.
        folder (str | None): Path to the output folder where files are saved.
        file_gpkg (fiona.Collection | None): File handle for the GeoPackage output file.
        file_routes_csv: File handle for the routes CSV output file.
        csv_writer_routes: CSV writer object for writing route data.
        file_transport_types_csv: File handle for the transport types CSV output file.
        csv_writer_transport_types: CSV writer object for writing transport type data.
        min_time (dt.datetime): Minimum time reference point for the simulation.
        route_origins (dict): Dictionary tracking route information for all agents.
        route_ids_per_day (dict[int, set[str]]): Dictionary mapping simulation days to sets
            of route IDs, used for memory management.
    """
    def __init__(self, delete_existing_folder: bool = True):
        """
        Initialize the PersistRoutesToGeoPackageAndCSV hook.

        Sets up the initial state for the hook, including file handles and tracking dictionaries.
        The hook will be ready to create output files and persist route data when the simulation runs.

        Args:
            delete_existing_folder (bool, optional): If True, any existing output folder with the
                same name will be deleted before creating new output files. If False, existing
                folders are preserved, which may cause errors if files already exist. Defaults to True.

        Returns:
            None
        """
        super().__init__()
        self.delete_existing_folder: bool = delete_existing_folder
        """Delete existing folder before running."""
        self.basename: str | None = None
        self.folder: str | None = None
        self.file_gpkg: fiona.Collection | None = None
        self.file_routes_csv = None
        self.csv_writer_routes = None
        self.file_transport_types_csv = None
        self.csv_writer_transport_types = None
        self.min_time: dt.datetime = dt.datetime.now()
        self.route_origins: dict = {}
        """Keep track of routes of agents."""
        self.route_ids_per_day: dict[int, set[str]] = {}
        """Keep track of route ids per day, so we can delete older routes to save memory."""

    def _initialize(self, config: Configuration):
        """
        Initialize output files and directories for persisting route data.

        This method sets up the necessary file structure and opens output files (GeoPackage
        and CSV) for storing agent route information. It creates a timestamped folder based
        on the simulation configuration, optionally removes existing data, and initializes
        file handles with appropriate schemas and headers. The method also sets the minimum
        time reference point for the simulation based on the start date.

        Args:
            config (Configuration): Configuration object containing simulation settings,
                including start_date (used to set min_time and create folder names),
                simulation_route (used in folder naming), and other parameters that define
                the simulation behavior. The start_date must be a valid date object.

        Returns:
            None: This method performs initialization by setting instance variables
                (min_time, basename, folder, file_gpkg, file_routes_csv, csv_writer_routes,
                file_transport_types_csv, csv_writer_transport_types) and creating output
                files, but does not return a value.
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

        filename = os.path.join(self.folder, f"{self.basename}_routes.")
        self.file_gpkg = fiona.open(filename + 'gpkg', 'w', driver='GPKG', crs='EPSG:4326', schema={'geometry': 'MultiLineString',
                                                                                      'properties': {'id': 'str',
                                                                                                     'last_transport_type': 'str',
                                                                                                     'variant_paths': 'str', # because some numbers are higher than C's int length
                                                                                                     'length_hrs': 'int',
                                                                                                     'arrival_day': 'int',
                                                                                                     'arrival_hour': 'int',
                                                                                                     'start_hubs': 'str',
                                                                                                     'start_times': 'str',
                                                                                                     'end_hub': 'str',
                                                                                                     'end_time': 'datetime',
                                                                                                     'overnight_hubs': 'str',
                                                                                                     'count_hubs': 'int',
                                                                                                     'count_edges': 'int',
                                                                                                     'count_foot': 'int',
                                                                                                     'count_donkey': 'int',
                                                                                                     'count_ox': 'int',
                                                                                                     'hubs': 'str',
                                                                                                     'hub_coordinates': 'str',
                                                                                                     'edges': 'str'}})

        self.file_routes_csv = open(filename + 'csv', 'w', newline='')
        self.csv_writer_routes = csv.writer(self.file_routes_csv)
        self.csv_writer_routes.writerow(
            ['ID', 'Last Transport Type', 'Variant Paths', 'Length (hrs)', 'Arrival Day', 'Arrival Hour',
             'Start Hubs', 'Start Times', 'End Hub', 'End Time', 'Overnight Hubs', 'Number of Hubs', 'Number of Edges',
             'Count Foot', 'Count Donkey', 'Count Ox', 'Hubs', 'Edges'])

        filename_transport_types = os.path.join(self.folder, f"{self.basename}_transport_types.csv")
        self.file_transport_types_csv = open(filename_transport_types, 'w', newline='')
        self.csv_writer_transport_types = csv.writer(self.file_transport_types_csv)
        self.csv_writer_transport_types.writerow(['ID', 'Edge', 'Foot', 'Donkey', 'Ox'])

        logger.info(f"Saving route data to {filename}gpkg, {filename}csv, and {filename_transport_types}.")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        """
        Execute the daily hook to persist agent routes to GeoPackage and CSV files.

        This method is called at the end of each simulation day to save route data for agents
        that have finished their journeys for the day. It initializes output files on first run
        and delegates the actual persistence logic to helper methods.

        Args:
            config (Configuration): Configuration object containing simulation settings and parameters,
                used for initialization of output files and determining simulation behavior.
            context (Context): Context object providing access to the route network and other
                simulation state information needed for extracting and persisting route data.
            agents (list[Agent]): List of all active agents in the simulation. This parameter is
                provided by the hook interface but not directly used in this implementation.
            agents_finished_for_today (list[Agent]): List of agents that have completed their
                travel for the current simulation day. These agents' route data will be persisted.
            results (SetOfResults): Container for simulation results and metrics. This parameter
                is provided by the hook interface but not directly used in this implementation.
            current_day (int): The current simulation day number, used for tracking routes per day
                and organizing output data.

        Returns:
            list[Agent]: The same list of agents_finished_for_today that was passed in, allowing
                the simulation to continue processing these agents through other hooks.
        """
        if self.skip:
            return agents_finished_for_today

        # initialize output
        if self.folder is None:
            self._initialize(config)

        self._persist_agents(agents_finished_for_today, config, context, current_day)

        return agents_finished_for_today

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context,
                          current_day: int) -> None:
        """
        Clean up and close all open file handles at the end of the simulation.

        This method is called when the simulation completes to ensure all output files
        (GeoPackage and CSV files) are properly closed and their data is flushed to disk.
        It should be called as part of the simulation teardown process to prevent data
        loss and resource leaks.

        Args:
            results (SetOfResults): Container for simulation results and metrics. This
                parameter is provided by the hook interface but not used in this implementation.
            config (Configuration): Configuration object containing simulation settings and
                parameters. This parameter is provided by the hook interface but not used
                in this implementation.
            context (Context): Context object providing access to the route network and other
                simulation state information. This parameter is provided by the hook interface
                but not used in this implementation.
            current_day (int): The final simulation day number when the simulation ended.
                This parameter is provided by the hook interface but not used in this implementation.

        Returns:
            None: This method performs cleanup operations and does not return a value.
        """
        self.file_gpkg.close()
        self.file_routes_csv.close()
        self.file_transport_types_csv.close()

    def _persist_agents(self, agents: list[Agent], config: Configuration, context: Context, current_day: int):
        """
        Persist agent route data to GeoPackage and CSV files for the current simulation day.

        This method processes a list of agents, aggregates their route information, and saves
        completed routes to both GeoPackage and CSV output files. It tracks route IDs per day,
        filters out cancelled agents, determines agent completion status, and writes route data
        for finished agents. After processing, it cleans up old route data to manage memory usage.

        Args:
            agents (list[Agent]): List of agents to process and persist. Each agent should have
                attributes including is_cancelled, is_finished, this_hub, uid, and route information.
            config (Configuration): Configuration object containing simulation settings, including
                simulation_ends which defines terminal hubs where agents are considered finished.
            context (Context): Context object providing access to the route network and other
                simulation state information needed for data extraction.
            current_day (int): The current simulation day number, used for tracking route IDs
                per day and determining which old data to clean up.

        Returns:
            None: This method performs side effects by writing to files and updating internal
                state (route_origins, route_ids_per_day) but does not return a value.
        """
        # aggregate the agents and save their data into a GeoPackage file
        agent_data = []

        # init set for the day
        self.route_ids_per_day[current_day] = set()

        for agent in agents:
            # ignore cancelled agents
            if agent.is_cancelled:
                continue

            # define finished status
            is_finished = agent.is_finished
            if not is_finished:
                if config.simulation_ends and agent.this_hub in config.simulation_ends:
                    is_finished = True

            node = self._save_route_origins(agent)
            self.route_ids_per_day[current_day].add(agent.uid)

            # add to data, if finished
            if is_finished:
                data = self._get_agent_data(context, agent, node, current_day)
                agent_data.append(data)
                self.csv_writer_routes.writerow(data['properties'].values())

        # delete old routes to free RAM
        self._delete_old_routes(current_day)

        self.file_gpkg.writerecords(agent_data)

    def _get_agent_data(self, context: Context, agent: Agent, node: dict, current_day: int):
        """
        Save and aggregate route origin information for an agent, including data from parent agents.

        This method collects comprehensive route information for an agent by combining its current
        route data with historical data from its parent agents. It tracks all hubs visited, edges
        traversed, transport types used, and timing information across the agent's lineage. The
        aggregated data is stored in the route_origins dictionary for future reference and returned
        for immediate use.

        Args:
            agent (Agent): The agent whose route origin information should be saved. The agent
                must have route, transport_types, parents, and uid attributes. The method uses
                agent.get_start_end() to retrieve start/end hub and time delta information.

        Returns:
            dict: A dictionary containing aggregated route information with the following keys:
                - start_delta (float): The earliest start time delta in hours from min_time
                - end_delta (float): The end time delta in hours from min_time
                - end_time (datetime): The actual end datetime of the route
                - overnight_hubs (set): Set of all hubs where overnight stays occurred
                - min_time (datetime): The earliest start time across all parent routes
                - start_hubs (set): Set of all starting hub names in the agent's lineage
                - start_times (set): Set of formatted start times ('%Y-%m-%d %H:%M')
                - count (int): Total count of variant paths (sum of parent counts plus 1)
                - hubs (set): Set of all hub names visited along the route
                - edges (dict): Dictionary mapping edge names to sets of transport types used
        """
        start_hub, end_hub, start_delta, end_delta = agent.get_start_end()

        edges = node['edges'].keys()

        lines: list[MultiLineString] = []
        count_foot = 0
        count_donkey = 0
        count_ox = 0
        for edge_name in edges:
            foot_flag = 0
            donkey_flag = 0
            ox_flag = 0

            lines.append(context.routes.es.find(name=edge_name)['geom'])
            if 'foot' in node['edges'][edge_name]:
                count_foot += 1
                foot_flag = 1
            if 'cart_donkey' in node['edges'][edge_name]:
                count_donkey += 1
                donkey_flag = 1
            if 'cart_oxen' in node['edges'][edge_name]:
                count_ox += 1
                ox_flag = 1

            # also write data to transport types csv
            self.csv_writer_transport_types.writerow([agent.uid, edge_name, foot_flag, donkey_flag, ox_flag])

        hub_coordinates = []
        for hub in context.routes.vs.select(name_in=node['hubs']):
            hub_coordinates.append(f"{hub['geom'].x},{hub['geom'].y}")

        return {'geometry': MultiLineString(lines), 'properties': {
            'id': agent.uid,
            'last_transport_type': agent.transport_type,
            'variant_paths': str(node['count']),
            'length_hrs': int(node['end_delta'] - node['start_delta']),
            'arrival_day': current_day,
            'arrival_hour': math.floor(agent.current_time % 24),
            'start_hubs': ', '.join(node['start_hubs']),
            'start_times': ', '.join(node['start_times']),
            'end_hub': end_hub,
            'end_time': str(node['end_time'].strftime('%Y-%m-%d %H:%M')),
            'overnight_hubs': ','.join(node['overnight_hubs']),
            'count_hubs': len(node['hubs']),
            'count_edges': len(edges),
            'count_foot': count_foot,
            'count_donkey': count_donkey,
            'count_ox': count_ox,
            'hubs': ','.join(list(node['hubs'])),
            'hub_coordinates': ' '.join(hub_coordinates),
            'edges': ','.join(edges),
        }}

    def _save_route_origins(self, agent: Agent) -> dict:
        """
        Save and aggregate route origin information for an agent, including data from parent agents.

        This method collects comprehensive route information for an agent by combining its current
        route data with historical data from its parent agents. It tracks all hubs visited, edges
        traversed, transport types used, and timing information across the agent's lineage. The
        aggregated data is stored in the route_origins dictionary for future reference and returned
        for immediate use.

        Args:
            agent (Agent): The agent whose route origin information should be saved. The agent
                must have route, transport_types, parents, and uid attributes. The method uses
                agent.get_start_end() to retrieve start/end hub and time delta information.

        Returns:
            dict: A dictionary containing aggregated route information with the following keys:
                - start_delta (float): The earliest start time delta in hours from min_time
                - end_delta (float): The end time delta in hours from min_time
                - end_time (datetime): The actual end datetime of the route
                - overnight_hubs (set): Set of all hubs where overnight stays occurred
                - min_time (datetime): The earliest start time across all parent routes
                - start_hubs (set): Set of all starting hub names in the agent's lineage
                - start_times (set): Set of formatted start times ('%Y-%m-%d %H:%M')
                - count (int): Total count of variant paths (sum of parent counts plus 1)
                - hubs (set): Set of all hub names visited along the route
                - edges (dict): Dictionary mapping edge names to sets of transport types used
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
        hubs = set()
        hubs.update(agent.route[::2])
        edges = {}
        for i, route in enumerate(agent.route[1::2]):
            edges[route] = {agent.transport_types[i]}

        for pid in agent.parents:
            parent = self.route_origins[pid]
            min_time = min(min_time, parent['min_time'])
            start_delta = min(start_delta, parent['start_delta'])
            start_hubs.update(parent['start_hubs'])
            start_times.update(parent['start_times'])
            overnight_hubs.update(parent['overnight_hubs'])
            count += parent['count']
            hubs.update(parent['hubs'])
            for route, transport_types in parent['edges'].items():
                if route in edges:
                    edges[route].update(transport_types)
                else:
                    edges[route] = transport_types.copy()

        # start?
        if len(start_hubs) == 0:
            start_hubs.add(start_hub)
            start_times.add(start_time.strftime('%Y-%m-%d %H:%M'))
        if count == 0:
            count = 1

        node = {
            'start_delta': start_delta,
            'end_delta': end_delta,
            'end_time': end_time,
            'overnight_hubs': overnight_hubs,
            'min_time': min_time,
            'start_hubs': start_hubs,
            'start_times': start_times,
            'count': count,
            'hubs': hubs,
            'edges': edges,
        }


        self.route_origins[agent.uid] = node

        return node

    def _delete_old_routes(self, current_day: int):
        """
        Delete route data from two days prior to free up memory.

        This method removes route origin information and route ID tracking for routes
        that are at least two days old. This helps manage memory usage during long
        simulations by removing data that is no longer needed for tracking agent lineage.

        Args:
            current_day (int): The current simulation day number. Routes from day
                (current_day - 2) will be deleted if current_day > 2.

        Returns:
            None
        """
        if current_day > 2:
            for pid in self.route_ids_per_day[current_day - 2]:
                del self.route_origins[pid]
            del self.route_ids_per_day[current_day - 2]