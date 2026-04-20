# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Persist agents' routes to a GeoPackage file/database. We will save each day separately, so it is easier to
comprehend the data.
"""
import copy
import csv
import datetime as dt
import logging
import math
import os
import shutil

import fiona
from shapely import MultiLineString, force_2d

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults

logger = logging.getLogger()

class PersistRoutesToGeoPackageAndCSV(SimulationDayHookInterface):
    def __init__(self, delete_existing_folder: bool = True, export_all_routes_gpkg: bool = True,
                 export_daily_routes_gpkg: bool = True, export_routes_csv: bool = True,
                 export_transport_types_csv: bool = True, export_overnight_hubs_csv: bool = True):
        """
        Initialize the PersistRoutesToGeoPackageAndCSV hook with export configuration options.

        This constructor sets up the hook for persisting agent route data to various output formats
        including GeoPackage files and CSV files. It configures which types of data should be exported
        and whether existing output folders should be deleted before running. All file handles and
        writers are initialized to None and will be created during the first run of the hook.

        Args:
            delete_existing_folder (bool, optional): If True, removes any existing output folder
                with the same name before creating a new one. This prevents data from previous
                simulation runs from mixing with current results. Defaults to True.
            export_all_routes_gpkg (bool, optional): If True, exports complete route data for all
                finished agents to a GeoPackage file containing geometry and comprehensive route
                properties. Defaults to True.
            export_daily_routes_gpkg (bool, optional): If True, exports daily aggregated route data
                to a separate GeoPackage file, showing routes grouped by destination hub for each
                simulation day. Defaults to True.
            export_routes_csv (bool, optional): If True, exports complete route data for all finished
                agents to a CSV file with the same information as the all_routes GeoPackage but in
                tabular format. Defaults to True.
            export_transport_types_csv (bool, optional): If True, exports detailed transport type
                usage statistics per edge to a CSV file, showing how many times each transport mode
                was used on each edge. Defaults to True.
            export_overnight_hubs_csv (bool, optional): If True, exports information about hubs where
                agents stayed overnight to a CSV file, including the number of agents, incoming routes,
                and origin information. Defaults to True.

        Returns:
            None: This is a constructor method that initializes instance variables but does not
                return a value.
        """
        super().__init__()
        self.delete_existing_folder: bool = delete_existing_folder
        """Delete existing folder before running."""
        self.basename: str | None = None
        self.folder: str | None = None
        self.export_all_routes_gpkg: bool = export_all_routes_gpkg
        self.file_all_routes_gpkg: fiona.Collection | None = None
        self.export_daily_routes_gpkg: bool = export_daily_routes_gpkg
        self.file_daily_routes_gpkg: fiona.Collection | None = None
        self.export_routes_csv: bool = export_routes_csv
        self.file_routes_csv = None
        self.csv_writer_routes = None
        self.export_transport_types_csv: bool = export_transport_types_csv
        self.file_transport_types_csv = None
        self.csv_writer_transport_types = None
        self.export_overnight_hubs_csv: bool = export_overnight_hubs_csv
        self.file_overnight_hubs_csv = None
        self.csv_writer_overnight_hubs = None
        self.min_time: dt.datetime = dt.datetime.now()
        self.hubs: dict = {}
        """Keep track of hubs per day."""
        self.hubs_yesterday: dict = {}
        """Keep track of hubs of yesterday."""

    def _initialize(self, config: Configuration):
        """
        Initialize output files and directories for persisting route data.
    
        This method sets up the necessary file structure and opens output files (GeoPackage
        and CSV) for storing agent route information. It creates a timestamped folder based
        on the simulation configuration, optionally removes existing data, and initializes
        file handles with appropriate schemas and headers. The method also sets the minimum
        time reference point for the simulation based on the start date.
    
        The method creates up to five different output files based on the export configuration:
        1. All routes GeoPackage: Complete route geometries and properties for finished agents
        2. Daily routes GeoPackage: Aggregated daily route data grouped by destination hub
        3. Routes CSV: Tabular version of all routes data
        4. Transport types CSV: Edge-level transport type usage statistics
        5. Overnight hubs CSV: Information about hubs where agents stayed overnight
    
        Args:
            config (Configuration): Configuration object containing simulation settings,
                including start_date (used to set min_time and create folder names),
                simulation_route (used in folder naming), and means_of_transport (list of
                transport types to track). The start_date must be a valid date object.
    
        Returns:
            None: This method performs initialization by setting instance variables
                (min_time, basename, folder, file_all_routes_gpkg, file_daily_routes_gpkg,
                file_routes_csv, csv_writer_routes, file_transport_types_csv,
                csv_writer_transport_types, file_overnight_hubs_csv, csv_writer_overnight_hubs)
                and creating output files, but does not return a value.
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

        if self.export_all_routes_gpkg:
            filename = os.path.join(self.folder, f"{self.basename}_routes.")
            properties = {'id': 'str',
                          'last_transport_type': 'str',
                          'variant_paths': 'str',  # because some numbers are higher than C's int length
                          'length_hrs': 'int',
                          'arrival_day': 'int',
                          'arrival_hour': 'int',
                          'start_hubs': 'str',
                          'start_times': 'str',
                          'end_hub': 'str',
                          'end_time': 'datetime',
                          'overnight_hubs': 'str',
                          'hubs': 'str',
                          'hub_coordinates': 'str',
                          'edges': 'str',
                          'count_hubs': 'int',
                          'count_edges': 'int'}
            if len(config.means_of_transport) > 0:
                for mean_of_transport in config.means_of_transport:
                    properties['count_' + mean_of_transport] = 'str' # because some numbers are higher than C's int length

            self.file_all_routes_gpkg = fiona.open(filename + 'gpkg', 'w', driver='GPKG', crs='EPSG:4326', schema={'geometry': 'MultiLineString',
                                                                                      'properties': properties})

        if self.export_daily_routes_gpkg:
            filename = os.path.join(self.folder, f"{self.basename}_daily_routes.")
            self.file_daily_routes_gpkg = fiona.open(filename + 'gpkg', 'w', driver='GPKG', crs='EPSG:4326',
                                                   schema={'geometry': 'MultiLineString',
                                                           'properties': {
                                                               'day': 'int',
                                                               'hub': 'str',
                                                               'incoming_routes': 'str',
                                                               'variant_number': 'str', # because some numbers are higher than C's int length
                                                               'agents': 'int',
                                                               'origins': 'str'
                                                           }})

        if self.export_routes_csv:
            self.file_routes_csv = open(filename + 'csv', 'w', newline='')
            self.csv_writer_routes = csv.writer(self.file_routes_csv)
            headers = ['ID', 'Last Transport Type', 'Variant Paths', 'Length (hrs)', 'Arrival Day', 'Arrival Hour',
                 'Start Hubs', 'Start Times', 'End Hub', 'End Time', 'Overnight Hubs', 'Hubs', 'Hub Coordinates', 'Edges',
                       'Number of Hubs', 'Number of Edges']
            if len(config.means_of_transport) > 0:
                for mean_of_transport in config.means_of_transport:
                    headers.append('Count ' + mean_of_transport.replace('_', ' ').title())
            self.csv_writer_routes.writerow(headers)

        if self.export_transport_types_csv:
            filename_transport_types = os.path.join(self.folder, f"{self.basename}_transport_types.csv")
            self.file_transport_types_csv = open(filename_transport_types, 'w', newline='')
            self.csv_writer_transport_types = csv.writer(self.file_transport_types_csv)
            headers = ['ID', 'Edge']
            if len(config.means_of_transport) > 0:
                for mean_of_transport in config.means_of_transport:
                    headers.append(mean_of_transport.replace('_', ' ').title())
            self.csv_writer_transport_types.writerow(headers)

        if self.export_overnight_hubs_csv:
            filename_overnight_hubs = os.path.join(self.folder, f"{self.basename}_overnight_hubs.csv")
            self.file_overnight_hubs_csv = open(filename_overnight_hubs, 'w', newline='')
            self.csv_writer_overnight_hubs = csv.writer(self.file_overnight_hubs_csv)
            self.csv_writer_overnight_hubs.writerow(['Day', 'Hub', 'Incoming Routes', 'Variant Number', 'Agents', 'Origins'])

        logger.info(f"Saving route data to {filename}gpkg, {filename}csv, and {filename_transport_types}.")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
        results: SetOfResults, current_day: int) -> list[Agent]:
        """
        Execute the daily hook to persist agent routes to GeoPackage and CSV files.

        This method is called at the end of each simulation day to save route data for agents
        that have finished their journeys for the day. It initializes output files on first run
        and delegates the actual persistence logic to helper methods. If the hook is configured
        to skip execution (self.skip is True), it returns immediately without processing.

        Args:
            config (Configuration): Configuration object containing simulation settings and parameters,
                used for initialization of output files and determining simulation behavior. Required
                for first-time initialization of output directories and file schemas.
            context (Context): Context object providing access to the route network and other
                simulation state information needed for extracting and persisting route data. Used
                to retrieve geometric and coordinate information from the network graph.
            agents (list[Agent]): List of all active agents in the simulation. This parameter is
                provided by the hook interface but not directly used in this implementation.
            agents_finished_for_today (list[Agent]): List of agents that have completed their
                travel for the current simulation day. These agents' route data will be persisted
                to the configured output files (GeoPackage and/or CSV).
            results (SetOfResults): Container for simulation results and metrics. This parameter
                is provided by the hook interface but not directly used in this implementation.
            current_day (int): The current simulation day number, used for tracking routes per day
                and organizing output data. This value is passed to helper methods for recording
                arrival day information and aggregating daily hub statistics.

        Returns:
            list[Agent]: The same list of agents_finished_for_today that was passed in, allowing
                the simulation to continue processing these agents through other hooks in the
                simulation pipeline.
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
        loss and resource leaks. The method checks each export flag and closes the
        corresponding file handle if it was opened during initialization.

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
        if self.export_all_routes_gpkg:
            self.file_all_routes_gpkg.close()
        if self.export_daily_routes_gpkg:
            self.file_daily_routes_gpkg.close()
        if self.export_routes_csv:
            self.file_routes_csv.close()
        if self.export_transport_types_csv:
            self.file_transport_types_csv.close()
        if self.export_overnight_hubs_csv:
            self.file_overnight_hubs_csv.close()

    def _persist_agents(self, agents: list[Agent], config: Configuration, context: Context, current_day: int):
        """
        Persist agent route data to GeoPackage and CSV files for the current simulation day.

        This method processes a list of agents to save their route information. For agents that
        have completed their journeys (finished or reached simulation end points), it extracts
        their complete route data and writes it to both GeoPackage and CSV output files. For
        agents still in transit, it updates their hub status for tracking across multiple days.
        After processing all agents, it aggregates hub data from the previous day and prepares
        for the next simulation day by rotating hub tracking dictionaries.

        Args:
            agents (list[Agent]): List of agents to process for the current day. This includes
                both agents that have finished their journeys and agents that are still traveling.
                Cancelled agents in this list are ignored and not persisted.
            config (Configuration): Configuration object containing simulation settings, including
                simulation_ends (list of hub IDs where simulation can terminate) and means_of_transport
                (list of transport types used in the simulation). These settings determine which
                agents are considered finished and what data to track.
            context (Context): Context object providing access to the route network graph, used
                to retrieve geometric and coordinate information for constructing spatial data
                representations of agent routes.
            current_day (int): The current simulation day number, used for recording arrival
                day information and tracking hub status across multiple days.

        Returns:
            None: This method performs file I/O operations to persist agent data and updates
                internal tracking dictionaries (self.hubs, self.hubs_yesterday), but does not
                return a value.
        """
        # aggregate the agents and save their data into a GeoPackage file
        agent_data = []

        for agent in agents:
            # ignore canceled agents
            if agent.is_cancelled:
                continue
            # aggregate the agent's node status
            self._update_hubs_status(config, agent, current_day)

        # update hubs with yesterday's data
        self._update_hubs(config, context, current_day)

        for agent in agents:
            if agent.is_finished or (config.simulation_ends and agent.this_hub in config.simulation_ends):
                data = self._get_agent_data(context, config, agent, current_day)
                agent_data.append(data)
                if self.export_routes_csv:
                    self.csv_writer_routes.writerow(data['properties'].values())

        # move today's hubs to yesterday's hubs
        self.hubs_yesterday = self.hubs
        self.hubs = {}

        if self.export_all_routes_gpkg and len(agent_data) > 0:
            self.file_all_routes_gpkg.writerecords(agent_data)

    def _get_agent_data(self, context: Context, config: Configuration, agent: Agent, current_day: int):
        """
        Extract and compile comprehensive route data for a finished agent.

        This method aggregates all route information for an agent that has completed its journey,
        including both the current day's travel and any accumulated data from previous days. It
        constructs a complete picture of the agent's multi-day journey by merging historical hub
        data with the current route, calculates statistics about transport types and route variants,
        extracts geometric data for spatial representation, and writes transport type information
        to CSV. The compiled data is formatted for output to both GeoPackage and CSV files.

        Args:
            context (Context): Context object providing access to the route network graph,
                used to retrieve geometric data (LineStrings) for edges and coordinate
                information for hubs visited during the agent's journey.
            config (Configuration): Configuration object containing simulation settings,
                particularly the means_of_transport list which determines which transport
                type counters need to be calculated and included in the output data.
            agent (Agent): The agent whose route data is being extracted. Must be a finished
                agent with a complete route. The agent's uid, route, transport_types,
                current_time, and start/end information are used to compile the output data.
            current_day (int): The current simulation day number when the agent finished,
                used to record the arrival day in the output properties.

        Returns:
            dict: A dictionary containing two keys:
                - 'geometry': A MultiLineString geometry object (forced to 2D) representing
                  the complete spatial path of the agent's journey, composed of all edges
                  traversed across all days.
                - 'properties': A dictionary of route attributes including agent ID, transport
                  types, timing information, hub and edge lists, coordinates, counts, and
                  statistics suitable for writing to GeoPackage and CSV files.
        """
        start_hub, end_hub, start_delta, _ = agent.get_start_end()

        hubs = set()
        edges = set()
        route_count = 1
        overnight_hubs = set()
        start_hubs = set()
        start_times = set()
        means_of_transport = {}
        edge_transport_types = {}

        if start_hub in self.hubs_yesterday:
            hub = self.hubs_yesterday[start_hub]
            route_count = hub['number_incoming_routes']

            hubs.update(hub['hubs'])
            edges.update(hub['edges'])

            overnight_hubs.update(hub['overnight_hubs'])
            start_hubs = hub['start_hubs']
            start_times = hub['start_times']

            start_delta = hub['min_delta']

            if len(config.means_of_transport) > 0:
                for mean_of_transport in config.means_of_transport:
                    means_of_transport[mean_of_transport] = hub['count_' + mean_of_transport]

            edge_transport_types = hub['edge_transport_types']
        else:
            # case when route is one day only
            start_hubs.add(start_hub)

            start_time = self.min_time + dt.timedelta(hours=start_delta)
            start_times.add(start_time.strftime('%Y-%m-%d %H:%M'))

            if len(config.means_of_transport) > 0:
                for mean_of_transport in config.means_of_transport:
                    means_of_transport[mean_of_transport] = 0

        if current_day > 1:
            overnight_hubs.add(start_hub + ':' + str(current_day-1))

        hubs.update(agent.route[::2])
        edges.update(agent.route[1::2])

        end_time = self.min_time + dt.timedelta(hours=agent.current_time)

        # add rest of transport types
        if len(config.means_of_transport) > 0:
            for i, edge in enumerate(agent.route[1::2]):
                t_type = agent.transport_types[i]
                means_of_transport[t_type] += 1

                if edge in edge_transport_types:
                    edge_transport_types[edge][t_type] += 1
                else:
                    edge_transport_types[edge] = {}
                    for mean_of_transport in config.means_of_transport:
                        edge_transport_types[edge][mean_of_transport] = 1 if t_type == mean_of_transport else 0

        lines: list[MultiLineString] = []
        for edge in edges:
            lines.append(context.routes.es.find(name=edge)['geom'])
            data = [agent.uid, edge]
            for value in edge_transport_types[edge].values():
                data.append(value)

            # also write data to transport types csv
            if self.export_transport_types_csv:
                self.csv_writer_transport_types.writerow(data)

        hub_coordinates = []
        for hub in context.routes.vs.select(name_in=hubs):
            hub_coordinates.append(f"{hub['geom'].x},{hub['geom'].y}")

        properties = {
            'id': agent.uid,
            'last_transport_type': agent.transport_type,
            'variant_paths': str(route_count),
            'length_hrs': int(agent.current_time - start_delta),
            'arrival_day': current_day,
            'arrival_hour': math.floor(agent.current_time % 24),
            'start_hubs': ', '.join(start_hubs),
            'start_times': ', '.join(start_times),
            'end_hub': end_hub,
            'end_time': str(end_time.strftime('%Y-%m-%d %H:%M')),
            'overnight_hubs': ','.join(overnight_hubs),
            'hubs': ','.join(list(hubs)),
            'hub_coordinates': ' '.join(hub_coordinates),
            'edges': ','.join(list(edges)),
            'count_hubs': len(hubs),
            'count_edges': len(edges),
        }

        for key, value in means_of_transport.items():
            properties['count_' + key] = str(value)

        return {'geometry': force_2d(MultiLineString(lines)), 'properties': properties}

    def _update_hubs_status(self, config: Configuration, agent: Agent, current_day: int):
        """
        Update the status of hubs for the current day based on an agent's route information.

        This method tracks and aggregates route data for agents that have not yet completed
        their journeys. It maintains a dictionary of hub states, recording information about
        routes passing through each hub, including the hubs and edges visited, transport types
        used, and timing information. For agents on the first day of simulation, it also
        records their starting locations and times. This data is later used to reconstruct
        complete multi-day routes when agents finish their journeys.

        Args:
            config (Configuration): Configuration object containing simulation settings,
                particularly the means_of_transport list which determines which transport
                type counters need to be initialized and updated for each hub.
            agent (Agent): The agent whose route information is being processed. The agent's
                route, transport_types, and start/end information are extracted to update
                the hub status. The agent should be one that has not yet finished its journey.
            current_day (int): The current simulation day number. When current_day is 1,
                additional initialization is performed to record the agent's starting hub,
                start time, and initial time delta for first-day tracking.

        Returns:
            None: This method updates the self.hubs dictionary in place and does not
                return a value.
        """
        start_hub, end_hub, start_delta, end_delta = agent.get_start_end()

        # find hub
        if end_hub not in self.hubs:
            self.hubs[end_hub] = {
                'routes': set(),
                'hubs': set(),
                'edges': set(),
                'number_incoming_routes': 0,
                'agents': 0,
                'start_hubs': set(),
                'start_times': set(),
                'min_delta': start_delta,
                'overnight_hubs': set(),
                'edge_transport_types': {},
            }

            # init means of transport
            if len(config.means_of_transport) > 0:
                for mean_of_transport in config.means_of_transport:
                    self.hubs[end_hub]['count_' + mean_of_transport] = 0

        # update hub data
        edges = agent.route[1::2]

        self.hubs[end_hub]['routes'].add(tuple(agent.route))
        self.hubs[end_hub]['hubs'].update(agent.route[::2])
        self.hubs[end_hub]['edges'].update(edges)
        if current_day > 1:
            self.hubs[end_hub]['overnight_hubs'].add(agent.route[0] + ':' + str(current_day-1))
        self.hubs[end_hub]['agents'] += 1

        start_time = self.min_time + dt.timedelta(hours=start_delta)

        if len(config.means_of_transport) > 0:
            for i, edge in enumerate(edges):
                t_type = agent.transport_types[i]
                self.hubs[end_hub]['count_' + t_type] += 1
                if edge in self.hubs[end_hub]['edge_transport_types']:
                    self.hubs[end_hub]['edge_transport_types'][edge][t_type] += 1
                else:
                    self.hubs[end_hub]['edge_transport_types'][edge] = {}
                    for mean_of_transport in config.means_of_transport:
                        self.hubs[end_hub]['edge_transport_types'][edge][mean_of_transport] = 1 if mean_of_transport == t_type else 0

        # start of simulation?
        if current_day == 1:
            self.hubs[end_hub]['start_hubs'].add(start_hub)
            self.hubs[end_hub]['start_times'].add(start_time.strftime('%Y-%m-%d %H:%M'))
            self.hubs[end_hub]['min_delta'] = start_delta

    def _update_hubs(self, config: Configuration, context: Context, current_day: int):
        """
        Update current day's hub data by aggregating information from previous day's hubs.

        This method traverses all routes in today's hubs and merges historical data from
        yesterday's hubs where routes connect. For each route starting at a hub that existed
        yesterday, it accumulates route counts, visited hubs, traversed edges, overnight
        locations, start points, timing information, and transport type statistics. This
        creates a cumulative view of multi-day journeys as agents progress through the network.

        Args:
            config (Configuration): Configuration object containing simulation settings,
                particularly the means_of_transport list which determines which transport
                type counters need to be updated during the aggregation process.

        Returns:
            None: This method updates the self.hubs dictionary in place and does not
                return a value.
        """
        daily_route_records = []

        for key, hub in self.hubs.items():
            origins = {}
            origins_numbers = {}
            # traverse routes and compare to yesterday's routes'
            for route in hub['routes']:
                start_hub = route[0]
                if start_hub in origins:
                    origins[start_hub] += 1
                else:
                    origins[start_hub] = 1
                if start_hub in self.hubs_yesterday:
                    hub_yesterday = self.hubs_yesterday[start_hub]
                    hub['number_incoming_routes'] += hub_yesterday['number_incoming_routes']
                    hub['hubs'].update(hub_yesterday['hubs'])
                    hub['edges'].update(hub_yesterday['edges'])
                    hub['overnight_hubs'].update(hub_yesterday['overnight_hubs'])
                    hub['start_hubs'].update(hub_yesterday['start_hubs'])
                    hub['start_times'].update(hub_yesterday['start_times'])
                    hub['min_delta'] = min(hub_yesterday['min_delta'], hub['min_delta'])
                    origins_numbers[start_hub] = hub_yesterday['number_incoming_routes']

                    if len(config.means_of_transport) > 0:
                        for mean_of_transport in config.means_of_transport:
                            hub['count_' + mean_of_transport] += hub_yesterday['count_' + mean_of_transport]

                    for edge in hub_yesterday['edge_transport_types']:
                        if edge in hub['edge_transport_types']:
                            for t_type in hub_yesterday['edge_transport_types'][edge]:
                                hub['edge_transport_types'][edge][t_type] += hub_yesterday['edge_transport_types'][edge][t_type]
                        else:
                            hub['edge_transport_types'][edge] = copy.deepcopy(hub_yesterday['edge_transport_types'][edge])
                else:
                    hub['number_incoming_routes'] += 1
                    origins_numbers[start_hub] = 1

            if self.export_overnight_hubs_csv or self.export_daily_routes_gpkg:
                origins_txt = []
                for o_key in sorted(origins):
                    origins_txt.append(f"{origins[o_key]} × {o_key} ({origins_numbers[o_key]})")

            if self.export_overnight_hubs_csv:
                self.csv_writer_overnight_hubs.writerow([current_day, key, len(hub['routes']), hub['number_incoming_routes'], hub['agents'], ' '.join(origins_txt)])

            if self.export_daily_routes_gpkg:
                edges_today = set()
                for route in hub['routes']:
                    edges_today.update(route[1::2])

                lines: list[MultiLineString] = []
                for edge in edges_today:
                    lines.append(context.routes.es.find(name=edge)['geom'])

                daily_route_records.append({'geometry': force_2d(MultiLineString(lines)), 'properties': {
                    'day': current_day,
                    'hub': key,
                    'incoming_routes': len(hub['routes']),
                    'variant_number': str(hub['number_incoming_routes']),
                    'agents': hub['agents'],
                    'origins': '\n'.join(origins_txt),
                }})

        if self.export_daily_routes_gpkg and len(daily_route_records) > 0:
            self.file_daily_routes_gpkg.writerecords(daily_route_records)
