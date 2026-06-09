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
    """
    Handles the persistence of agent routes and metadata to a GeoPackage file.

    This class is responsible for saving agent simulation data into a GeoPackage format. It ensures
    that data is recorded in a structured way, allowing for easy analysis and visualization of
    agent movement and routes. The class manages the lifecycle of the GeoPackage file and ensures
    proper initialization, updating, and cleanup operations.

    :ivar delete_existing_folder: Indicates whether the existing folder should be deleted before
        starting a new simulation run.
    :type delete_existing_folder: bool
    :ivar basename: The base name for the output files and folder, generated based on simulation
        route and start date.
    :type basename: str | None
    :ivar folder: The folder path where the GeoPackage file and other data will be stored.
    :type folder: str | None
    :ivar file: The fiona file handle representing the GeoPackage where the agent data will be
        persisted.
    :type file: fiona.Collection | None
    :ivar min_time: The minimal start time for the simulation, derived from the configuration start
        date.
    :type min_time: datetime.datetime
    :ivar route_origins: A dictionary to track the origins of agent routes. Used to map and
        aggregate data across the simulation.
    :type route_origins: dict
    """
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
        """
        Initializes the necessary components for simulation data storage and prepares the
        environment for storing agent information.

        This method sets the minimum simulation time, creates a structured folder for storing
        simulation data, and opens a GeoPackage (GPKG) file for writing agent-related information.
        If pre-existing data exists and the configuration allows, it removes the old folder before
        creating a new one.

        :param config: The configuration object containing simulation-related settings.
            Must include `start_date` (datetime), `simulation_route` (string), and any
            additional settings required for the initialization process.
        :type config: Configuration

        :return: None
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

        filename = os.path.join(self.folder, f"{self.basename}_agents.gpkg")
        self.file = fiona.open(filename, 'w', driver='GPKG', layer='agents', crs='EPSG:4326',
                                schema={'geometry': 'LineString',
                                        'properties': {'id': 'str', 'type': 'str', 'start_hub': 'str', 'end_hub': 'str',
                                                       'day': 'int', 'length_hrs': 'float', 'start_time': 'datetime',
                                                       'end_time': 'datetime', 'is_finished': 'bool', 'stops': 'str',
                                                       'hubs': 'str', 'edges': 'str'}})

        logger.info(f"Saving agent data to {filename}.")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        """
        Executes the main behavior of the method based on the provided parameters to manage
        and persist the state of agents. If the `skip` attribute is set to `True`, the
        function bypasses its normal operation and immediately returns the
        `agents_finished_for_today`.

        :param config: Configuration object containing the necessary settings for initialization
                       and persistence operations.
        :type config: Configuration
        :param context: Context object providing contextual information required during the
                        operation.
        :type context: Context
        :param agents: List of Agent objects that are being processed during the method's execution.
        :type agents: list[Agent]
        :param agents_finished_for_today: List of Agent objects that have completed their tasks for
                                           the current day and require persistence.
        :type agents_finished_for_today: list[Agent]
        :param results: Aggregated results from the ongoing operations or simulations being managed
                        by the method.
        :type results: SetOfResults
        :param current_day: The current day identifier used to determine the scope and progression
                            of the process.
        :type current_day: int
        :return: List of Agent objects that have completed their tasks for the current day and have
                 been persisted successfully.
        :rtype: list[Agent]
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
        self.file.close()

    def _persist_agents(self, agents: list[Agent], config: Configuration, context: Context, current_day: int):
        """
        Aggregates agent data and persists it into a GeoPackage file.

        This method processes a list of agents, filtering out canceled agents and determining
        their completion status based on the simulation configuration and hub statuses. The
        processed data for valid agents is then written to a GeoPackage file using the file
        writer associated with the class.

        :param agents: List of agent objects to be processed.
        :param config: Configuration object containing simulation settings and parameters.
        :param context: Context object providing necessary execution context.
        :param current_day: The current simulation day as an integer.
        :return: None
        """
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
        """
        Retrieves agent data in the form of a dictionary containing geometry and properties.

        The method processes the agent's route and constructs geometry details, including the
        route's coordinates, determining whether they need to be reversed. It also computes
        route-specific properties such as start and end hubs, time details, and additional metadata.
        This information is returned in a structured format ready for further use.

        :param context: A `Context` object that provides the larger environment
            and facilitates access to geospatial data.
        :param agent: An `Agent` object whose routing data and properties
            are being analyzed.
        :param current_day: An integer representing the current simulation day.
        :param is_finished: A boolean indicating whether the agent's task is
            complete or still in progress.
        :return: A dictionary containing the processed geometry and associated
            properties for the specified agent.
        :rtype: dict
        """
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
            'type': agent.transport_type,
            'start_hub': start_hub,
            'end_hub': end_hub,
            'day': current_day,
            'length_hrs': end_delta - start_delta,
            'start_time': start_time,
            'end_time': end_time,
            'is_finished': is_finished,
            'stops': str(agent.rest_history),
            'hubs': ','.join(agent.route[::2]),
            'edges': ','.join(agent.route[1::2]),
        }}
