# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation base classes.

.. warning::
    This module is treated as private API.
    Users should not need to use this module directly.
"""

from __future__ import annotations

import abc
import datetime as dt
import logging
from enum import Enum
from typing import Dict, List

import geopandas as gp
import nanoid
import netCDF4 as nc
import networkx as nx
import numpy as np
import yaml

__all__ = [
    "SkipStep",
    "Configuration",
    "Context",
    "State",
    "SpaceTimeData",
    "Agent",
    "SetOfResults",
    "PreparationInterface",
    "SimulationPrepareDayInterface",
    "SimulationDefineStateInterface",
    "SimulationStepInterface",
    "OutputInterface",
]


########################################################################################################################
# Utilities
########################################################################################################################

def generate_uid() -> str:
    return nanoid.generate('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 12)


########################################################################################################################
# Configuration
########################################################################################################################

class SkipStep(Enum):
    """
    Enum to represent skipped steps when running core
    """
    NONE = "none"
    SIMULATION = "simulation"
    OUTPUT = "output"

    def __str__(self):
        return self.value


class Configuration:
    """
    Class containing the configuration obtained from the command line or created programmatically. Will be created
    by the Preparation class (reparation.py) and passed to the simulation component (sim.py).
    """

    def __init__(self):
        self.verbose: bool = False
        """
        More verbose output/logging
        """
        self.quiet: bool = False
        """
        Suppress output/logging
        """
        self.skip_step: SkipStep = SkipStep.NONE
        """
        Skip certain steps in the execution
        """
        self.preparation: List[PreparationInterface] = []
        """
        Preparation step classes to execute
        """
        self.simulation_prepare_day: List[SimulationPrepareDayInterface] = []
        """simulation hook classes that are executed on each agent at the start of the day"""
        self.simulation_define_state: List[SimulationDefineStateInterface] = []
        """simulation hook classes that are executed on each agent at the start of the day"""

        self.simulation_step: List[SimulationStepInterface] = []
        """
        Simulation step classes to execute
        """
        self.output: List[OutputInterface] = []
        """
        Output step classes to execute
        """
        self.simulation_start: str | None = None
        """"Start hub for simulation"""
        self.simulation_end: str | None = None
        """"End hub for simulation"""
        self.start_date: dt.date | None = None
        """used as global start date (e.g. in nc files)"""

        self.break_simulation_after: int = 100
        """Break single simulation entity after not advancing for this many steps"""

        # define logging
        logging.basicConfig(format='%(asctime)s %(message)s')

    def __setattr__(self, att, value):
        # observe changes in logger settings
        if att == 'verbose' and value:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
        if att == 'quiet' and value:
            logger = logging.getLogger()
            logger.setLevel(logging.ERROR)
        return super().__setattr__(att, value)

    def __repr__(self):
        return yaml.dump(self)

    def __getstate__(self):
        state = self.__dict__.copy()
        # delete out, because we cannot pickle this
        if 'out' in state:
            del state['out']

        if state['skip_step'] != SkipStep.NONE:
            state['skip_step'] = state['skip_step'].value
        else:
            del state['skip_step']

        return state


########################################################################################################################
# Context
########################################################################################################################

class SpaceTimeData(object):
    """Keeps spacial and temporal data - NetCDF format."""

    def __init__(self, data: nc.Dataset, variables: dict[str, dict[str, any]], latitude: str = 'latitude',
                 longitude: str = 'longitude', time: str = 'time', start_date: dt.date | None = None):
        # self.data: Dataset = data
        #
        # self.latitude: str = latitude
        # """Name of latitude in dataset"""
        # self.longitude: str = longitude
        # """Name of longitude in dataset"""
        # self.time: str = time
        # """Name of time in dataset"""
        # self.variables: dict[str, dict[str, any]] = variables
        # """Variables to map values on"""
        self.start_date: dt.date | None = start_date
        """Start date different from global one."""

        # create aggregated data
        self.lat: np.ma.core.MaskedArray = data.variables[latitude][:]
        """latitude array"""
        self.lon: np.ma.core.MaskedArray = data.variables[longitude][:]
        """longitude array"""
        self.times: nc.Variable = data.variables[time]
        """time dataset"""

        # add variables
        self.variables: Dict[str, nc.Variable] = {}
        self.offsets: Dict[str, float] = {}
        for key in variables:
            var_name = key
            if 'variable' in variables[key]:
                var_name = variables[key]['variable']
            if var_name in data.variables:
                self.variables[key] = data.variables[var_name]
                if 'offset' in variables[key]:
                    self.offsets[key] = variables[key]['offset']
            else:
                logging.getLogger().error(data.variables)
                raise Exception('Variable does not exist in dataset: ' + var_name)

        # set min/max values for quicker tests below
        self.min_lat = self.lat.min()
        self.max_lat = self.lat.max()
        self.min_lon = self.lon.min()
        self.max_lon = self.lon.max()
        times = self.times[:]
        self.min_times = times.min()
        self.max_times = times.max()

    def _get_date_number(self, day: int, hours: float, config: Configuration) -> float | None:
        """
        Returns date number for given day and configuration - returns none if datetimes have not been set

        :param day: day offset (starts with 1)
        :param hours: time offset in day (hours)
        :param config: configuration

        :return: date number or None
        """
        # own or global date?
        if self.start_date is not None:
            start_date = self.start_date
        else:
            start_date = config.start_date

        if start_date is None:
            return None

        my_datetime = dt.datetime(start_date.year, start_date.month, start_date.day) + dt.timedelta(days=day-1, hours=hours)

        return nc.date2num(my_datetime, self.times.units, calendar=self.times.calendar, has_year_zero=False)

    def _in_bounds(self, lat: float, lon: float, date_number: float) -> bool:
        """
        Tests if lat, lon and time are within the bounds of the dataset
        :param lat: latitude
        :param lon: longitude
        :param date_number: date number
        :return: true if in bounds, false otherwise
        """
        if self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon and self.min_times <= date_number <= self.max_times:
            return True

        return False

    def get(self, lat: float, lon: float, day: int, hours: float, config: Configuration,
            fields: list[str] | None = None) -> dict[str, any] | None:
        # convert to date number
        date_num = self._get_date_number(day, hours, config)
        if date_num is None:
            return None

        # check bounds
        if not self._in_bounds(lat, lon, date_num):
            return None

        # add all fields, if none have been set
        if fields is None or len(fields) == 0:
            fields = list(self.variables.keys())

        # find the closest indexes
        lat_idx = (np.abs(self.lat - lat)).argmin()
        lon_idx = (np.abs(self.lon - lon)).argmin()
        time_idx = (np.abs(self.times[:] - date_num)).argmin()

        # aggregate variables
        variables: dict[str, any] = {}

        for field in fields:
            if field in self.variables:
                value = self.variables[field][time_idx][lat_idx][lon_idx]

                # apply offset, if it exists
                if field in self.offsets:
                    value += self.offsets[field]

                variables[field] = value

        return variables


class Context(object):
    """The context object is a read-only container for simulation threads."""

    def __init__(self):
        # raw data
        self.raw_roads: gp.geodataframe.GeoDataFrame | None = None
        self.raw_hubs: gp.geodataframe.GeoDataFrame | None = None

        self.graph: nx.MultiGraph | None = None
        """Graph data for roads and other ways"""
        self.routes: nx.MultiDiGraph | None = None
        """
        Path to be traversed from start to end - it is a directed version of the graph above. Used by the simulation to
        find the correct route.
        """
        self.space_time_data: Dict[str, SpaceTimeData] = {}

    def get_path_by_id(self, path_id) -> Dict | None:
        """Get path by id"""
        if self.graph:
            return self.graph.get_edge_data(path_id[0], path_id[1], path_id[2])
        return None

    def get_hub_by_id(self, hub_id) -> Dict | None:
        """Get hub by id"""
        if self.graph:
            return self.graph.nodes()[hub_id]
        return None

    def get_directed_path_by_id(self, path_id, start_hub) -> Dict | None:
        """Get path by id and set `is_reversed` attribute, if start_hub is not hubaid of path"""
        path = self.get_path_by_id(path_id)

        if not path:
            return None

        path = path.copy()
        path['is_reversed'] = start_hub != path['hubaid']

        return path


########################################################################################################################
# Agent and State
########################################################################################################################

class State(object):
    """State class - this will take information on the current state of a simulation agent, it will be reset each day"""

    def __init__(self):
        self.uid: str = generate_uid()
        """unique id"""

        self.time_taken: float = 0.
        """Time taken in this step"""
        self.time_for_legs: List[float] = []
        """Time taken for all legs of this step"""
        self.data_for_legs: List[Dict[str, any]] = []
        """Environmental data for each leg"""
        self.signal_stop_here: bool = False
        """Signal forced stop here"""

    def reset(self) -> State:
        """Prepare state for new day"""
        self.time_taken = 0.
        self.time_for_legs = []
        self.data_for_legs = []
        self.signal_stop_here = False

        return self
    #
    # def hash(self) -> str:
    #     """Return unique id of this state"""
    #     return ''


class Agent(object):
    """Agent - simulating single travelling entity at a specific time and date"""

    def __init__(self, this_hub: str, next_hub: str, route_key: str, state: State | None = None,
                 current_time: float = 0., max_time: float = 0.):
        self.uid: str = generate_uid()
        """unique id"""

        """read-only reference to context"""
        if state is None:
            state = State()
        self.state: State = state
        """state of agent"""

        self.this_hub: str = this_hub
        """Current hub"""
        self.next_hub: str = next_hub
        """Destination hub"""
        self.route_key: str = route_key
        """Key/vertex id of route between hubs"""

        self.current_day: int = 1
        """Current day of agent - copied from simulation"""
        self.current_time: float = current_time
        """Current time stamp of agent during this day"""
        self.max_time: float = max_time
        """Current maximum timestamp for this day"""

        self.day_finished: int = -1
        """finished at this day"""
        self.day_cancelled: int = -1
        """cancelled at this day"""
        self.tries: int = 0
        """internal value for tries at this hub - will break at 100"""

        self.route_data: nx.MultiDiGraph = nx.MultiDiGraph()
        """keeps route taken"""
        self.last_possible_resting_place: str = this_hub
        """keeps last possible resting place"""
        self.last_possible_resting_time: float = current_time
        """keeps timestamp of last resting place"""

    def prepare_for_new_day(self, current_day: int = 1):
        """reset to defaults for a day"""
        self.current_day = current_day
        self.current_time = 8.
        self.max_time = 16.
        self.last_possible_resting_place = self.this_hub
        self.last_possible_resting_time = self.current_time
        self.state = self.state.reset()

    def __repr__(self) -> str:
        if self.day_finished >= 0:
            return f'Agent {self.uid} ({self.this_hub}) - [finished day {self.day_finished}, {self.current_time:.2f}]'
        if self.day_cancelled >= 0:
            return f'Agent {self.uid} ({self.this_hub}->{self.next_hub} [{self.route_key}]) - [cancelled day {self.day_cancelled}, {self.current_time:.2f}]'
        return f'Agent {self.uid} ({self.this_hub}->{self.next_hub} [{self.route_key}]) [{self.current_time:.2f}/{self.max_time:.2f}]'

    def __eq__(self, other) -> bool:
        return self.this_hub == other.this_hub and self.next_hub == other.next_hub and self.route_key == other.route_key

    def hash(self) -> str:
        return self.this_hub + self.next_hub + self.route_key

    def generate_uid(self) -> str:
        """(re)generate unique id (nanoid) of agent"""
        self.uid = generate_uid()
        return self.uid


########################################################################################################################
# Set of Results
########################################################################################################################

class SetOfResults:
    """Set of results represents the results of a simulation"""

    def __init__(self):
        self.agents_finished: List[Agent] = []
        """keeps list of finished agents"""
        self.agents_cancelled: List[Agent] = []
        """keeps list of cancelled agents"""

    def __repr__(self) -> str:
        return yaml.dump(self)

    def __str__(self):
        return "SetOfResults"


########################################################################################################################
# Preparation, Simulation, and Output Interfaces
########################################################################################################################

class PreparationInterface(abc.ABC):
    """
    Preparation module interface
    """

    def __init__(self):
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def run(self, config: Configuration, context: Context) -> Context:
        """
        Run the preparation module

        :param config: configuration (read-only)
        :param context: context (can be changed and returned)
        :return: updated context object
        """
        pass


class SimulationPrepareDayInterface(abc.ABC):
    """
    Simulation module interface for hooks starting at a new day
    """

    def __init__(self):
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def prepare_for_new_day(self, config: Configuration, context: Context, agent: Agent):
        pass


class SimulationDefineStateInterface(abc.ABC):
    """
    Simulation module interface for hooks defining the state of an agent at each node
    """

    def __init__(self):
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def define_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        pass


class SimulationStepInterface(abc.ABC):
    """
    Simulation step module interface - core of interface defining state
    """

    def __init__(self):
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def update_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        """
        Run the simulation module - run at the start of each simulation step, should be used as preparation for the
        actual simulation.

        :param config: configuration (read-only)
        :param context: context (read-only)
        :param agent: current agent (contains state object)
        :return: updated state object
        """
        pass


class OutputInterface(abc.ABC):
    """
    Output module interface
    """

    def __init__(self):
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def run(self, config: Configuration, context: Context, set_of_results: SetOfResults) -> any:
        """
        Run the output module

        :param config: configuration (read-only)
        :param context: context (read-only)
        :param set_of_results: set of results (read-only)
        :return: any output data
        """
        pass
