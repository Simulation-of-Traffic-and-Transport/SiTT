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
import logging
from enum import Enum
from typing import Dict, List

import geopandas as gp
import networkx as nx
import yaml

from utils import generate_uid

__all__ = [
    "SkipStep",
    "Configuration",
    "Context",
    "State",
    "Agent",
    "SetOfResults",
    "PreparationInterface",
    "SimulationPrepareDayInterface",
    "SimulationDefineStateInterface",
    "SimulationStepInterface",
    "OutputInterface",
]


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
        self.signal_stop_here: bool = False
        """Signal forced stop here"""

    def prepare_for_new_day(self) -> State:
        """Prepare state for new day"""
        self.time_taken = 0.
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

    def prepare_for_new_day(self):
        """reset to defaults for a day"""
        self.current_time = 8.
        self.max_time = 16.
        self.last_possible_resting_place = self.this_hub
        self.last_possible_resting_time = self.current_time
        self.state = self.state.prepare_for_new_day()

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

########################################################################################################################
# Abstract base classes
########################################################################################################################

# TODO: move to own file?
# TODO

########################################################################################################################
# Core + Simulation stuff
########################################################################################################################

# TODO
