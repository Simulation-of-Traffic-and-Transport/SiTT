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
import hashlib
import logging
from enum import Enum
from typing import Dict, List, Tuple

import geopandas as gp
import networkx as nx
import yaml

__all__ = [
    "SkipStep",
    "Configuration",
    "Context",
    "Status",
    "StepData",
    "State",
    "SetOfResults",
    "PreparationInterface",
    "SimulationInterface",
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
        self.simulation: List[SimulationInterface] = []
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
# State
########################################################################################################################


class Status(Enum):
    """Representation of the status of a state"""
    RUNNING = 0
    """still running"""
    FINISHED = 1
    """finished"""
    CANCELLED = 2
    """cancelled for some reason"""

    def __str__(self):
        return self.name


class StepData(object):
    """Step data object, this will take the current step data - temporary object"""

    def __init__(self, start_idx: int):
        self.current_leg: int = start_idx
        self.next_leg: int = start_idx
        """Next leg after this whole day, default is 8.0, this can be changed using the before step"""
        self.time_available: float = 8.0
        """Time available this step"""
        self.time_used: float = 0.
        """Time used during this step"""
        # this class can contain any custom data


class State(object):
    """State class - this will take information on the current state of a simulation step"""

    def __init__(self, path: List[Tuple]):
        if len(path) == 0:
            raise ValueError("path must not be empty")

        # base data
        self.path: List[Tuple] = path
        """Full path for this state"""
        self.roads: List[str] = [x[2] for x in path]
        """Road list to take"""
        self.hubs: List[str] = [path[0][0]] + [x[1] for x in path]

        # overall data
        self.status: Status = Status.RUNNING
        """Current status"""
        self.step: int = 1
        """Current step"""
        self.current_leg: int = 0
        """Current leg of journey (index of current path entry, if finished, index should be len(self.path) + 1)"""

        # future data - calculated during each simulation step
        self.step_data: StepData = StepData(0)
        """Step data for next step"""

        # construct id
        self.id = ' → '.join(self.roads)
        """Human readable id"""
        self.uid = hashlib.md5(self.id.encode()).hexdigest()
        """md5 digest of id"""
        # this class can contain any custom data

    def initialize_next_step(self):
        """Initialize next step - run before each simulation step"""
        self.step_data = StepData(self.current_leg)

    def has_advanced_today(self) -> bool:
        """Return true if next leg greater than previous one"""
        return self.step_data.next_leg > self.current_leg

    def get_current_start_hub(self) -> str:
        """Get current start hub"""
        return self.hubs[self.current_leg]

    def get_total_length_m(self, context: Context) -> float:
        total: float = 0.

        """Get total length of this state path in m"""
        for path in self.path:
            p = context.get_path_by_id(path)
            if p:
                total += p['length_m']

        return total

    def advance(self):
        if self.status is Status.CANCELLED:
            return

        """advance a step"""
        self.current_leg = self.step_data.next_leg

        # finished?
        if self.current_leg >= len(self.path):
            self.status = Status.FINISHED
        else:
            self.step += 1

    def __str__(self):
        return "State " + self.uid + ' (' + self.id + '), ' + str(self.status)

    def __getstate__(self):
        state = self.__dict__.copy()
        # delete certain stuff we cannot or do not want to picke
        del state['step_data']


########################################################################################################################
# Set of Results
########################################################################################################################


class SetOfResults:
    """Set of results represents the results of a simulation"""
    pass


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


class SimulationInterface(abc.ABC):
    """
    Simulation module interface
    """

    def __init__(self):
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def run_before(self, config: Configuration, context: Context, state: State) -> State:
        """
        Run the simulation module - run at the start of each simulation step, should be used as preparation for the
        actual simulation.

        :param config: configuration (read-only)
        :param context: context (read-only)
        :param state: state of current agent
        :return: updated state object
        """
        pass

    @abc.abstractmethod
    def run(self, config: Configuration, context: Context, state: State) -> State:
        """
        Run the simulation module - main step of the simulation. Should modify the next step entry, so the state can
        advance after this step.

        :param config: configuration (read-only)
        :param context: context (read-only)
        :param state: state of current agent
        :return: updated state object
        """
        pass

    @abc.abstractmethod
    def run_after(self, config: Configuration, context: Context, state: State) -> State:
        """
        Run the simulation module - run at the end of each simulation step, should be used to clean up stuff.

        :param config: configuration (read-only)
        :param context: context (read-only)
        :param state: state of current agent
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
    def run(self, config: Configuration, context: Context, set_of_results: SetOfResults):
        """
        Run the output module

        :param config: configuration (read-only)
        :param context: context (read-only)
        :param set_of_results: set of results (read-only)
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
