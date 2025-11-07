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
from typing import Generator

import igraph as ig
import nanoid
import yaml

# import directly and export to __init__.py
from .spatio_temporal_data import SpatioTemporalInterface, SpaceTimeData, SpaceData

__all__ = [
    "SkipStep",
    "Configuration",
    "Context",
    "State",
    "SpatioTemporalInterface",
    "SpaceTimeData",
    "SpaceData",
    "Agent",
    "SetOfResults",
    "PreparationInterface",
    "SimulationDayHookInterface",
    "SimulationStepHookInterface",
    "SimulationStepInterface",
    "OutputInterface",
]

########################################################################################################################
# Utilities
########################################################################################################################

id_counter = 0


def generate_nanoid() -> str:
    return nanoid.generate('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 12)


def generate_id() -> str:
    """This utility function will generate uids for agents in increasing numerical order, padded with leading zeros."""
    global id_counter

    id_counter += 1
    return str(id_counter).zfill(6)


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
        self.preparation: list[PreparationInterface] = []
        """
        Preparation step classes to execute
        """
        self.simulation_day_hook_pre: list[SimulationDayHookInterface] = []
        """simulation hook classes that are run on whole data at the start of the day"""
        self.simulation_day_hook_post: list[SimulationDayHookInterface] = []
        """simulation hook classes that are run on whole data at the end of the day"""
        self.simulation_define_state: list[SimulationDefineStateInterface] = []
        """simulation hook classes that are executed on each agent at each node"""
        self.simulation_step_hook: list[SimulationStepHookInterface] = []

        self.simulation_step: list[SimulationStepInterface] = []
        """
        Simulation step classes to execute
        """
        self.output: list[OutputInterface] = []
        """
        Output step classes to execute
        """
        self.overnight_trace_back: bool = True
        """Trace back to last hub with overnight stay"""
        self.simulation_starts: list[str] | None = None
        """"Starting hubs for simulation"""
        self.simulation_ends: list[str] | None = None
        """"End hubs for simulation"""
        self.simulation_route: str | None = None
        """Route key for simulation - should be lowercase"""
        self.simulation_route_reverse: bool = False
        """Is route reversed?"""
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

    def get_agent_date(self, agent: Agent, additional_offset: float = 0.) -> dt.datetime:
        # get start date as datetime object
        current_date: dt.datetime = (dt.datetime.combine(self.start_date, dt.datetime.min.time()))
        # TODO: check DST changes - we must not have these!

        # calculate current day and time
        current_date += dt.timedelta(hours=agent.current_time)

        # add additional offset
        current_date += dt.timedelta(hours=additional_offset)

        return current_date

########################################################################################################################
# Context
########################################################################################################################


class Context(object):
    """The context object is a read-only container for simulation threads."""

    def __init__(self):
        self.graph: ig.Graph | None = None
        """Full (multi-)graph data for roads, rivers and other paths (undirected)"""
        self.routes: ig.Graph | None = None
        """
        Path to be traversed from start to end - it is a directed version of the graph above. Used by the simulation to
        find the correct route. It is a multidigraph containing possible routes.
        """
        self.space_time_data: dict[str, SpatioTemporalInterface] = {}

    def get_path_by_id(self, path_id: str) -> ig.Edge | None:
        """Get path by id"""
        if self.graph:
            return self.routes.es.find(name=path_id)
        return None

    def get_hub_by_id(self, hub_id) -> ig.Vertex | None:
        """Get hub by id"""
        if self.graph:
            return self.routes.vs.find(name=hub_id)
        return None


########################################################################################################################
# Agent and State
########################################################################################################################

class State(object):
    """State class - this will take information on the current state of a simulation agent, it will be reset each step"""

    def __init__(self):
        self.uid: str = generate_nanoid()
        """unique id"""

        self.time_taken: float = 0.
        """Time taken in this step"""
        self.time_for_legs: list[float] = []
        """Time taken for all legs of this step"""
        self.data_for_legs: list[dict[str, any]] = []
        """Environmental data for each leg"""
        self.signal_stop_here: bool = False
        """Signal forced stop here"""
        self.last_coordinate_after_stop: tuple[float, float] | None = None
        """Saves last coordinate after stop - for logging purposes"""
        self.is_reversed: bool = False
        """True, if route is to be traversed in reverse order"""


    def reset(self) -> State:
        """Prepare state for new step"""
        self.time_taken = 0.
        self.time_for_legs = []
        self.data_for_legs = []
        self.signal_stop_here = False
        self.last_coordinate_after_stop = None
        self.is_reversed: bool = False

        return self

    def __repr__(self) -> str:
        return f'State {self.uid} TT={self.time_taken:.2f} STOP_HERE={self.signal_stop_here}'

    #
    # def hash(self) -> str:
    #     """Return unique id of this state"""
    #     return ''


class Agent(object):
    """Agent - simulating single travelling entity at a specific time and date"""

    def __init__(self, this_hub: str, next_hub: str, route_key: str, state: State | None = None,
                 current_time: float = 0., max_time: float = 0.):
        self.uid: str = generate_id()
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
        """Key id of next/current route between hubs ("name" attribute of edge)"""
        self.last_route: str | None = None
        """Key if of last route taken"""
        self.parent: str | None = None
        """UIDs of parent agents"""

        self.current_time: float = current_time
        """Current time stamp of agent (each 24 is a day)"""
        self.max_time: float = max_time
        """Current maximum timestamp for this day"""
        self.start_time: float = current_time
        """Keep start time of today"""

        self.is_finished: bool = False
        """finished at this day"""
        self.is_cancelled: bool = False
        """cancelled at this day"""
        self.tries: int = 0
        """internal value for tries at this hub - will break at a defined number"""
        self.last_resting_place: str = this_hub
        """keep track of last resting place"""

        self.visited_hubs: set[str] = set()
        """keeps visited hubs (for all days)"""
        self.forced_route: list[str] = []
        """force route for this agent for next day"""
        self.route_data: ig.Graph = ig.Graph(directed=True)
        """keep complete route taken for all days, including resting places, etc."""

        # rest history
        self.rest_history: list[tuple[float, float, str]] = []
        """History of rests, each entry is (time, length in hours)"""

        # keeps any additional data
        self.additional_data: dict[str, any] = {}

    def prepare_for_new_day(self, current_day: int = 1, current_time: float = 8., max_time: float = 16.):
        """
        reset to defaults for a day

        :param current_day: current day to set
        :param current_time:
        :param max_time:
        :return:
        """
        # set values for new day
        self.current_time = (current_day-1) * 24 + current_time
        self.start_time = current_time
        self.max_time = (current_day-1) * 24 + max_time
        # self.rest_history = [] # keep
        self.additional_data = {}
        self.state = self.state.reset()

    def __repr__(self) -> str:
        if self.is_finished:
            return f'Agent {self.uid} ({self.this_hub}) - [finished {self.is_finished}, {self.current_time:.2f}]'
        if self.is_cancelled:
            return f'Agent {self.uid} ({self.this_hub}->{self.next_hub} [{self.route_key}]) - [cancelled {self.is_cancelled}, {self.current_time:.2f}]'
        return f'Agent {self.uid} ({self.this_hub}->{self.next_hub} [{self.route_key}]) [{self.current_time:.2f}/{self.max_time:.2f}]'

    def __eq__(self, other) -> bool:
        return self.this_hub == other.this_hub and self.next_hub == other.next_hub and self.route_key == other.route_key

    # def hash(self) -> str:
    #     return self.this_hub + self.next_hub + str(self.route_key) + "_" + str(self.current_time)

    def generate_uid(self) -> str:
        """generate an unique id of agent"""
        self.uid = generate_id()
        return self.uid

    def set_hub_departure(self, hub: str, departure: float):
        try:
            hub = self.route_data.vs.find(name=hub)
            hub['departure'] = departure
        except:
            self.route_data.add_vertex(name=hub, arrival=None, departure=departure, overnight=False)

    def set_hub_arrival(self, hub: str, arrival: float):
        try:
            hub = self.route_data.vs.find(name=hub)
            hub['arrival'] = arrival
        except:
            self.route_data.add_vertex(name=hub, arrival=arrival, departure=None, overnight=False)

    def create_route_data(self, from_hub: str, to_hub: str, route_key: str, departure: float, arrival: float, leg_times: list[float]):
        # create departure and arrival hubs
        self.set_hub_departure(from_hub, departure)
        self.set_hub_arrival(to_hub, arrival)

        # create list of time points in route
        t = departure
        times = [t]

        for leg_time in leg_times:
            t += leg_time
            times.append(t)

        attr = {'name': route_key, 'times': times}
        # additional state infos
        # if self.state.is_reversed:
        #     attr['is_reversed'] = True
        if self.state.last_coordinate_after_stop is not None:
            attr['last_coordinates'] = self.state.last_coordinate_after_stop

        self.route_data.add_edge(from_hub, to_hub, **attr)

    def add_rest(self, length: float, time: float = -1, reason: str = 'resting') -> None:
        """
        Add rest event to history
        :param length: length of rest in hours
        :param time: time point (hour/minute) - if not set or below 0, use current time of agent
        :param reason: reason for rest
        """
        if time < 0:
            time = self.current_time

        self.rest_history.append((time, length, reason))

    def get_longest_rest_time_within(self, current_time: float, length: float) -> float | None:
        """
        Return longest rest time within given time and length
        :param current_time: current time (hour/minute)
        :param length: length in hours to check back
        :return: longest rest time within given time and length in hours
        """
        # calculate start time
        start_time = current_time - length

        min_time: float | None = None

        # now go back the rest history
        for ts in self.get_rest_times_within(start_time):
            if ts[0] < start_time:
                break
            if min_time is None or ts[1] > min_time:
                min_time = ts[1]

        return min_time

    def get_rest_times_within(self, start_time) -> Generator[tuple[float, float, str], None, None]:
        # go back the rest history
        for time, length, reason in reversed(self.rest_history):
            if time >= start_time:
                yield time, length, reason
            else:
                break

    def get_most_recent_rest_time(self) -> float | None:
        """
        Return the most recent rest time
        :return: most recent rest time in hours
        """
        if self.rest_history and len(self.rest_history) > 0:
            return self.rest_history[-1][0]
        else:
            return None

    def get_rest_times_from_to(self, start_time: float, end_time: float, sort_by_length: bool = False) -> list[tuple[float, float, str]]:
        fitting_rest_times = []

        for rest in self.rest_history:
            if start_time <= rest[0] and rest[0]+rest[1] <= end_time:
                fitting_rest_times.append(rest)

        # sort be length (longest rest first)
        if sort_by_length:
            fitting_rest_times.sort(key=lambda x: x[1], reverse=True)

        return fitting_rest_times


########################################################################################################################
# Set of Results
########################################################################################################################

class SetOfResults:
    """Set of results represents the results of a simulation"""

    def __init__(self):
        self.min_dt: float = 0.
        """minimum departure time of agents (day, time)"""
        self.max_dt: float = 0.
        """maximum arrival time of agents (day, time)"""
        self.agents: ig.Graph = ig.Graph(directed=True)
        """general list of agents - as list of descend from starting hubs to ending ones"""

    def add_agent(self, agent: Agent) -> None:
        # add vertex
        self.agents.add_vertex(name=agent.uid, agent=agent)
        # add edge from parent to myself
        if agent.parent:
            self.agents.add_edge(agent.parent, agent.uid, name=agent.route_key)

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

class SimulationDayHookInterface(abc.ABC):
    """
    Simulation module interface for hooks at the start or the end of a day - expect to return a (new) list of agents
    """
    def __init__(self):
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent], current_day: int) -> list[Agent]:
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
        self.conditions: dict[str, any] = {}

    def check_conditions(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> bool:
        """Checks conditions for this step"""
        # skip set to true?
        if self.skip:
            return False

        # no conditions?
        if not self.conditions or len(self.conditions) == 0:
            return True

        # check conditions
        if 'types' in self.conditions and len(self.conditions['types']) > 0:
            # check type of route ahead
            if next_leg['type'] not in self.conditions['types']:
                return False

        if 'not_types' in self.conditions and len(self.conditions['not_types']) > 0:
            # check type of route ahead
            if next_leg['type'] in self.conditions['not_types']:
                return False

        return True

    @abc.abstractmethod
    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        """
        Run the simulation module - run at the start of each simulation step, should be used as preparation for the
        actual simulation. run_hooks must be called within this method.

        :param config: configuration (read-only)
        :param context: context (read-only)
        :param agent: current agent (contains state object)
        :param next_leg: next leg (Edge)
        :return: updated state object
        """
        pass

    @staticmethod
    def run_hooks(config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, coords: tuple,
                  time_offset: float) -> tuple[float, bool]:
        """
        Call hooks for a simulation step - this method has to be called in update_state in an appropriate position.

        :param config:
        :param context:
        :param agent:
        :param next_leg:
        :param coords:
        :param time_offset:
        :return: tuple of new time offset and whether the simulation was cancelled
        """
        for hook in config.simulation_step_hook:
            (time_offset, cancelled) = hook.run_hook(config, context, agent, next_leg, coords, time_offset)
            if cancelled:
                # update agent state
                agent.state.signal_stop_here = True
                return time_offset, True
        return time_offset, False


class SimulationStepHookInterface(abc.ABC):
    """
    Simulation step hook module interface - used for hooks called by simulation steps
    """
    # def __init__(self):
    #     # runtime settings
    #     self.skip: bool = False
    #     self.conditions: dict[str, any] = {}
    #
    # def check_conditions(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> bool:
    #     """Checks conditions for this step"""
    #     # skip set to true?
    #     if self.skip:
    #         return False
    #
    #     # no conditions?
    #     if not self.conditions or len(self.conditions) == 0:
    #         return True
    #
    #     # check conditions
    #     if 'types' in self.conditions and len(self.conditions['types']) > 0:
    #         # check type of route ahead
    #         if next_leg['type'] not in self.conditions['types']:
    #             return False
    #
    #     if 'not_types' in self.conditions and len(self.conditions['not_types']) > 0:
    #         # check type of route ahead
    #         if next_leg['type'] in self.conditions['not_types']:
    #             return False
    #
    #     return True

    @abc.abstractmethod
    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, coords: tuple, time_offset: float) -> tuple[float, bool]:
        """
        Run the hook - to be implemented by specific classes

        :param config:
        :param context:
        :param agent:
        :param next_leg:
        :param coords:
        :param time_offset:
        :return: tuple of new time offset and a boolean indicating if the day was cancelled
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
