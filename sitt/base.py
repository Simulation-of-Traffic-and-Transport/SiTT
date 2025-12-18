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

        self.break_simulation_after: int = 10
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


class Agent(object):
    """Agent - simulating single travelling entity at a specific time and date"""

    def __init__(self, this_hub: str, next_hub: str, route_key: str, state: State | None = None,
                 current_time: float = 0., max_time: float = 0., do_not_generate_uid: bool = False):
        self.uid: str = '' if do_not_generate_uid else generate_id()
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
        self.cancel_reason: str | None = None
        """reason for cancellation"""
        self.tries: int = 0
        """internal value for tries at this hub - will break at a defined number"""
        self.last_resting_place: str = this_hub
        """keep track of last resting place"""

        self.visited_hubs: set[str] = set()
        """keeps visited hubs"""
        self.forced_route: list[str] = []
        """keeps history of agent"""
        self.last_overnight_hub: str = this_hub
        """keeps last overnight hub (for overnight travel)"""
        self.route: list[str] = []
        """keeps ids of hubs and routes (even == route, odd == hub)"""
        self.route_times: dict[str, list[float]] = {}
        """keeps times for each route"""
        self.route_reversed: list[bool] = []
        """keeps information if route is reversed"""
        self.route_before_traceback: list[str] = []
        """keeps ids of hubs before traceback to last possible resting place"""
        self.route_reversed_before_traceback: list[bool] = []
        """keeps information if route is reversed before traceback to last possible resting place"""
        self.parents: list[str] = []

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
        self.additional_data = {}
        self.state = self.state.reset()
        self.last_overnight_hub: str = self.this_hub
        # self.rest_history = [] # keep
        # self.route = []
        # self.route_data = {}

    def __repr__(self) -> str:
        if self.is_finished:
            return f'Agent {self.uid} ({self.this_hub}) - [finished {self.is_finished}, {self.current_time:.2f}]'
        if self.is_cancelled:
            return f'Agent {self.uid} ({self.this_hub}->{self.next_hub} [{self.route_key}]) - [cancelled {self.is_cancelled}, {self.current_time:.2f}]'
        return f'Agent {self.uid} ({self.this_hub}->{self.next_hub} [{self.route_key}]) [{self.current_time:.2f}/{self.max_time:.2f}]'

    def __eq__(self, other) -> bool:
        return self.this_hub == other.this_hub and self.next_hub == other.next_hub and self.route_key == other.route_key

    def get_start_end(self) -> tuple[str, str, float, float]:
        min_dt = None
        max_dt = None
        start_hub = None
        end_hub = None

        # only if we have route times
        if len(self.route_times):
            # get route times for first and last routes
            min_dt = self.route_times[self.route[1]][0]
            max_dt = self.route_times[self.route[-2]][-1]
        else:
            min_dt = self.start_time
            max_dt = self.start_time

        # get start and end hubs
        if len(self.route) > 0:
            start_hub = self.route[0]
            end_hub = self.route[-1]

        return start_hub, end_hub, min_dt, max_dt

    def generate_uid(self) -> str:
        """generate an unique id of agent"""
        self.uid = generate_id()
        return self.uid

    def add_rest(self, length: float, time: float = -1, reason: str = 'rest') -> None:
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
            if ts[0] + ts[1] < start_time:
                break
            if min_time is None or ts[1] > min_time:
                min_time = ts[1]

        return min_time

    def get_rest_times_within(self, start_time) -> Generator[tuple[float, float, str], None, None]:
        # go back the rest history
        for time, length, reason in reversed(self.rest_history):
            if time + length >= start_time:
                yield time, length, reason
            else:
                continue

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

        return fitting_rest_times if len(fitting_rest_times) > 0 else []

    def create_route_data(self, from_hub: str, to_hub: str, route_key: str, departure: float, is_revered: bool = False) -> None:
        # check, if route is consistent
        if len(self.route) > 0:
            if self.route[-1] != from_hub:
                raise ValueError(f"Route from {self.route[-1]} to {from_hub} is inconsistent.")
        else: # first agent
            self.route.append(from_hub)

        # create route entries
        self.route.append(route_key)
        self.route.append(to_hub)
        # remember reversed
        self.route_reversed.append(is_revered)

        # create list of time points in route
        t = departure
        times = [t]

        # calculate actual time for legs (dt and not time taken)
        for leg_time in self.state.time_for_legs:
            t += leg_time
            times.append(t)

        # create route data entries
        self.route_times[route_key] = times

    def iterate_routes(self) -> Generator:
        for i, key in enumerate(self.route):
            # odd or even?
            if i % 2 == 0:
                arrival = None
                departure = None
                rest = None

                if i != 0:
                    # get arrival time
                    arrival = self.route_times[self.route[i-1]][-1]
                if i!= len(self.route) - 1:
                    # get departure time
                    departure = self.route_times[self.route[i+1]][0]

                # do we have a rest here?
                if arrival is not None and departure is not None:
                    rest = self.get_rest_times_from_to(arrival, departure)
                    if len(rest) > 0:
                        rest = rest[0]
                    else:
                        rest = None

                # this is a hub
                yield {'type': 'hub', 'uid': key, 'arrival': arrival, 'departure': departure, 'rest': rest, 'idx': i}
            else:
                times = self.route_times[key]

                # get rest times for this edge
                rest = self.get_rest_times_from_to(times[0], times[-1])
                if len(rest) == 0:
                    rest = None

                # this is an edge
                yield {'type': 'edge', 'uid': key, 'legs': times, 'rest': rest, 'idx': i, 'reversed': self.route_reversed[int((i-1)/2)]}

########################################################################################################################
# Set of Results
########################################################################################################################

class SetOfResults:
    """Set of results represents the results of a simulation"""

    def __init__(self):
        self.agents: list[Agent] = []
        """general list of agents"""

    def add_agent(self, agent: Agent) -> None:
        # add vertex
        self.agents.append(agent)

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
    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent], results: SetOfResults, current_day: int) -> list[Agent]:
        pass

    @abc.abstractmethod
    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context, current_day: int) -> None:
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
