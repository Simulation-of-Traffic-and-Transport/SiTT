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
from .spatio_temporal_data import SpatioTemporalInterface, XArrayNetCDFData

__all__ = [
    "SkipStep",
    "Configuration",
    "Context",
    "State",
    "SpatioTemporalInterface",
    "XArrayNetCDFData",
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
    """
    Generates a secure, URL-friendly unique identifier string using the nanoid library.

    This function creates a random identifier string that consists of uppercase letters,
    lowercase letters, and digits. The length of the generated identifier is always 12
    characters.

    :return: A 12-character long alphanumeric string.
    :rtype: str
    """
    return nanoid.generate('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 12)


def generate_id() -> str:
    """
    Generate an increasing zero-padded unique identifier string.

    This function increments a global counter and generates a zero-padded identifier
    string of the counter's value. The ID is always six characters long, padded with
    leading zeros as needed.

    :return: A uniquely generated identifier string based on the incremented global
        counter.
    :rtype: str
    """
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
        self.means_of_transport: list[str] = []
        """
        Types of transport used in the simulation - multiplies the number of vehicles in the simulation and tags them
        Can be used in the steps to define the types of vehicles. If empty, all agents will be of an empty type (None).
        """
        self.overnight_trace_back: bool = True
        """Trace back to last hub with overnight stay"""
        self.keep_agent_data_in_results: bool = True
        """Keep agent data in set of results (might use quite a lot of memory, but will enable analysis of agent routes)"""
        self.keep_leg_times: bool = True
        """Keep leg times in agent data (uses up memory and if you do not need them, set to false)"""
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
        """
        Sets an attribute on the object and observes changes in logger settings when specific
        attributes are modified. If the 'verbose' attribute is set to a truthy value, the logging
        level is updated to INFO, enabling more detailed logging output. If the 'quiet' attribute
        is set to a truthy value, the logging level is set to ERROR, suppressing less important
        messages. Changes to other attributes are passed to the superclass for assignment.

        :param att: The name of the attribute being set.
        :type att: str
        :param value: The value to assign to the attribute.
        :return: None
        """
        # observe changes in logger settings
        if att == 'verbose' and value:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
        if att == 'quiet' and value:
            logger = logging.getLogger()
            logger.setLevel(logging.ERROR)
        return super().__setattr__(att, value)

    def __repr__(self):
        """
        Represents the string representation of an object in YAML format.

        This method serializes the object into a YAML string representation using ``yaml.dump``.
        It is useful for creating a human-readable, serialized version of the object.

        :return: A string containing the YAML serialized representation of the object.
        :rtype: str
        """
        return yaml.dump(self)

    def __getstate__(self):
        """
        Gets the state of the object for serialization, ensuring non-serializable
        attributes and unnecessary data are excluded.

        This method creates a copy of the object's current state and modifies it
        by removing or transforming attributes that cannot or should not be
        serialized.

        :return: A dictionary representing the serializable state of the object.
        :rtype: dict
        """
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
        """
        Calculate the current date and time for a given agent, based on a start date and
        offset values.

        This method computes the agent's current datetime by combining the `start_date`
        with the agent's current time in hours. Additionally, an optional offset in hours
        can be applied to this computation. This function does not account for DST (Daylight
        Saving Time) changes, as noted in the TODO comment (reminder).

        :param agent: An Agent instance that includes the current time in hours as a
            float. This value specifies the agent's position in the simulation relative
            to the start date.
        :param additional_offset: A float representing an extra offset in hours to be
            added to the computed datetime. Defaults to 0.
        :return: A datetime object representing the agent's current datetime, calculated
            using the `start_date`, the agent's current time, and the additional offset.
        :rtype: datetime.datetime
        """
        # get start date as datetime object
        current_date: dt.datetime = (dt.datetime.combine(self.start_date, dt.datetime.min.time()))
        # TODO: check DST changes - we must not have these!
        # DST has been checked in other modules, so this should not be a problem, but we might have to double-check
        # this some time, so the TODO remains here as a reminder.

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
        """
            Represents a data structure to manage and process graphical data for paths and routes.

            Attributes
            ----------
            graph : ig.Graph | None
                Full (multi-)graph data for roads, rivers and other paths (undirected).

            routes : ig.Graph | None
                Path to be traversed from start to end - it is a directed version of the graph. Used by the simulation to
                find the correct route. It is a multidigraph containing possible routes.

            space_time_data : dict[str, SpatioTemporalInterface]
                Spatio-temporal data associated with the graph and routes.
        """
        self.graph: ig.Graph | None = None
        """Full (multi-)graph data for roads, rivers and other paths (undirected)"""
        self.routes: ig.Graph | None = None
        """
        Path to be traversed from start to end - it is a directed version of the graph above. Used by the simulation to
        find the correct route. It is a multidigraph containing possible routes.
        """
        self.space_time_data: dict[str, SpatioTemporalInterface] = {}
        """
        Spatio-temporal data associated with the graph and routes. This serves as a buffer for lookups.
        """

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

    def find_space_time_data(self, lat: float, lon: float, date: dt.datetime, field: str):
        """Finds a specific data field from spatio-temporal data sources.

        This method iterates through all available spatio-temporal data sources
        in the context and attempts to retrieve the value for a specific field
        at a given geographic coordinate and time.

        Args:
            lat (float): The latitude for the data lookup.
            lon (float): The longitude for the data lookup.
            date (dt.datetime): The date and time for the data lookup.
            field (str): The name of the data field to find (e.g., 'wind_speed').

        Returns:
            The value of the requested field if found, otherwise None. The type
            of the return value depends on the data source.
        """
        for data in self.space_time_data.values():
            values = data.get(lat, lon, date)
            if field in values:
                return values[field]
        return None

    def find_multiple_space_time_data(self, lat: float, lon: float, date: dt.datetime, *field: str):
        """Finds multiple data fields from spatio-temporal data sources.

        This method iterates through all available spatio-temporal data sources
        in the context and attempts to retrieve the values for multiple specified
        fields at a given geographic coordinate and time.

        Args:
            lat (float): The latitude for the data lookup.
            lon (float): The longitude for the data lookup.
            date (dt.datetime): The date and time for the data lookup.
            *field (str): A variable number of strings, where each string is the
                name of the data field to find (e.g., 'wind_speed', 'temperature').

        Returns:
            dict: A dictionary where keys are the requested field names and values
            are the corresponding data values found. If a field is found in
            multiple data sources, its value will be overwritten by the last
            one found.
        """
        values = {}
        for data in self.space_time_data.values():
            v = data.get(lat, lon, date)
            for f in field:
                values[f] = v[f]
        return values

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
        """Prepare state for a new step"""
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
        self.cancel_details: str | None = None
        """details on cancellation"""
        self.tries: int = 0
        """internal value for tries at this hub - will break at a defined number"""
        self.last_resting_place: str = this_hub
        """keep track of last resting place"""

        self.visited_hubs: set[str] = set()
        """keeps visited hubs"""
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

        # transport type
        self.transport_type: str | None = None
        """this is used to differentiate between different types of agents, e.g. foot, cart_donkey, cart_oxen"""
        self.transport_types: list[str] = []
        """keeps track of transport types for each agent for each leg, e.g. foot, cart_donkey, cart_oxen"""

        # keeps any additional data
        self.additional_data: dict[str, any] = {}
        """additional data for the agent, keys are arbitrary strings, values are any type"""

    def prepare_for_new_day(self, current_day: int = 1, current_time: float = 8., max_time: float = 16.):
        """
        Prepare the system for a new day by setting the time, resetting the state, and initializing
        necessary attributes for tracking the day's operations.

        This method calculates the start of the new day based on the provided parameters
        and resets certain states as part of the preparation process.

        :param current_day: The day number to be set, with 1 representing the first day.
        :param current_time: The starting time for the current day in hours (default is 8.0).
        :param max_time: The maximum allowed time for operations during the current day in hours (default is 16.0).
        :return: None
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
        sig = '' if self.transport_type is None or self.transport_type == '' else f' ({self.transport_type})'
        if self.is_finished:
            return f'Agent {self.uid}{sig} ({self.this_hub}) - [finished {self.is_finished}, {self.current_time:.2f}]'
        if self.is_cancelled:
            return f'Agent {self.uid}{sig} ({self.this_hub}->{self.next_hub} [{self.route_key}]) - [cancelled {self.is_cancelled}, {self.current_time:.2f}]'
        return f'Agent {self.uid}{sig} ({self.this_hub}->{self.next_hub} [{self.route_key}]) [{self.current_time:.2f}/{self.max_time:.2f}]'

    def __eq__(self, other) -> bool:
        return self.this_hub == other.this_hub and self.next_hub == other.next_hub and self.route_key == other.route_key

    def get_start_end(self) -> tuple[str, str, float, float]:
        """
        Get the start and end hubs along with their corresponding timestamps.

        This method retrieves the first and last hubs from the agent's route, as well as
        the departure time from the start hub and the arrival time at the end hub. If route
        times are available, it uses the actual recorded times; otherwise, it falls back to
        the agent's start time.

        :return: A tuple containing:
            - start_hub (str): The identifier of the starting hub, or None if no route exists
            - end_hub (str): The identifier of the ending hub, or None if no route exists
            - min_dt (float): The departure time from the start hub (in hours)
            - max_dt (float): The arrival time at the end hub (in hours)
        :rtype: tuple[str, str, float, float]
        """
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
        """
        Generate a sequence of rest periods from the rest history that overlap or occur
        after the given start time.

        This method iterates over the rest history in reverse chronological order and
        yields rest periods that either overlap the `start_time` or started after it.

        :param start_time: The starting time (float) from which to filter rest periods.
        :return: A generator yielding tuples of the form (time, length, reason), where
            `time` is the starting time of the rest period, `length` is its duration,
            and `reason` is a string explaining the reason for the rest.
        :rtype: Generator[tuple[float, float, str], None, None]
        """
        # go back the rest history
        for time, length, reason in reversed(self.rest_history):
            if time + length >= start_time:
                yield time, length, reason
            else:
                continue

    def get_most_recent_rest_time(self) -> float | None:
        """
        Retrieve the most recent rest time from the rest history.

        This method evaluates if there is any available rest history data and,
        if so, returns the most recent rest time recorded. If no rest history
        exists, it returns None.

        :return: The most recent rest time or None if no rest history exists.
        :rtype: float | None
        """
        if self.rest_history and len(self.rest_history) > 0:
            return self.rest_history[-1][0]
        else:
            return None

    def get_rest_times_from_to(self, start_time: float, end_time: float, sort_by_length: bool = False) -> list[tuple[float, float, str]]:
        """
        Extracts and filters rest time intervals within a specified time range.

        This method retrieves rest time intervals from the `rest_history` attribute
        that start and end within the given `start_time` and `end_time` range. The
        option to sort the resulting list by the duration of the rest periods is available.

        :param start_time: The starting point of the time range to filter rest intervals.
        :param end_time: The ending point of the time range to filter rest intervals.
        :param sort_by_length: Whether to sort the returned list by the duration of
            the rest intervals in descending order. Defaults to False.
        :return: A list of tuples, where each tuple represents a rest time interval
            with starting time (float), length (float), and an associated description (str).
        """
        fitting_rest_times = []

        for rest in self.rest_history:
            if start_time <= rest[0] and rest[0]+rest[1] <= end_time:
                fitting_rest_times.append(rest)

        # sort be length (longest rest first)
        if sort_by_length:
            fitting_rest_times.sort(key=lambda x: x[1], reverse=True)

        return fitting_rest_times if len(fitting_rest_times) > 0 else []

    def create_route_data(self, from_hub: str, to_hub: str, route_key: str, departure: float, is_revered: bool = False) -> None:
        """
        Creates data for a route by validating its consistency, appending route details, and calculating
        time points for the route legs.

        :param from_hub: The starting point of the route (hub name or identifier).
        :type from_hub: str
        :param to_hub: The destination point of the route (hub name or identifier).
        :type to_hub: str
        :param route_key: A unique key or identifier for the route.
        :type route_key: str
        :param departure: The departure time from the starting hub.
        :type departure: float
        :param is_revered: Indicates whether the route is reversed (optional, defaults to False).
        :type is_revered: bool
        :return: None
        """
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
        # remember the transport type
        self.transport_types.append(self.transport_type)

        # create route data entries
        self.route_times[route_key] = [departure]

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
        """
        Iterates through and yields details of routes, providing information about
        hubs and edges. The method determines if the current item in the route is
        a hub or an edge and yields corresponding data.

        :return: A generator yielding dictionaries containing details about hubs and edges
        :rtype: Generator[Dict[str, Any], None, None]

        Dictionaries yielded by the generator may include:
          - For hubs:
              - type (str): Type of the item, will always be 'hub'.
              - uid (str): Unique identifier of the hub.
              - arrival (Optional[Any]): Arrival time to the hub, or None if not applicable.
              - departure (Optional[Any]): Departure time from the hub, or None if not applicable.
              - rest (Optional[Any]): The rest time at the hub, or None if no rest time exists.
              - idx (int): Index of the hub in the route.

          - For edges:
              - type (str): Type of the item, will always be 'edge'.
              - uid (str): Unique identifier of the edge.
              - legs (List[Any]): A list of times corresponding to the legs of the edge.
              - rest (Optional[Any]): The rest time for the edge, or None if no rest time exists.
              - idx (int): Index of the edge in the route.
              - reversed (bool): Indicates whether the edge is reversed.
        """
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
        """
        Adds an agent to the agents list.

        This method takes an agent object and appends it to the list of agents
        contained within the instance. This operation effectively represents
        adding a vertex to the graph data structure.

        :param agent: The agent object to be added to the agents list.
        :type agent: Agent

        :return: None
        :rtype: None
        """
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
        """
        Initialize the runtime settings for the object.

        This constructor sets up default values for attributes that control runtime
        behavior and conditions.

        Attributes
        ----------
        skip : bool
            A flag indicating whether to skip the current operation. Defaults to False.
        conditions : list[str]
            A list of conditions that may influence runtime decisions. Defaults to an
            empty list.
        """
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def run(self, config: Configuration, context: Context) -> Context:
        """
        Execute the primary logic of the class implementation. This method is an
        abstract method and must be implemented by a concrete subclass. The method
        receives a configuration and an execution context, performs its operation,
        and returns an updated context.

        :param config: The configuration instance containing the required settings
            for executing the method.
        :type config: Configuration
        :param context: The context instance that provides information about the
            execution state and is updated during the execution of this method.
        :type context: Context
        :return: An updated Context instance reflecting the changes made during
            method execution.
        :rtype: Context
        """
        pass

class SimulationDayHookInterface(abc.ABC):
    """
    Simulation module interface for hooks at the start or the end of a day - expect to return a (new) list of agents
    """
    def __init__(self):
        """
        Initializes the class. This constructor method sets up the runtime settings
        that determine the behavior of the instance.

        :ivar skip: A flag indicating whether to skip the current execution. Defaults to False.
        :type skip: bool
        :ivar conditions: A list of conditions to be applied during execution. Defaults to an
            empty list of strings.
        :type conditions: list[str]
        """
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent], results: SetOfResults, current_day: int) -> list[Agent]:
        """
        Executes the main logic for processing agents, their statuses, and results within the given execution
        context and configuration. This method is abstract and must be implemented by subclasses. It should
        handle the agents' actions for the specified day, track finished agents, and update results based
        on their progress.

        :param config: Configuration object containing all necessary simulation or execution settings.
        :param context: Context object providing shared state and environment for the execution.
        :param agents: List of Agent objects representing entities participating in the execution.
        :param agents_finished_for_today: List of Agent objects that have completed their actions for the
            current day. This list is updated during execution.
        :param results: SetOfResults object used to collect or store outcomes generated by the agents or execution.
        :param current_day: Integer representing the current day of the execution being processed.

        :return: List of Agent objects representing the state of agents, potentially with updated statuses
            or modifications, after processing for the current day has been completed.
        """
        pass

    @abc.abstractmethod
    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context, current_day: int) -> None:
        """
        Summarizes the simulation results and finalizes the simulation process.

        This method is declared as an abstract method and must be implemented by
        any subclass. Its purpose is to process the results of the simulation
        based on the provided configuration, context, and simulation day. It
        handles any post-simulation tasks required to finalize the simulation.

        :param results: A collection of results from the simulation. The structure
            or format of the results depends on the specific implementation.
        :param config: The configuration object that provides settings or parameters
            used to control the simulation behavior.
        :param context: The current state or context in which the simulation is
            being executed. This may include environment details, state variables,
            or auxiliary information for finalization tasks.
        :param current_day: The current day in the simulation timeline or the day
            on which the simulation concludes, represented as an integer.
        :return: None
        """
        pass


class SimulationDefineStateInterface(abc.ABC):
    """
    Simulation module interface for hooks defining the state of an agent at each node
    """

    def __init__(self):
        """
        Represents a class responsible for managing runtime settings, which includes the ability
        to toggle skipping behavior and configure a list of conditions.

        Attributes:
            skip (bool): A flag indicating whether certain operations should be skipped.
            conditions (list[str]): A list of conditions used for runtime configuration.
        """
        # runtime settings
        self.skip: bool = False
        self.conditions: list[str] = []

    @abc.abstractmethod
    def define_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        """
        Defines the state of the system based on the provided configuration, execution context,
        and agent details. This method is abstract and must be implemented in a subclass.

        :param config: Configuration settings required to define the state.
        :type config: Configuration
        :param context: Contextual information about the system's execution environment.
        :type context: Context
        :param agent: The agent responsible for interacting with the system.
        :type agent: Agent
        :return: The state of the system determined based on the input parameters.
        :rtype: State
        """
        pass


class SimulationStepInterface(abc.ABC):
    """
    Simulation step module interface - core of interface defining state
    """

    def __init__(self):
        """
        Initializes an instance of the class with default runtime settings.

        :ivar skip: A boolean flag indicating whether to skip certain operations.
        :ivar conditions: A dictionary representing conditions used during runtime.
        :ivar cancel: A dictionary representing cancellation settings during runtime.
        """
        # runtime settings
        self.skip: bool = False
        self.conditions: dict[str, any] = {}
        self.cancel: dict[str, any] = {}

    def check_conditions(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> bool:
        """
        Evaluates specified conditions to determine whether certain criteria are met.

        This method checks whether the conditions specified for the given context,
        configuration, agent, and next leg are satisfied. If the `skip` attribute
        is set to True, the method automatically returns False without evaluating
        any conditions.

        :param config: Configuration object containing settings and parameters for
            evaluating conditions.
        :type config: Configuration
        :param context: Context object that provides necessary contextual
            information for evaluating conditions.
        :type context: Context
        :param agent: Agent performing actions or involved in the current
            context of evaluation.
        :type agent: Agent
        :param next_leg: The next edge or step in the graph or sequence of
            operations being evaluated.
        :type next_leg: ig.Edge
        :return: Returns True if all conditions are satisfied, or False otherwise.
            If the `skip` attribute is set to True, the method returns False immediately.
        :rtype: bool
        """
        # skip set to true?
        if self.skip:
            return False

        return self._check_conditions(self.conditions, config, context, agent, next_leg)

    def check_cancel(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> bool:
        """
        Check if the cancel conditions are met for a given agent and next leg in the context.

        This method evaluates the specified cancellation conditions against the provided configuration,
        context, agent state, and the leg of the journey being considered. The result indicates whether
        the cancel conditions are not met.

        :param config: Configuration settings providing necessary parameters for the evaluation.
        :type config: Configuration
        :param context: Operational context in which the check is performed.
        :type context: Context
        :param agent: Agent object representing the entity being evaluated.
        :type agent: Agent
        :param next_leg: Edge object representing the next leg of the journey to consider.
        :type next_leg: ig.Edge
        :return: A boolean value indicating whether the cancel conditions are not met.
        :rtype: bool
        """
        return not self._check_conditions(self.cancel, config, context, agent, next_leg)

    @staticmethod
    def _check_conditions(conditions, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> bool:
        """
        Evaluates a set of conditions against the provided parameters, such as route type, agent transport
        type, and attributes of the next edge. The function returns `True` if all specified conditions are
        satisfied, and `False` otherwise. It is typically used to validate whether the agent can proceed
        along the specified path based on the given constraints.

        :param conditions: A dictionary containing multiple condition categories to evaluate.
            Supported condition keys include:
                - 'types': List of valid route types that the `next_leg['type']` must match.
                - 'not_types': List of route types that the `next_leg['type']` must not match.
                - 'transport_types': List of valid transport types for the agent.
                - 'not_transport_types': List of invalid transport types for the agent.
                - 'additional_data': Dictionary where keys represent additional agent attributes,
                  and values are lists of allowable values for the corresponding attributes.
                - 'edge_data': Dictionary where keys represent edge attribute names, and values are lists
                  of disallowed values for those attributes in the `next_leg`.

        :param config: Configuration object used for routing validation within the context.
            The specific influence this parameter has on the validation can vary depending on how
            conditions are structured and verified.

        :param context: Contextual information that might help determine global/system-level
            conditions or rules to evaluate. Provides valuable state information that complements
            condition-checking.

        :param agent: Instance of the Agent class, which includes properties like its
            transport type and additional metadata (e.g., custom or user-defined properties
            in `additional_data`).

        :param next_leg: The next edge/segment in the route graph being evaluated. This includes its
            attributes and type, influencing how conditions are matched or violated.

        :return bool: Returns `True` if all conditions are satisfied, `False` otherwise.
        """
        # no conditions?
        if not conditions or len(conditions) == 0:
            return True

        # check conditions
        if 'types' in conditions and len(conditions['types']) > 0:
            # check the type of route ahead
            if next_leg['type'] not in conditions['types']:
                return False

        if 'not_types' in conditions and len(conditions['not_types']) > 0:
            # check the type of route ahead (not)
            if next_leg['type'] in conditions['not_types']:
                return False

        if 'transport_types' in conditions and len(conditions['transport_types']) > 0:
            # check the transport type of the agent
            if agent.transport_type not in conditions['transport_types']:
                return False

        if 'not_transport_types' in conditions and len(conditions['not_transport_types']) > 0:
            # check the transport type of the agent (not)
            if agent.transport_type in conditions['not_transport_types']:
                return False

        if 'additional_data' in conditions and len(conditions['additional_data']) > 0:
            for key, values in conditions['additional_data'].items():
                if key not in agent.additional_data or agent.additional_data[key] not in values:
                    return False

        if 'edge_data' in conditions and len(conditions['edge_data']) > 0:
            attrs = next_leg.attributes()
            for key, values in conditions['edge_data'].items():
                if key in attrs and attrs[key] in values:
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
    def run_hooks(config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple,
                  time_offset: float) -> tuple[float, bool]:
        """
        Call hooks for a simulation step - this method has to be called in update_state in an appropriate position.

        :param config: configuration data
        :param context: context data
        :param agent: current agent data
        :param next_leg: next leg data
        :param i: current simulation step number (coordinate in leg)
        :param coords: current coordinate in leg
        :param time_offset: current time offset
        :return: tuple of new time offset and whether the simulation was canceled
        """
        for hook in config.simulation_step_hook:
            (time_offset, done, cancelled) = hook.run_hook(config, context, agent, next_leg, i, coords, time_offset)
            if done:
                # done indicates that we do not process more hooks
                break
            if cancelled:
                # update agent state
                agent.state.signal_stop_here = True
                return time_offset, True
        return time_offset, False


class SimulationStepHookInterface(abc.ABC):
    """
    Simulation step hook module interface - used for hooks called by simulation steps
    """
    def __init__(self):
        # runtime settings
        self.skip: dict | None = None

    @abc.abstractmethod
    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple, time_offset: float) -> tuple[float, bool, bool]:
        """
        Run the hook - to be implemented by specific classes

        :param config: configuration data
        :param context: context data
        :param agent: current agent data
        :param next_leg: next leg data
        :param i: current simulation step number (coordinate in leg)
        :param coords: current coordinate in leg
        :param time_offset: current time offset
        :return: tuple of new time offset and two boolean: first one indicates "done" (stop processing more hooks), the second one indicates "canceled" (stop agent for now)
        """
        pass

    def do_skip(self, agent: Agent, next_leg: ig.Edge):
        """
        Checks if the current hook should be skipped based on agent properties.

        This method evaluates the `self.skip` conditions, which can be configured
        to bypass the hook for certain agent transport types or additional data attributes.

        Args:
            agent (Agent): The agent currently being processed.
            next_leg (ig.Edge): The next leg of the journey.

        Returns:
            bool: True if the hook should be skipped, False otherwise.
        """
        # check skip conditions
        if self.skip and len(self.skip) > 0:
            if 'types' in self.skip and len(self.skip['types']) > 0:
                if next_leg['type'] in self.skip['types']:
                    return True

            if 'transport_types' in self.skip and len(self.skip['transport_types']) > 0:
                 if agent.transport_type in self.skip['transport_types']:
                    return True

            # additional data check - e.g. agent has a specific additional data type set
            if 'additional_data' in self.skip and len(self.skip['additional_data']) > 0:
                for key, values in self.skip['additional_data'].items():
                    if key in agent.additional_data and agent.additional_data[key] in values:
                        return True
        return False

class OutputInterface(abc.ABC):
    """
    Output module interface
    """

    def __init__(self):
        """
        Initializes an instance of the class with runtime settings.

        Attributes
        ----------
        skip : bool
            A flag to indicate if certain operations should be skipped.
        conditions : list[str]
            A list of condition strings applicable to runtime logic.
        """
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
