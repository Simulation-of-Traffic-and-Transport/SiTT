# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create basic json output"""
import json
import logging
from typing import Any

import igraph as ig
import numpy as np
from shapely.geometry import mapping

from sitt import Agent, Configuration, Context, OutputInterface, SetOfResults, is_truthy

logger = logging.getLogger()


class JSONOutput(OutputInterface):
    """Formats raw departure/arrival data into a structured list of dictionaries.

    This method groups events by time, converts the time to total hours,
    and aggregates agent IDs and reasons for each time point.

    Args:
        data (list[tuple[tuple[int, float], str, str]]): A list of event tuples.
            Each tuple contains a time tuple (day, hour), an agent UID, and a reason string.
        add_reasons (bool, optional): If True, includes the reasons for the event
            in the output. Defaults to True.

    Returns:
        list[dict]: A sorted list of dictionaries, where each dictionary represents
            a time point and contains the time in total hours ('t'), a list of
            agent UIDs ('agents'), and optionally a list of reasons ('reasons').
    """

    def __init__(self, to_string: bool = True, show_output: bool = False, save_output: bool = False,
                 filename: str = 'simulation_output.json', indent: int = 0):
        """Initializes the JSONOutput instance.

        This constructor sets up the configuration for the JSON output module,
        determining how the simulation results will be formatted and delivered.

        Args:
            to_string (bool, optional): If True, the output is returned as a
                JSON formatted string. Defaults to True.
            show_output (bool, optional): If True, the generated JSON is logged
                to the console. Defaults to False.
            save_output (bool, optional): If True, the generated JSON is saved
                to a file. Defaults to False.
            filename (str, optional): The name of the file to save the JSON
                output to. Can contain placeholders like `${ROUTE}`.
                Defaults to 'simulation_output.json'.
            indent (int, optional): The number of spaces to use for indentation
                in the JSON output. A value of 0 means no indentation.
                Defaults to 0.
        """
        super().__init__()
        self.to_string: bool = to_string
        """Convert data to string"""
        self.show_output: bool = show_output
        """Display output in logging"""
        self.save_output: bool = save_output
        """Save output to file?"""
        self.filename: str = filename
        """Filename for output file?"""
        self.indent: int | None = indent
        """Display JSON nicely (if > 0, indent by this number of spaces)?"""

        self.config: Configuration | None = None
        self.context: Context | None = None

    def run(self, config: Configuration, context: Context, set_of_results: SetOfResults) -> str:
        """Executes the JSON output generation process.

        This is the main method of the JSONOutput class. It orchestrates the
        conversion of simulation results into a JSON format. Based on the
        instance's configuration, it can return the JSON as a string, log it to
        the console, and/or save it to a file. The output filename can contain
        placeholders like `${ROUTE}` which will be replaced with values from the
        configuration.

        Args:
            config (Configuration): The simulation configuration object, used to
                access parameters like the simulation route name.
            context (Context): The simulation context object, containing shared
                simulation state.
            set_of_results (SetOfResults): The object containing the complete
                results of the simulation run.

        Returns:
            str: The generated JSON as a string if `to_string` is True, or an
                empty string if the output is skipped. If `to_string` is False,
                the raw dictionary is returned (note: this differs from the
                type hint).
        """
        if self.skip:
            return ''

        # replace some stuff in filename
        filename = self.filename
        filename = filename.replace('${ROUTE}', config.simulation_route)
        if config.start_date is not None:
            filename = filename.replace('${SIMULATION_START}', config.start_date.strftime('%Y-%m-%d'))

        logger.info(f"OutputInterface JSONOutput run: {filename}")

        self.config = config
        self.context = context

        # indent 0 is treated as no indent
        if self.indent == 0:
            self.indent = None

        # create dictionary from data using methods below
        result = self.create_dict_from_data(set_of_results)
        if self.to_string:
            result = json.dumps(result, indent=self.indent)
        if self.show_output:
            # always log at log level to show output
            logger.log(logger.level, result)

        if self.save_output:
            file = open(filename, 'w')

            # already converted to string?
            if self.to_string:
                file.write(result)
            else:
                file.write(json.dumps(result, indent=self.indent))

            file.close()

        return result

    def create_dict_from_data(self, set_of_results: SetOfResults) -> dict[str, Any]:
        """Creates a dictionary from the simulation results for JSON output.

        This method orchestrates the conversion of all relevant simulation data
        from a SetOfResults object into a single, structured dictionary. It
        gathers simulation metadata, processes agent data, and extracts node
        and path information from the final graph state. The resulting
        dictionary is the root object for the final JSON output.

        Args:
            set_of_results (SetOfResults): The object containing the complete
                results of the simulation run, including the agent graph and
                the final route graph.

        Returns:
            dict[str, Any]: A dictionary containing the structured simulation
                results, including metadata, agent journeys, and graph
                topology with traversal data.
        """
        agents, min_dt, max_dt = self._agent_graph_to_data(set_of_results.agents)

        time_slices = self._agents_to_time_slices(set_of_results.agents, min_dt, max_dt)

        # add nodes and paths
        nodes, paths = self._graph_to_data(set_of_results)

        # convert start date to stringq
        start_date = ""
        if self.config.start_date:
            start_date = str(self.config.start_date)[0:10]

        # create result dictionary
        return {
            "from": self.config.simulation_starts,
            "to": self.config.simulation_ends,
            "start": _dt_to_hours(set_of_results.min_dt),
            "end": _dt_to_hours(set_of_results.max_dt),
            "simulation_route": self.config.simulation_route,
            "simulation_route_reverse": self.config.simulation_route_reverse,
            "start_date": start_date,
            "break_simulation_after": self.config.break_simulation_after,
            "agents": agents,
            "nodes": nodes,
            "paths": paths,
            "time_slices": time_slices,
        }

    def _agent_graph_to_data(self, agents: ig.Graph) -> tuple[list[dict], float, float]:
        """Converts a graph of agents into a list of serializable dictionaries.

        This method iterates through each vertex in the provided agent graph,
        extracts the 'agent' object from the vertex attributes, and converts
        it into a dictionary format using the `_agent_to_data` method.

        Args:
            agents (ig.Graph): The graph where each vertex represents an agent
                and contains an 'agent' attribute holding the Agent object.

        Returns:
            list[dict]: A list of dictionaries, where each dictionary represents
                the data of a single agent.
        """
        list_of_agents: list[dict] = []
        min_dt = float('inf')
        max_dt = float('-inf')

        for v in agents.vs:
            agent = self._agent_to_data(v['agent'])
            list_of_agents.append(agent)
            if 'start' in agent and agent['start'] < min_dt:
                min_dt = agent['start']
            if 'end' in agent and agent['end'] > max_dt:
                max_dt = agent['end']

        list_of_agents = sorted(list_of_agents, key=lambda x: x['uid'])
        return list_of_agents, min_dt, max_dt

    def _agent_to_data(self, agent: Agent) -> dict:
        """Converts a single agent object into a serializable dictionary.

        This method extracts key information about an agent's journey and status
        and formats it into a dictionary for output.

        Args:
            agent (Agent): The agent object to process.

        Returns:
            dict: A dictionary containing the agent's data, including its UID,
                traversed hubs and edges, start and end times, and final status
                (finished or cancelled).
        """
        agent_data: dict[str, Any] = {
            "uid": agent.uid,
            "hubs": agent.route_data.vs['name'],
            "edges": agent.route_data.es['name'],
        }

        # parent?
        if agent.parent is not None:
            agent_data['parent'] = agent.parent

        # get first and last hub
        start_hub = agent.route_data.vs[0]
        end_hub = agent.route_data.vs[-1]
        if 'departure' in start_hub.attributes():
            agent_data['start'] = _dt_to_hours(start_hub['departure'])
        if 'arrival' in end_hub.attributes():
            agent_data['end'] = _dt_to_hours(end_hub['arrival'])

        if agent.is_cancelled:
            agent_data['cancelled'] = True
        if agent.is_finished:
            agent_data['finished'] = True

        return agent_data

    def _agents_to_time_slices(self, agents: ig.Graph, min_dt: float, max_dt: float) -> dict[float, list[dict[str, Any]]]:
        # initialize time slices dictionary
        time_data: dict[float, dict[tuple[float, float], list[str]]] = {}
        # we arrange the slices time 10, so we do not have floating point precision issues
        for t in np.arange(min_dt*10, max_dt*10+1):
            t = float(np.round(t/10, decimals=1))
            time_data[t] = {}

        # now, iterate through agents and add their journeys to time slices
        for v in agents.vs:
            time_slices_used: set[float] = set()
            agent = v['agent']
            # iterate through vertices and aggregate points
            for h in agent.route_data.vs:
                # get hub coordinates
                hub_geom = self.context.routes.vs.find(name=h['name'])['geom']
                coords = (hub_geom.x, hub_geom.y)
                for i, t in enumerate(_enumerate_arrival_departure(h['arrival'] if 'arrival' in h.attributes() else None, h['departure'] if 'departure' in h.attributes() else None)):
                    # create xy in time slice, if needed
                    if coords not in time_data[t]:
                        time_data[t][coords] = []
                    time_data[t][coords].append(v['name'])

            # iterate through edges and aggregate points
            for e in agent.route_data.es:
                # get edge coordinates
                edge_coords = self.context.routes.es.find(name=e['name'])['geom'].coords
                # iterate through times
                for i, t in enumerate(e['times']):
                    # skip first and last entry, because we add vertices here
                    if i == 0 or i == len(e['times']) - 1:
                        continue
                    t = _dt_to_hours(t)
                    # we only add the agent to one time slice
                    if t in time_slices_used:
                        continue
                    time_slices_used.add(t)
                    coord = edge_coords[i]
                    x = coord[0]
                    y = coord[1]
                    # create xy in time slice, if needed
                    if (x, y) not in time_data[t]:
                        time_data[t][(x, y)] = []
                    time_data[t][(x, y)].append(v['name'])

        time_slices = {}

        for t, coords in time_data.items():
            time_slices[t] = []
            for coord, agent_list in coords.items():
                agent_list.sort()
                time_slices[t].append({
                    'latLng': [coord[1], coord[0]],
                    'agents': agent_list,
                })

        return time_slices

    def _graph_to_data(self, set_of_results: SetOfResults) -> tuple[list[dict], list[dict]]:
        """Extracts and formats node and path data from the simulation results graph.

        This method iterates through the vertices (nodes) and edges (paths) of the
        route graph contained within the `set_of_results`. It converts graph-specific
        data types (like Shapely geometries) into JSON-serializable formats and
        structures the attributes for the final output.

        Args:
            set_of_results (SetOfResults): The object containing the simulation
                results, including the final state of the route graph.

        Returns:
            tuple[list[dict], list[dict]]: A tuple containing two elements:
                - A list of dictionaries, where each dictionary represents a node
                  and its attributes.
                - A list of dictionaries, where each dictionary represents a path
                  (edge) and its attributes.
        """
        nodes: list[dict] = []
        paths: list[dict] = []

        # aggregate node data
        for node in self.context.routes.vs:
            data = {'id': node['name']}

            for key in node.attribute_names():
                if key == 'geom':
                    data['lng'] = node['geom'].x
                    data['lat'] = node['geom'].y
                    if node['geom'].has_z and node['geom'].z > 0.:
                        data['height'] = node['geom'].z
                    #data['geom'] = mapping(node['geom'])
                elif key == 'overnight':
                    data['overnight'] = is_truthy(node['overnight'])
                elif node[key] is not None:
                    data[key] = node[key]

            nodes.append(data)

        # aggregate path data
        for path in self.context.routes.es:
            paths.append({
                'id': path['name'],
                "from": path['from'],
                "to": path['to'],
                'type': path["type"],
                'length_m': path['length_m'],
                'geom': mapping(path['geom']),
            })

        return nodes, paths

    def _format_departure_arrival_times(self, data: list[tuple[tuple[int, float], str, str]], add_reasons: bool = True) -> list[dict]:
        """Formats raw departure/arrival data into a structured list of dictionaries.

        This method groups events by time, converts the time to total hours,
        and aggregates agent IDs and reasons for each time point.

        Args:
            data (list[tuple[tuple[int, float], str, str]]): A list of event tuples.
                Each tuple contains a time tuple (day, hour), an agent UID, and a reason string.
            add_reasons (bool, optional): If True, includes the reasons for the event
                in the output. Defaults to True.

        Returns:
            list[dict]: A sorted list of dictionaries, where each dictionary represents
                a time point and contains the time in total hours ('t'), a list of
                agent UIDs ('agents'), and optionally a list of reasons ('reasons').
        """
        result = {}
        for entry in data:
            if entry[0] not in result:
                result[entry[0]] = {
                    "t": _dt_to_hours(entry[0]),
                    "agents": [],
                }
                if add_reasons:
                    result[entry[0]]['reasons'] = []
            result[entry[0]]['agents'].append(entry[1])
            if add_reasons:
                result[entry[0]]['reasons'].append(entry[2])

        # sort result by time
        result = list(result.values())
        sorted(result, key=lambda x: x['t'])

        return result

    def __repr__(self):
        return json.dumps(self)

    def __str__(self):
        return "JSONOutput"

def _dt_to_hours(dt: tuple[int, float]) -> float:
    """Converts a (day, hour) tuple to total hours.

    Args:
        dt (tuple[int, float]): The date/time tuple, where the first element is the day (1-based)
            and the second element is the hour of the day.

    Returns:
        float: The total number of hours since the beginning of day 1.
    """
    if dt is None:
        return 0.
    return np.round((dt[0]-1)*24. + dt[1], decimals=1)

def _enumerate_arrival_departure(arrival: tuple[int, float] | None, departure: tuple[int, float] | None) -> list[float]:
    """Generates a list of time points between an arrival and departure time.

    This function creates a sequence of time points, in total hours, with a
    0.1-hour resolution, spanning from the arrival time to the departure time,
    inclusive. It handles cases where one or both times might be None.

    Args:
        arrival (tuple[int, float] | None): The arrival time as a (day, hour)
            tuple. If None, and departure is provided, a list with only the
            departure time is returned.
        departure (tuple[int, float] | None): The departure time as a (day,
            hour) tuple. If None, and arrival is provided, a list with only
            the arrival time is returned.

    Returns:
        list[float]: A list of time points in total hours. If both arrival
            and departure are provided, it returns a list of times from
            arrival to departure in 0.1-hour increments. If only one is
            provided, it returns a list with that single time. If both are
            None, it returns an empty list.
    """
    # sanity, this should not happen in the best of worlds...
    if arrival is None and departure is None:
        return []
    # either arrival or departure is None
    if departure is None:
        return [_dt_to_hours(arrival)]
    if arrival is None:
        return [_dt_to_hours(departure)]

    times = []

    for t in np.arange(_dt_to_hours(arrival) * 10, _dt_to_hours(departure) * 10 + 1):
        times.append(float(np.round(t / 10, decimals=1)))

    return times