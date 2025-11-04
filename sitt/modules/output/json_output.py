# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create basic json output"""
import json
import logging
from typing import Any

import igraph as ig
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
        agents = self._agent_graph_to_data(set_of_results.agents)

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
        }

    def _agent_graph_to_data(self, agents: ig.Graph) -> list[dict]:
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

        for v in agents.vs:
            list_of_agents.append(self._agent_to_data(v['agent']))

        return list_of_agents

    def _agent_list_to_data(self, agents: list[Agent]) -> tuple[list[dict], dict[str, dict[str, Any]]]:
        """Converts a single agent object into a serializable dictionary format.

        This method processes an Agent object to extract its summary data and
        the history of its traversal through the graph for a given day. It
        captures the agent's final status, the graph elements (nodes and edges)
        it interacted with, and any other agents it was grouped with.

        Args:
            agent (Agent): The agent object to be converted.

        Returns:
            tuple[dict, dict[str, dict[str, Any]]]: A tuple containing two dictionaries:
                - The first dictionary holds the agent's summary data, including its
                  UID, final status ('finished' or 'cancelled'), the day and hour
                  of completion/cancellation, and a list of all associated agent
                  UIDs it may have merged with.
                - The second dictionary represents the traversal history, mapping
                  node and edge IDs to dictionaries containing their details and
                  the agents present on them.
        """
        main_agent_list: list[dict] = []
        agent_list: dict[str, dict[str, Any]] = {}

        for agent in agents:
            # get data, is a dict of agent data and list of agents
            agent_data, added_list = self._agent_to_data(agent)

            # aggregate agent data
            agent_list = self._merge_history_lists(agent_list, added_list)

            main_agent_list.append(agent_data)

        return main_agent_list, agent_list

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
            "start": _dt_to_hours(agent.route_data.vs[0]['departure']),
            "end": _dt_to_hours(agent.route_data.vs[-1]['arrival']),
        }

        if agent.day_cancelled >= 0:
            agent_data['cancelled'] = True
        if agent.day_finished >= 0:
            agent_data['finished'] = True

        return agent_data

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
        for node in set_of_results.route.vs:
            data = {'id': node['name']}

            for key in node.attribute_names():
                if key == 'geom':
                    data['geom'] = mapping(node['geom'])
                elif key == 'overnight':
                    data['overnight'] = is_truthy(node['overnight'])
                elif key == 'arrival':
                    if len(node['arrival']) > 0:
                        data['arrival'] = self._format_departure_arrival_times(node['arrival'])
                elif key == 'departure':
                    if len(node['departure']) > 0:
                        data['departure'] = self._format_departure_arrival_times(node['departure'])
                else:
                    data[key] = node[key]

            nodes.append(data)

        # aggregate path data
        for path in set_of_results.route.es:
            leg_times = []
            for leg_time in path['leg_times']:
                leg_times.append(self._format_departure_arrival_times(leg_time, False) if len(leg_time) > 0 else [])
            paths.append({
                'id': path['name'],
                "from": path['from'],
                "to": path['to'],
                'type': path["type"],
                'length_m': path['length_m'],
                'geom': mapping(path['geom']),
                'leg_times': leg_times,
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
    return (dt[0]-1)*24. + dt[1]