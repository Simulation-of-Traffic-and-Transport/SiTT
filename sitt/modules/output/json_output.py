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
        data (list[tuple[float, str, str]]): A list of event tuples.
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
        agents, min_dt, max_dt = self._agent_list_to_data(set_of_results.agents)

        activities = self._agent_activities(set_of_results.agents)

        # add hubs and paths
        hubs, paths = self._graph_to_data(activities)

        # convert start date to stringq
        start_date = ""
        if self.config.start_date:
            start_date = str(self.config.start_date)[0:10]

        # create result dictionary
        return {
            "from": self.config.simulation_starts,
            "to": self.config.simulation_ends,
            "start": min_dt,
            "end": max_dt,
            "simulation_route": self.config.simulation_route,
            "simulation_route_reverse": self.config.simulation_route_reverse,
            "start_date": start_date,
            "break_simulation_after": self.config.break_simulation_after,
            "agents": agents,
            "hubs": hubs,
            "paths": paths,
        }

    def _agent_list_to_data(self, agents: list[Agent]) -> tuple[list[dict], float, float]:
        """Processes a list of agents to generate serializable data and time bounds.

        This method iterates through a list of Agent objects, converting each one
        into a dictionary format suitable for JSON serialization using the
        `_agent_to_data` helper method. While processing, it also tracks the
        earliest start time (min_dt) and the latest end time (max_dt) across all
        agents' journeys. The final list of agent data is sorted by agent UID.

        Args:
            agents (list[Agent]): A list of Agent objects from the simulation.

        Returns:
            tuple[list[dict], float, float]: A tuple containing:
                - A list of dictionaries, each representing an agent's data, sorted by UID.
                - The overall earliest start time (min_dt) among all agents.
                - The overall latest end time (max_dt) among all agents.
        """
        list_of_agents: list[dict] = []
        min_dt = float('inf')
        max_dt = float('-inf')

        for agent in agents:
            data = self._agent_to_data(agent)
            list_of_agents.append(data)
            if data['min_dt'] is not None and data['min_dt'] < min_dt:
                min_dt = data['min_dt']
            if data['max_dt'] is not None and data['max_dt'] > max_dt:
                max_dt = data['max_dt']

        list_of_agents = sorted(list_of_agents, key=lambda x: x['uid'])
        return list_of_agents, float(np.round(np.floor(min_dt*10)/10, decimals=1) if min_dt < float('inf') else None), float(np.round(np.ceil(max_dt*10)/10, decimals=1) if max_dt > float('-inf') else None)

    def _agent_to_data(self, agent: Agent) -> dict:
        """Converts an Agent object into a serializable dictionary.

        This method extracts key information from an Agent object, including its
        unique ID, start and end hubs, journey times, and the sequence of hubs
        and edges in its route. It also includes status flags for cancellation
        or completion. The resulting dictionary is designed for easy conversion
        to JSON.

        Args:
            agent (Agent): The agent object containing the journey and status
                information to be serialized.

        Returns:
            dict: A dictionary representing the agent's data, including UID,
                start/end points, times, route, and status.
        """
        start_hub, end_hub, min_dt, max_dt = agent.get_start_end()

        agent_data: dict[str, Any] = {
            "uid": agent.uid,
            "start_hub": start_hub,
            "end_hub": end_hub,
            "min_dt": min_dt,
            "max_dt": max_dt,
            "hubs": agent.route[::2],
            "edges": agent.route[1::2],
        }

        if agent.is_cancelled:
            agent_data['cancelled'] = True
            if agent.state.last_coordinate_after_stop is not None:
                agent_data['last_coordinate_after_stop'] = agent.state.last_coordinate_after_stop
        if agent.is_finished:
            agent_data['finished'] = True

        return agent_data

    @staticmethod
    def _agent_activities(agents: list[Agent]) -> dict:
        activities: dict[tuple[str, str], dict[tuple[str, str], dict]] = {}

        for agent in agents:
            for route in agent.iterate_routes():
                entity_key = (route['uid'], route['type'])
                if entity_key not in activities:
                    activities[entity_key] = {}

                if route['type'] == 'hub':
                    key = (route['arrival'], route['departure'])
                else:
                    key = (route['legs'][0], route['legs'][-1])

                if key not in activities[entity_key]:
                    if route['type'] == 'hub':
                        rest = route['rest'] if 'rest' in route and route['rest'] is not None else None

                        # get sleep until time if available
                        departure = route['departure']
                        if departure is None and 'sleep_until' in agent.additional_data:
                            departure = agent.additional_data['sleep_until']
                            rest = [(route['arrival'], departure - route['arrival'], 'sleep')]

                        activities[entity_key][key] = {
                            "arrival": route['arrival'],
                            "departure": departure,
                            "agents": [agent.uid],
                        }
                        if rest is not None:
                            activities[entity_key][key]['rest'] = rest
                    else:
                        activities[entity_key][key] = {
                            "legs": route['legs'],
                            "agents": [agent.uid],
                            "reversed": route['reversed'],
                        }
                        if 'rest' in route and route['rest'] is not None:
                            activities[entity_key][key]['rest'] = route['rest']
                else:
                    activities[entity_key][key]['agents'].append(agent.uid)

        return activities

    def _graph_to_data(self, activities: dict[tuple[str, str], dict[tuple[str, str], dict]]) -> tuple[list[dict], list[dict]]:
        """Extracts and formats node and path data from the simulation results graph.

        This method iterates through the vertices (hubs) and edges (paths) of the
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
        hubs: list[dict] = []
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

            if (node['name'], 'hub') in activities:
                data['activity'] = list(activities[(node['name'], 'hub')].values())

            hubs.append(data)

        # aggregate path data
        for path in self.context.routes.es:
            data = {
                'id': path['name'],
                "from": path['from'],
                "to": path['to'],
                'type': path["type"],
                'length_m': path['length_m'],
                'geom': mapping(path['geom']),
            }

            if (path['name'], 'edge') in activities:
                data['activity'] = list(activities[(path['name'], 'edge')].values())

            paths.append(data)

        return hubs, paths

    def _format_departure_arrival_times(self, data: list[tuple[float, str, str]], add_reasons: bool = True) -> list[dict]:
        """Formats raw departure/arrival data into a structured list of dictionaries.

        This method groups events by time, converts the time to total hours,
        and aggregates agent IDs and reasons for each time point.

        Args:
            data (list[tuple[float, str, str]]): A list of event tuples.
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
                    "t": _round_time(entry[0]),
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

def _round_time(dt: float) -> float:
    if dt is None:
        return 0.
    return np.round(dt, decimals=1)
