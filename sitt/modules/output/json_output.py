# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create basic json output"""
import json
import logging
from typing import Dict, List, Tuple

import networkx as nx
from shapely.geometry import mapping

from sitt import Agent, Configuration, Context, OutputInterface, SetOfResults, is_truthy

logger = logging.getLogger()


class JSONOutput(OutputInterface):
    """Create basic json output"""

    def __init__(self, to_string: bool = True, show_output: bool = False, save_output: bool = False,
                 filename: str = 'simulation_output.json', indent: int = 0):
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

    def run(self, config: Configuration, context: Context, set_of_results: SetOfResults) -> str:
        if self.skip:
            return ''

        logger.info("OutputInterface JSONOutput run")

        # indent 0 is treated as no indent
        if self.indent == 0:
            self.indent = None

        # create dictionary from data using methods below
        result = self.create_dict_from_data(config, context, set_of_results)
        if self.to_string:
            result = json.dumps(result, indent=self.indent)
        if self.show_output:
            # always log at log level to show output
            logger.log(logger.level, result)

        if self.save_output:
            file = open(self.filename, 'w')

            # already converted to string?
            if self.to_string:
                file.write(result)
            else:
                file.write(json.dumps(result, indent=self.indent))

            file.close()

        return result

    def create_dict_from_data(self, config: Configuration, context: Context, set_of_results: SetOfResults) -> Dict[str, any]:
        """create a dict from passed data"""

        legs: Dict[str, Dict[str, any]] = {}

        merge_legs, agents_finished = self._agent_list_to_data(config, context, set_of_results.agents_finished)
        legs = self._append_to_legs(legs, merge_legs)
        merge_legs, agents_cancelled = self._agent_list_to_data(config, context, set_of_results.agents_cancelled)
        legs = self._append_to_legs(legs, merge_legs)

        nodes, paths = self._graph_to_data(context.graph)

        # TODO add more data from configuration and context
        return {
            "simulation_start": config.simulation_start,
            "simulation_end": config.simulation_end,
            "agents_finished": agents_finished,
            "agents_cancelled": agents_cancelled,
            "legs": legs,
            "nodes": nodes,
            "paths": paths,
        }

    def _agent_list_to_data(self, config: Configuration, context: Context, agents: List[Agent]) -> Tuple[Dict[str, Dict[str, any]], List[dict]]:
        """converts a list of agents to raw data"""
        agent_list: List[dict] = []
        legs: Dict[str, Dict[str, any]] = {}

        for agent in agents:
            # get data, is a dict of legs and agent data
            merge_legs, agent = self._agent_to_data(config, context, agent)

            # aggregate leg data
            legs = self._append_to_legs(legs, merge_legs)

            agent_list.append(agent)

        return legs, agent_list

    def _agent_to_data(self, config: Configuration, context: Context, agent: Agent) -> Tuple[Dict[str, Dict[str, any]], dict]:
        """converts a single agent to raw data, it is a dict of legs and agent data"""

        status: str = 'undefined'
        day: int = 0
        if agent.day_cancelled >= 0:
            status = 'cancelled'
            day = agent.day_cancelled
        if agent.day_finished >= 0:
            status = 'finished'
            day = agent.day_finished

        legs: Dict[str, Dict[str, any]] = {}
        uids: Dict[str, bool] = {agent.uid: True}
        for leg in agent.route_data.edges(data=True, keys=True):
            legs[leg[2]] = {'from': leg[0], 'to': leg[1], 'agents': leg[3]['agents']}

            for uid in leg[3]['agents']:
                uids[uid] = True

        agent = {
            "uid": agent.uid,
            "uids": list(uids.keys()),
            "status": status,
            "day": day,
            "hour": agent.current_time,
        }

        return legs, agent

    def _append_to_legs(self, legs: Dict[str, Dict[str, any]], merge: Dict[str, Dict[str, any]]) -> Dict[str, Dict[str, any]]:
        """Helper to merge legs"""

        for key in merge:
            if key in legs:
                for agent in merge[key]['agents']:
                    if agent not in legs[key]['agents']:
                        legs[key]['agents'][agent] = merge[key]['agents'][agent]
            else:
                legs[key] = merge[key]

        return legs

    def _graph_to_data(self, graph: nx.MultiGraph) -> Tuple[List[dict], List[dict]]:
        nodes: List[dict] = []
        paths: List[dict] = []

        # aggregate node data
        for node in graph.nodes(data=True):
            data = {'id': node[0]}

            for key in node[1]:
                if key == 'geom':
                    data['geom'] = mapping(node[1]['geom'])
                elif key == 'overnight':
                    data['overnight'] = is_truthy(node[1]['overnight'])
                else:
                    data[key] = node[1][key]

            nodes.append(data)

        # aggregate path data
        for path in graph.edges(data=True, keys=True):
            paths.append({
                'id': path[2],
                'from': path[0],
                'to': path[1],
                'length_m': path[3]['length_m'],
                'geom': mapping(path[3]['geom']),
            })

        return nodes, paths

    def __repr__(self):
        return json.dumps(self)

    def __str__(self):
        return "JSONOutput"
