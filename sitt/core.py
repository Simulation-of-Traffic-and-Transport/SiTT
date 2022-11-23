# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Core classes needed to run the application.

.. warning::
    This module is treated as private API.
    Users should not need to use this module directly.
"""

import abc
import copy
import logging
import os.path
from typing import Dict, List

import networkx as nx

from sitt import Configuration, Context, SkipStep, SetOfResults, Agent

__all__ = ['BaseClass', 'Core', 'Preparation', 'Simulation', 'Output']

logger = logging.getLogger()


########################################################################################################################
# Core itself
########################################################################################################################


class Core:
    """
    Core of Simulation
    """

    def __init__(self, config: Configuration):
        """
        Constructor.

        :param config: configuration object
        """
        self.config: Configuration = config

    def run(self) -> List[any] | None:
        """
        Run simulation.

        :return: list of outputs or none if output is skipped
        """
        # preparation step - this step must be run always
        preparation = Preparation(self.config)
        context = preparation.run()

        # simulation step
        if self.config.skip_step != SkipStep.SIMULATION:
            sim = Simulation(self.config, context)
            set_of_results = sim.run()

            # final step: output
            if self.config.skip_step != SkipStep.OUTPUT:
                output = Output(self.config, context, set_of_results)
                return output.run()

        return None


########################################################################################################################
# Abstract base class for Preparation, Simulation, and Output.
########################################################################################################################


class BaseClass(abc.ABC):
    def __init__(self, config: Configuration | None = None):
        """
        Constructor.

        :param config: configuration object
        """
        self.config = config

    def is_skipped(self, module: object, context: Context) -> bool:
        """check for skip"""
        if hasattr(module, 'skip') and module.skip:
            logger.info("Skipping %s due to setting" % module)
            return True

        if hasattr(module, 'conditions') and module.conditions and len(module.conditions) > 0:
            for condition in module.conditions:
                condition_key = condition
                prerequisite = False
                if condition.startswith('not_'):
                    condition_key = condition[4:]
                    prerequisite = True

                mydata = module.conditions[condition]

                if self.condition_ok(condition_key, condition, mydata, module, context=context) == prerequisite:
                    logger.info("Skipping %s due to unmet condition: %s = %s" % (module, condition, mydata))
                    return True

        return False

    def condition_ok(self, key: str, condition: str, data: any, module: object, context: Context = None) -> bool:
        """Handle single condition"""
        if key == 'file_must_exist':
            return os.path.exists(data)
        elif key == 'data_must_exist':
            if 'class' in data:
                c = self.class_instance_for_name(data['class'], module, context)
                if c is not None:
                    if 'key' in data and hasattr(c, data['key']):
                        return getattr(c, data['key']) is not None
            logger.warning("%s not in %s not valid: %s = %s" % (condition, module, condition, data))
        else:
            # Show warning if unknown condition
            logger.warning("Unknown condition in %s: %s = %s" % (module, condition, data))

        return True

    def class_instance_for_name(self, name: str, module: object, context: Context) -> object | None:
        if name == 'context':
            return context
        if name == 'config':
            return self.config
        if name == 'module':
            return module
        return None


########################################################################################################################
# Preparation class
########################################################################################################################


class Preparation(BaseClass):
    """
    Preparation class - will aggregate all information for the simulation
    """

    def __init__(self, config: Configuration):
        super().__init__(config)

    def run(self) -> Context:
        """
        Run the preparation

        :return: created context object
        """
        logger.info("******** Preparation: started ********")

        context = Context()

        # run modules
        for module in self.config.preparation:
            if not self.is_skipped(module, context):
                context = module.run(self.config, context)

        logger.info("******** Preparation: finished ********")

        return context


########################################################################################################################
# Simulation classes
########################################################################################################################

class Simulation(BaseClass):
    """
    Main simulation class - this will run the actual simulation.
    """

    def __init__(self, config: Configuration, context: Context):
        """
        Constructor.

        :param config: configuration object
        :param context: context object
        """
        super().__init__(config)
        self.context = context

        self.current_day = 1
        """Current day of simulation"""

    def check(self) -> bool:
        """check settings"""
        ok = True

        # Checking start and stop hubs
        if not self.config.simulation_start:
            logger.error("simulation_start is empty - simulation failed!")
            ok = False
        if not self.config.simulation_end:
            logger.error("simulation_end is empty - simulation failed!")
            ok = False
        if not self.context.routes:
            logger.error("routes is empty - simulation failed!")
            ok = False

        if logger.level <= logging.INFO:
            logger.info("start:  " + self.config.simulation_start)
            logger.info("end:    " + self.config.simulation_end)
            logger.info("routes: " + str(self.context.routes))

        return ok

    def create_agents_on_node(self, hub: str, agent_to_clone: Agent | None = None) -> List[Agent]:
        """Create a number of virtual agents on a given node"""
        agents: List[Agent] = []

        if agent_to_clone is None:
            agent_to_clone = Agent(hub, '', '', current_time=8.0, max_time=16.0)

        for target in self.context.routes[hub]:
            for route_key in self.context.routes[hub][target]:
                # create new agent for each option
                new_agent = copy.deepcopy(agent_to_clone)
                new_agent.this_hub = hub
                new_agent.next_hub = target
                new_agent.route_key = route_key

                agents.append(new_agent)

        # create new uids, if agents have split
        if len(agents) > 1:
            for agent in agents:
                agent.generate_uid()

        return agents

    def prune_agent_list(self, agent_list: List[Agent]) -> List[Agent]:
        """prune agent list to include """
        hashed_agents: Dict[str, Agent] = {}

        for ag in agent_list:
            hash_id = ag.hash()
            if hash_id not in hashed_agents:
                hashed_agents[hash_id] = ag
            else:
                # merge graphs - we want to have all possible graphs at the end
                for leg in ag.route_data.edges(data=True, keys=True):
                    if hashed_agents[hash_id].route_data.has_edge(leg[0], leg[1], leg[2]):
                        data = hashed_agents[hash_id].route_data.get_edge_data(leg[0], leg[1], leg[2])
                        changed = False
                        for uid in leg[3]['agents']:
                            if uid not in data['agents']:
                                data['agents'][uid] = leg[3]['agents'][uid]
                                changed = True
                        if changed:
                            hashed_agents[hash_id].route_data[leg[0]][leg[1]][leg[2]]['agents'] = data['agents']
                    else:
                        hashed_agents[hash_id].route_data.add_edge(leg[0], leg[1], leg[2], agents=leg[3]['agents'])

        return list(hashed_agents.values())

    def run(self) -> SetOfResults:
        """
        Run the simulation

        :return: created set of results object
        """
        logger.info("******** Simulation: started ********")

        results = SetOfResults()

        # check settings
        if not self.check():
            return results

        # create initial set of agents to run
        agents = self.create_agents_on_node(self.config.simulation_start)
        # reset day counter
        self.current_day = 1

        # do the loop - this is the outer loop for the whole simulation
        while len(agents):
            agents_finished_for_today: List[Agent] = []
            """keeps finished agents for this day"""
            agents_proceed: List[Agent] = []
            """keeps list of agents that proceed today"""

            # prepare context for single day
            for agent in agents:
                agent.prepare_for_new_day(self.current_day)
                # run SimulationPrepareDayInterfaces
                for prep_day in self.config.simulation_prepare_day:
                    prep_day.prepare_for_new_day(self.config, self.context, agent)

            if logger.level <= logging.INFO:
                logger.info("Running day " + str(self.current_day) + " with " + str(len(agents)) + " active agent(s).")

            # do single day loop - this is the outer loop for the simulation (per day)
            while len(agents):
                # do single step
                for agent in agents:
                    # calculate state of agent at this node
                    agent.state.reset()  # reset first
                    # and module calls
                    for def_state in self.config.simulation_define_state:
                        agent.state = def_state.define_state(self.config, self.context, agent)

                    # run the actual state update loop
                    for sim_step in self.config.simulation_step:
                        agent.state = sim_step.update_state(self.config, self.context, agent)

                    # proceed or stop here?
                    if not agent.state.signal_stop_here and agent.state.time_taken > 0 and agent.current_time + agent.state.time_taken <= agent.max_time:
                        # proceed..., first add time
                        start_time = agent.current_time
                        agent.current_time += agent.state.time_taken

                        # add route data
                        agent.route_data.add_edge(agent.this_hub, agent.next_hub, key=agent.route_key,
                                                  agents={agent.uid: {'day': self.current_day, 'start': start_time,
                                                                      'end': agent.current_time,
                                                                      'leg_times': agent.state.time_for_legs}})

                        # finished?
                        if agent.next_hub == self.config.simulation_end:
                            agent.this_hub = self.config.simulation_end
                            agent.next_hub = ''
                            agent.route_key = ''
                            agent.day_finished = self.current_day
                            results.agents_finished.append(agent)
                        else:
                            # proceed to new hub
                            if self.context.graph.nodes[agent.next_hub]['overnight'] == 'y':
                                # overnight stay? if yes, save it
                                agent.last_possible_resting_place = agent.next_hub
                                agent.last_possible_resting_time = agent.current_time

                            agents_proceed.extend(self.create_agents_on_node(agent.next_hub, agent))
                    else:
                        # time exceeded, end day

                        # break if tries are exceeded
                        agent.tries += 1
                        if agent.tries > self.config.break_simulation_after:
                            agent.day_cancelled = self.current_day - self.config.break_simulation_after
                            results.agents_cancelled.append(agent)
                        else:
                            # traceback to last possible resting place, if needed
                            if self.context.graph.nodes[agent.this_hub]['overnight'] == 'n':
                                # compile entries to delete from graph
                                hubs_to_delete = []
                                edges_to_delete = []

                                for path in nx.all_simple_edge_paths(agent.route_data,
                                                                     agent.last_possible_resting_place, agent.this_hub):
                                    for leg in path:
                                        hubs_to_delete.append(leg[1])  # add the second node, because first is either last_possible_resting_place or has been added already
                                        edges_to_delete.append(leg[2])  # add vertex id

                                agent.route_data.remove_edges_from(edges_to_delete)
                                agent.route_data.remove_nodes_from(hubs_to_delete)

                                agents_finished_for_today.extend(
                                    self.create_agents_on_node(agent.last_possible_resting_place, agent))
                            else:
                                agents_finished_for_today.append(agent)

                agents = agents_proceed
                agents_proceed = []

            # day finished, let's check if we have unfinished agents left
            if len(agents_finished_for_today):
                agents = self.prune_agent_list(agents_finished_for_today)
            else:
                agents = []

            # increase day
            self.current_day += 1

        logger.info("******** Simulation: finished ********")

        return results


########################################################################################################################
# Output class
########################################################################################################################


class Output(BaseClass):
    """
    Main simulation class - this will run the actual simulation.
    """

    def __init__(self, config: Configuration, context: Context, set_of_results: SetOfResults):
        """
        Constructor.

        :param config: configuration object
        :param context: context object
        :param set_of_results: SetOfResults object
        """
        super().__init__(config)
        self.context = context
        self.set_of_results = set_of_results

    def run(self) -> List[any]:
        """
        Run the output

        :return: created set of results object
        """
        logger.info("******** Output: started ********")

        outputs: List[any] = []

        # run modules
        for module in self.config.output:
            outputs.append(module.run(self.config, self.context, self.set_of_results))

        logger.info("******** Output: finished ********")

        return outputs
