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

    def run(self):
        """
        Run simulation.
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
                output.run()


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
                my_virtual_agent = copy.deepcopy(agent_to_clone)
                my_virtual_agent.this_hub = hub
                my_virtual_agent.next_hub = target
                my_virtual_agent.route_key = route_key

                agents.append(my_virtual_agent)

        return agents

    def prune_agent_list(self, agent_list) -> List[Agent]:
        """prune agent list to include """
        hashed_agents: Dict[str, Agent] = {}

        for ag in agent_list:
            uid = ag.uid()
            if uid not in hashed_agents:
                hashed_agents[uid] = ag
            else:
                # merge graphs - we want to have all possible graphs at the end
                hashed_agents[uid].route_data.add_nodes_from(ag.route_data.nodes(data=True))
                hashed_agents[uid].route_data.add_edges_from(ag.route_data.edges(data=True))

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
                agent.prepare_for_new_day()
                # TODO: include module list here

            # do single day loop - this is the outer loop for the simulation (per day)
            while len(agents):
                # do single step
                for agent in agents:
                    # calculate context of agent at this node
                    # TODO: include module list here

                    # TODO: call central simulation model here
                    # precalculate next hub
                    leg = self.context.graph.get_edge_data(agent.this_hub, agent.next_hub, agent.route_key)
                    time_taken = leg['length_m'] / 5000
                    # TODO: this should be the actual call...

                    # proceed or stop here?
                    if agent.current_time + time_taken <= agent.max_time:
                        # proceed..., first add time
                        agent.current_time += time_taken

                        # add route taken
                        if agent.route_data.number_of_nodes() == 0:
                            agent.route_data.add_node(agent.this_hub, geom=self.context.graph.nodes[agent.this_hub]['geom'])

                        agent.route_data.add_node(agent.next_hub, geom=self.context.graph.nodes[agent.next_hub]['geom'])
                        agent.route_data.add_edge(agent.this_hub, agent.next_hub, key=agent.route_key,
                                                  time_taken=time_taken)

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
                                # get entries to delete from graph
                                hubs_to_delete = []
                                edges_to_delete = []
                                hub = agent.last_possible_resting_place
                                while hub != agent.this_hub:
                                    hub_id = next(iter(agent.route_data[agent.last_possible_resting_place]))
                                    if hub_id != agent.last_possible_resting_place:
                                        hubs_to_delete.append(hub_id)
                                    edges_to_delete.append(
                                        next(iter(agent.route_data[agent.last_possible_resting_place][hub_id])))
                                    hub = hub_id
                                agent.route_data.remove_edges_from(edges_to_delete)
                                agent.route_data.remove_nodes_from(hubs_to_delete)

                                agents_finished_for_today.extend(
                                    self.create_agents_on_node(agent.last_possible_resting_place, agent))
                            else:
                                agents_finished_for_today.append(agent)
                    # TODO: call central simulation model here - end

                agents = agents_proceed
                agents_proceed = []

            # day finished, let's check if we have unfinished agents left
            if len(agents_finished_for_today):
                agents = self.prune_agent_list(agents_finished_for_today)
            else:
                agents = []

            # increase day
            self.current_day += 1

        # # loop while agents still exist
        #
        # # TODO:
        # # Überdenken, vielleicht splitten wir die Simulation eher logisch auf...
        # # Core-Runner-Modul (1)
        # # Vor und Nach jeweils Module...
        #
        # # Grundsätzliche Ideen im Notizbuch
        # # Pro Agent:
        # # Routen via virtuelle Agenten durchlaufen, die einen Tagesablauf simulieren...
        # # Pro Node: Kontext berechnen (Wetter, etc.) vorher
        #
        # while len(agents):
        #     logger.info(f"Simulating day {state.day}")
        #
        #     agents_after_day_by_hash = {}
        #
        #     for agent in agents:
        #         # dummy run - advance two steps
        #         for target in self.context.routes[agent.next_target]:
        #             for route_key in self.context.routes[agent.next_target][target]:
        #                 # clone agent
        #                 if target != self.config.simulation_end:
        #                     new_agents = self.prepare_agents_at_hub(target, agent.state)
        #                     for new_agent in new_agents:
        #                         agents_after_day_by_hash[new_agent.uid()] = new_agent
        #
        #                     logger.info(f"Agent traversed {agent.start} --{agent.next_leg}--> {agent.next_leg} --{route_key}-->{target}")
        #                 else:
        #                     logger.info("One agent reached end point")
        #
        #     # fill agents from hashed list
        #     agents = []
        #     for key in agents_after_day_by_hash:
        #         agents.append(agents_after_day_by_hash[key])
        #
        #     # increase day
        #     if len(agents) > 0:
        #         state.day += 1
        #
        # # This is the first take of how we handle the simulation:
        # # We will create all simple paths in the graph and let one agent run through each one
        # # we might use concurrent multiprocessing for this
        # # results = []
        #
        # # main loop
        # # for p in nx.all_simple_edge_paths(self.context.graph, self.config.simulation_start,
        # #                                  self.config.simulation_end):
        # #    print(run_simulation(State(p), self.config, self.context))
        # #    # TODO: add to set of results

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

    def run(self):
        """
        Run the output

        :return: created set of results object
        """
        logger.info("******** Output: started ********")

        # run modules
        for module in self.config.output:
            module.run(self.config, self.context, self.set_of_results)

        logger.info("******** Output: finished ********")
