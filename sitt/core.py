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
from typing import Any

import geopandas as gpd
import igraph as ig
import pandas as pd

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

    def run(self) -> list[Any] | None:
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

    def condition_ok(self, key: str, condition: str, data: Any, module: object, context: Context = None) -> bool:
        """Handle single condition"""
        if key == 'file_must_exist':
            return os.path.exists(data)
        elif key == 'data_must_exist':
            if 'class' in data:
                c = self.class_instance_for_name(data['class'], module, context)
                if c is not None:
                    if 'key' in data and hasattr(c, data['key']):
                        attr = getattr(c, data['key'])
                        # check pandas and geopandas type
                        if type(attr) == gpd.GeoDataFrame or type(attr) == pd.DataFrame:
                            return attr.size > 0
                        return attr is not None
            logger.warning("%s not in %s not valid: %s = %s" % (condition, module, condition, data))
        elif key == 'is_true':
            return data is True
        elif key == 'is_false':
            return data is False
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
        self.context: Context = context
        """Context object for the simulation"""
        self.results = SetOfResults()
        """Set of results for the simulation"""
        self.current_day: int = 1
        """Current day of simulation"""

    def check(self) -> bool:
        """check settings"""
        ok = True

        # Checking start and stop hubs
        if not self.config.simulation_starts or len(self.config.simulation_starts) == 0:
            logger.error("simulation_starts is empty - simulation failed!")
            ok = False
        if not self.config.simulation_ends or len(self.config.simulation_ends) == 0:
            logger.error("simulation_ends is empty - simulation failed!")
            ok = False
        if not self.context.routes:
            logger.error("routes is empty - simulation failed!")
            ok = False

        if logger.level <= logging.INFO:
            logger.info("starts:  " + ", ".join(self.config.simulation_starts))
            logger.info("ends:    " + ", ".join(self.config.simulation_ends))

        return ok

    def run(self) -> SetOfResults:
        """
        Run the simulation - this is the entry point to actually start the simulation core

        :return: created set of results object
        """
        logger.info("******** Simulation: started ********")

        # check settings
        if not self.check():
            return self.results

        # prepare the initial state
        agents = self._initialize_simulation()

        # do the loop - this is the outer loop for the whole simulation (per day)
        # it will run until there are no agents left
        while len(agents):
            agents = self._run_single_day(agents)

            # # TODO: make this configurable
            # if self.current_day > 30:
            #     logger.info("Simulation finished after 30 days!")
            #     break

        # end simulation - do some history and statistics
        self._end_simulation()

        logger.info("******** Simulation: finished ********")

        return self.results

    def _initialize_simulation(self) -> list[Agent]:
        # set day counter to first day
        self.current_day = 1

        # create initial set of agents to run
        agents = []
        #  for each hub in the start list, create agents on that hub and add them to the list of agents to run
        for hub in self.config.simulation_starts:
            # iterate outgoing edges from the hub
            for edge in self.context.routes.incident(hub):
                e = self.context.routes.es[edge]
                target = e.target_vertex['name']

                if len(self.config.means_of_transport) > 0:
                    # multiple types of transport: create a new agent for each type and outgoing edge and add it to the
                    # list
                    for t_type in self.config.means_of_transport:
                        agent = Agent(hub, target, e['name'])
                        agent.transport_type = t_type
                        agents.append(agent)
                else:
                    # only one type of transport: create a new agent for each outgoing edge and add it to the list
                    agents.append(Agent(hub, target, e['name']))

        return agents

    def _run_single_day(self, agents: list[Agent]) -> list[Agent]:
        """
        Run single day - called in the outer loop of run

        :param agents: list of agents
        :return: new list of agents (can be empty list -> this indicates the end of the simulation)
        """
        agents_finished_for_today: list[Agent] = []
        """keeps finished agents for this day"""

        # prepare agents for a single day - run for each agent
        for agent in agents:
            agent.prepare_for_new_day(current_day=self.current_day)

        # run day hook pre
        for day_hook_pre in self.config.simulation_day_hook_pre:
            agents = day_hook_pre.run(self.config, self.context, agents, agents_finished_for_today, self.results, self.current_day)

        if logger.level <= logging.INFO:
            logger.info("Running day " + str(self.current_day) + " with " + str(len(agents)) + " active agent(s).")

        step = 1
        # do a single day loop - this is the inner loop for the simulation (per day)
        while len(agents):
            agents_proceed: list[Agent] = []
            """keeps list of agents that proceed today"""

            # prune agents - remove duplicates before processing
            len_before = len(agents)
            agents = self._prune_agent_list(agents)
            len_after = len(agents)

            if len_before - len_after > 0 and logger.level <= logging.INFO:
                logger.info(
                    f"   - Pruned {len_before - len_after} duplicate agents from the list, leaving {len_after} agents.")

            # do a single step for each agent
            for agent in agents:
                self._run_single_step(agent, agents_proceed, agents_finished_for_today)

            agents = agents_proceed

            if logger.level <= logging.INFO:
                logger.info(f" - Step {step}, {len(agents)} agents, {len(agents_finished_for_today)} finished.")
                step += 1

        logger.info("Day " + str(self.current_day) + " finished.")
        # run day hook post
        for day_hook_post in self.config.simulation_day_hook_post:
            agents_finished_for_today = day_hook_post.run(self.config, self.context, agents, agents_finished_for_today, self.results, self.current_day)

        logger.info("Day " + str(self.current_day) + " finished - post.")

        agents_proceeding_tomorrow = self._finish_day(agents_finished_for_today)

        logger.info(f"{len(agents_proceeding_tomorrow)} proceeding.")

        # increase day
        self.current_day += 1

        return agents_proceeding_tomorrow

    def _run_single_step(self, agent: Agent, agents_proceed: list[Agent],
                         agents_finished_for_today: list[Agent]):
        """
        Run a single step for a specific agent - all parameters will be mutated in this method!

        :param agent: agent to run results for (mutated)
        :param agents_proceed: list of agents that proceed today (mutated)
        :param agents_finished_for_today:  list of agents that have finished for today (mutated)
        """

        # calculate state of agent at this node
        agent.state.reset()  # reset first
        # and run define state hooks, if any
        for def_state in self.config.simulation_define_state:
            agent.state = def_state.define_state(self.config, self.context, agent)

        # save route_key to variable, so we can set last_route below
        remembered_route_key = agent.route_key
        # get the next leg from context
        next_leg: ig.Edge = self.context.get_path_by_id(agent.route_key)

        # run the actual state update loop
        for sim_step in self.config.simulation_step:
            # conditions are met?
            if sim_step.check_conditions(self.config, self.context, agent, next_leg):
                # traverse in reversed order?
                if agent.this_hub != next_leg['from']:
                    agent.state.is_reversed = True
                    if agent.next_hub != next_leg['from']:
                        print(f"error - legs reversed {agent.uid} in {next_leg['name']} with {next_leg['from']} -> {next_leg['to']}, agent status is: {agent.this_hub} -> {agent.next_hub} via {agent.route_key}")

                # is this step cancelled?
                if sim_step.check_cancel(self.config, self.context, agent, next_leg):
                    agent.state.signal_stop_here = True
                    agent.is_cancelled = True
                    agent.cancel_reason = 'cancelled due to step cancellation'
                else:
                    # run state update - step hooks have to be called in this method
                    agent.state = sim_step.update_state(self.config, self.context, agent, next_leg)

                break

        # calculate times
        start_time = agent.current_time
        end_time = agent.current_time + agent.state.time_taken

        # step has been run, now we have to check certain conditions

        # end day:
        # case 1) signal to stop day here
        # case 2) time_taken is negative - brute force signal to stop here
        # case 3) time reached or exceeded for today
        if agent.state.signal_stop_here or agent.state.time_taken < 0 or end_time >= agent.max_time:
            self._agent_end_day(agent, agents_finished_for_today)
        else:
            # proceed the agent to new hub

            # add hub and vertex history (this will add the vertex to the agent's history)
            agent.create_route_data(agent.this_hub, agent.next_hub, agent.route_key, start_time, agent.state.is_reversed)

            # set time and last route
            agent.current_time = end_time
            agent.last_route = remembered_route_key

            # case 4) end of simulation reached -> finish agent and add to day finish
            if agent.next_hub in self.config.simulation_ends:
                self._agent_finish(agent, agents_finished_for_today)
            else:
                # case 5) proceed to next hub
                self._agent_proceed(agent, agents_proceed, agents_finished_for_today)

    @staticmethod
    def _prune_agent_list(agents: list[Agent]) -> list[Agent]:
        """
        Remove duplicate agents from a list while preserving finished agents.

        This method creates a deduplicated list of agents by identifying unique agents based on their
        route key, current hub, next hub, transport type, and current time. Agents that are marked as
        finished are always included in the result, regardless of whether they appear to be duplicates.
        For agents that are not finished, only the first occurrence of each unique combination is kept.

        :param agents: A list of Agent objects to be pruned. Each agent contains information about
            its current state, including route, hub locations, transport type, and completion status.
        :return: A deduplicated list of Agent objects. All finished agents are preserved, and for
            non-finished agents, only unique combinations based on (route_key, this_hub, next_hub,
            transport_type, current_time) are retained. The order of agents in the original list
            is preserved for the first occurrence of each unique agent.
        """
        # prune agents to create only unique ones
        agent_list = []
        unique_agents = set()

        for agent in agents:
            if not agent.is_finished and len(agent.forced_route) == 0:
                key = (agent.route_key, agent.this_hub, agent.next_hub, agent.transport_type, agent.current_time)
                if key in unique_agents:
                    continue
                unique_agents.add(key)
            agent_list.append(agent)

        return agent_list

    @staticmethod
    def _agent_finish(agent: Agent, agents_finished_for_today: list[Agent]):
        agent.visited_hubs.add(agent.next_hub)
        agent.this_hub = agent.next_hub
        agent.next_hub = ''
        agent.route_key = ''
        agent.is_finished = True
        # set arrival time
        agents_finished_for_today.append(agent)


    def _agent_proceed(self, agent: Agent, agents_proceed: list[Agent], agents_finished_for_today: list[Agent]):
        # if we deal with overnight tracebacks, we want to remember the last possible resting place and time
        if self.config.overnight_trace_back:
            # get some data about the hub that was just reached
            reached_hub = self.context.routes.vs.find(name=agent.next_hub)
            # reached hub is an overnight hub?
            if reached_hub['overnight']:
                agent.last_overnight_hub = reached_hub['name']
                # save the last possible resting place
            else:
                # check neighbors if there is an overnight hub close by
                if 'overnight_hub' in reached_hub.attribute_names() and reached_hub['overnight_hub']:
                    # do we have a connection to this hub? => only if we find it in neighbors, do we add it to the last
                    # possible resting places
                    for n in reached_hub.neighbors():
                        if n['name'] == reached_hub['overnight_hub']:
                            # mark hub as the last overnight hub
                            agent.last_overnight_hub = reached_hub['name']
                            break
                        elif n['name'] in agent.visited_hubs:
                            # remove from visited hubs
                            agent.visited_hubs.remove(n['name'])

        # add current hub to visited ones
        agent.visited_hubs.add(agent.next_hub)
        # update current hub
        agent.this_hub = agent.next_hub

        # shorten forced routes
        if len(agent.forced_route) > 0:
            agent.forced_route = agent.forced_route[1:]

        # add to the list of agents to proceed
        agents_ok, agents_cancelled = self._split_agent_on_hub(agent)
        agents_proceed.extend(agents_ok)
        agents_finished_for_today.extend(agents_cancelled)

    def _agent_end_day(self, agent: Agent, agents_finished_for_today: list[Agent]):
        """
        End this day for agent.

        :param agent: agent to run results for (mutated)
        :param agents_finished_for_today:  list of agents that have finished for today (mutated)
        """

        if logger.level <= logging.DEBUG:
            logging.debug(f"Agent {agent.uid} [{agent.this_hub}]: ending day")

        # break if tries are exceeded
        agent.tries += 1

        # if tries exceeded, cancel agent
        if agent.tries > self.config.break_simulation_after:
            agent.is_cancelled = True
            agent.cancel_reason = f"Exceeded agent tries on this route"
            agent.cancel_details = f"tries: {self.config.break_simulation_after}, route via: " + ', '.join(agent.route[::2])
        elif agent.state.signal_stop_here and agent.this_hub == agent.last_resting_place:
            # agent has not proceeded at all today - cancel it!
            self.__set_agent_no_sleep(agent, agents_finished_for_today)
            return
        else:
            # reset forced route data
            agent.forced_route = []

            # traceback to last possible resting place, if needed
            if self.config.overnight_trace_back and self.context.graph.vs.find(name=agent.this_hub)['overnight'] is not True:
                # copy route to history
                agent.route_before_traceback = agent.route.copy()
                agent.route_reversed_before_traceback = agent.route_reversed.copy()

                # get index of last overnight hub
                last_overnight_hub_index = agent.route.index(agent.last_overnight_hub)
                # do not track back to the beginning - cancel such agents, because they give an interesting insight into
                # routes that could not be tracked today
                if last_overnight_hub_index == 0:
                    self.__set_agent_no_sleep(agent, agents_finished_for_today)
                    return

                to_delete = agent.route[last_overnight_hub_index+1:]
                # get hubs and routes for deletion
                hubs = to_delete[1::2]
                routes = to_delete[::2]
                last_known_departure = None # keep last known time

                # delete from history
                for hub in hubs:
                    # if hub in agent.visited_hubs:
                    agent.visited_hubs.remove(hub)
                for route in routes:
                    last_known_departure = agent.route_times[route][0]
                    del agent.route_times[route]
                # update data
                agent.this_hub = agent.last_overnight_hub
                agent.route = agent.route[:last_overnight_hub_index + 1]
                agent.route_reversed = agent.route_reversed[:int((len(agent.route)-1)/2)]
                agent.forced_route = routes
                # reduced to none?
                if len(agent.route) < 2:
                    agent.current_time = last_known_departure
                else:
                    agent.current_time = agent.route_times[agent.route[-2]][-1]

                # delete rest history that is more than or same as the maximum last resting time
                for i in range(len(agent.rest_history)):
                    if agent.current_time is None or agent.rest_history[i][0] >= agent.current_time:
                        agent.rest_history = agent.rest_history[:i]
                        break

            # add last resting place
            agent.last_resting_place = agent.this_hub

        # add to list of agents that have finished for today
        agents_finished_for_today.append(agent)

    @staticmethod
    def __set_agent_no_sleep(agent: Agent, agents_finished_for_today: list[Agent]):
        """
        Mark an agent as cancelled due to lack of sleep and add it to the finished agents list.

        This method cancels an agent that could not find a suitable resting place for the day.
        It sets the cancellation status and reason, resets the agent's route data to only include
        the starting hub, and adds the agent to the list of agents that have finished for today.

        :param agent: The agent to be marked as cancelled. This agent will be mutated by setting
            its cancellation status, reason, and resetting its route information.
        :param agents_finished_for_today: List of agents that have finished their activities for
            the current day. The cancelled agent will be appended to this list.
        """
        agent.is_cancelled = True
        agent.cancel_reason = f"No sleep today"
        agent.cancel_details = "route via: " + ', '.join(agent.route[::2])
        agent.route = agent.route[:1]
        agent.route_reversed = []
        agent.route_times = {}
        agents_finished_for_today.append(agent)

    def _get_possible_routes_for_agent_on_hub(self, agent: Agent) -> list[tuple[str, str]]:
        """Get a list of possible routes for an agent on a hub.

        This method determines the next possible routes an agent can take from its current hub.
        It considers forced routes and avoids visiting hubs that have already been visited.

        Args:
            agent: The agent for which to find possible routes.

        Returns:
            A list of tuples, where each tuple contains the route name (str) and the target hub name (str).
        """
        possible_routes: list[tuple[str, str]] = []

        # if we have a forced route, only consider this
        if len(agent.forced_route) > 0:
            e = self.context.routes.es.find(name=agent.forced_route[0])
            if e.source_vertex['name'] != agent.this_hub:
                raise Exception("Agent has a forced route, but it does not start from the current hub.")

            # add target hub and route name to possible routes
            possible_routes.append((agent.forced_route[0], e.target_vertex['name'],))
        else:
            for edge in self.context.routes.incident(agent.this_hub, mode='out'):
                e = self.context.routes.es[edge]
                route_name = e['name']
                target_hub = e.target_vertex

                # is target hub a no-go? if yes, skip
                if 'no_go' in target_hub.attributes() and target_hub['no_go'] is True:
                    continue

                # Does the target exist in our route data? If yes, skip, we will not visit the same place twice!
                if target_hub['name'] in agent.visited_hubs:
                    continue

                # add target hub and route name to possible routes
                possible_routes.append((route_name, target_hub['name'],))

        return possible_routes

    def _split_agent_on_hub(self, agent: Agent) -> tuple[list[Agent], list[Agent]]:
        """Split an agent into multiple agents if there are multiple possible routes.

        When an agent arrives at a hub, this method checks for all possible
        outgoing routes. If there's more than one valid route, the agent is
        cloned for each additional route. The original agent takes the first
        possible route, and deep copies are created for the others. This allows
        the simulation to explore multiple paths simultaneously. If no valid
        routes are found, the agent is marked as cancelled.

        Args:
            agent: The agent to be split or processed. Its current state is used
                to determine the next possible routes.

        Returns:
            A tuple containing two lists:
            - The first list contains agents that can proceed on their new routes.
            - The second list contains agents that have been cancelled due to a
              lack of possible routes from the current hub.
        """
        possible_routes = self._get_possible_routes_for_agent_on_hub(agent)
        hub = self.context.graph.vs.find(name=agent.this_hub)

        # if no possible routes, we can't move forward'
        if len(possible_routes) == 0:
            # add to failed routes
            agent.is_cancelled = True
            agent.cancel_reason = "No possible routes left (dead end)"
            agent.cancel_details = "route via: " + ', '.join(agent.route[::2])
            coords = hub['geom']
            agent.state.last_coordinate_after_stop = (coords.x, coords.y)
            return [], [agent]

        # contains routes: clone agent for each possible route
        agents: list[Agent] = []

        for i, (route_name, target_hub) in enumerate(possible_routes):
            # first route - use the original agent
            if i == 0:
                new_agent = agent
            else:
                # other routes - create new agent, copy it and create new uid
                new_agent = copy.deepcopy(agent)
                new_agent.generate_uid()

            # set new targets
            new_agent.next_hub = target_hub
            new_agent.route_key = route_name

            agents.append(new_agent)

        return agents, []

    def _finish_day(self, agents: list[Agent]) -> list[Agent]:
        # first, we group our agents per hub - finished or cancelled agents are ignored, se we might have an empty list here
        agents_per_hub = self._group_agents_by_hub(agents)
        agents_proceeding_tomorrow: list[Agent] = []

        for hub, agent_list in agents_per_hub.items():
            # check if overnight stay is actually an end point of the simulation, if so, we set agent to finished
            v = self.context.graph.vs.find(name=hub)
            if 'overnight_hub' in v.attribute_names() and v['overnight_hub'] in self.config.simulation_ends:
                for agent in agent_list:
                    agent.is_finished = True
                    if self.config.keep_agent_data_in_results:
                        self.results.add_agent(agent)

                # continue - do not add agents to proceed tomorrow
                continue

            # list of all agent ids that led to this hub
            agent_ids = []
            for agent in agent_list:
                if not agent.is_cancelled:
                    agent_ids.append(agent.uid)
            agent = Agent(hub, '', '', do_not_generate_uid=True)
            has_agents_to_proceed = False

            for agent in agent_list:
                if agent.is_cancelled or agent.is_finished:
                    if self.config.keep_agent_data_in_results:
                        self.results.add_agent(agent)
                else:
                    has_agents_to_proceed = True

                    agent.visited_hubs.update(agent.visited_hubs)
                    agent_ids.append(agent.uid)

            if has_agents_to_proceed:
                means_of_transport = self.config.means_of_transport if len(self.config.means_of_transport) > 0 else [None]

                # get all possible routes for this hub
                for route in self._get_possible_routes_for_agent_on_hub(agent):
                    for mean_of_transport in means_of_transport:
                        new_agent = Agent(hub, route[1], route[0])
                        new_agent.visited_hubs = copy.deepcopy(agent.visited_hubs)
                        new_agent.parents = copy.deepcopy(agent_ids)
                        new_agent.tries = 0
                        new_agent.transport_type = mean_of_transport
                        agents_proceeding_tomorrow.append(new_agent)

        return agents_proceeding_tomorrow

    @staticmethod
    def _group_agents_by_hub(agents: list[Agent]) -> dict[str, list[Agent]]:
        """
        Group agents by their current hub location.

        This method organizes a list of agents into a dictionary where each key represents
        a hub name and the corresponding value is a list of all agents currently located
        at that hub. This grouping facilitates hub-specific processing of agents during
        the simulation.

        :param agents: A list of Agent objects to be grouped. Each agent must have a
            'this_hub' attribute indicating its current hub location.
        :return: A dictionary mapping hub names (strings) to lists of Agent objects.
            Each key is a hub identifier, and the value is a list containing all agents
            currently at that hub. If no agents are at a particular hub, that hub will
            not appear as a key in the dictionary.
        """
        agents_per_hub: dict[str, list[Agent]] = {}

        for agent in agents:
            if agent.this_hub not in agents_per_hub:
                agents_per_hub[agent.this_hub] = []
            agents_per_hub[agent.this_hub].append(agent)

        return agents_per_hub

    def _end_simulation(self):
        """
        Run end simulation tasks
        """
        for day_hook_pre in self.config.simulation_day_hook_pre:
            day_hook_pre.finish_simulation(self.results, self.config, self.context, self.current_day)
        for day_hook_post in self.config.simulation_day_hook_post:
            day_hook_post.finish_simulation(self.results, self.config, self.context, self.current_day)


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

    def run(self) -> list[Any]:
        """
        Run the output

        :return: created set of results object
        """
        logger.info("******** Output: started ********")

        outputs: list[Any] = []

        # run modules
        for module in self.config.output:
            outputs.append(module.run(self.config, self.context, self.set_of_results))

        logger.info("******** Output: finished ********")

        return outputs
