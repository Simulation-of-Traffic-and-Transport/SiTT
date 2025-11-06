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

                # create new agent for each outgoing edge and add it to the list
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

        # prepare agents for single day - run for each agent
        for agent in agents:
            agent.prepare_for_new_day(current_day=self.current_day)

        # run day hook pre
        for day_hook_pre in self.config.simulation_day_hook_pre:
            agents = day_hook_pre.run(self.config, self.context, agents, self.results, self.current_day)

        if logger.level <= logging.INFO:
            logger.info("Running day " + str(self.current_day) + " with " + str(len(agents)) + " active agent(s).")

        step = 1
        # do single day loop - this is the inner loop for the simulation (per day)
        while len(agents):
            agents_proceed: list[Agent] = []
            """keeps list of agents that proceed today"""

            # do single step for each agent
            for agent in agents:
                self._run_single_step(agent, agents_proceed, agents_finished_for_today)

            agents = agents_proceed

        if logger.level <= logging.DEBUG:
            logger.debug(f" - step {step} {len(agents)} {len(agents_finished_for_today)}")
            step += 1

        # run day hook post
        for day_hook_post in self.config.simulation_day_hook_post:
            agents = day_hook_post.run(self.config, self.context, agents, self.results, self.current_day)

        agents_proceeding_tomorrow = self._finish_day(agents_finished_for_today)

        # increase day
        self.current_day += 1

        return agents_proceeding_tomorrow

    def _run_single_step(self, agent: Agent, agents_proceed: list[Agent],
                         agents_finished_for_today: list[Agent]):
        """
        Run single stop for a specific agent - all parameters will be mutated in this method!

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
                        print("error!")

                # run state update - step hooks have to be called in this method
                agent.state = sim_step.update_state(self.config, self.context, agent, next_leg)

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
            # proceed agent to new hub

            # add hub and vertex history (this will add the vertex to the agent's history)
            agent.create_route_data(agent.this_hub, agent.next_hub, agent.route_key, start_time, end_time, agent.state.time_for_legs)

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
    def _agent_finish(agent: Agent, agents_finished_for_today: list[Agent]):
        agent.this_hub = agent.next_hub
        agent.next_hub = ''
        agent.route_key = ''
        agent.is_finished = True
        agents_finished_for_today.append(agent)


    def _agent_proceed(self, agent: Agent, agents_proceed: list[Agent], agents_finished_for_today: list[Agent]):
        # if we deal with overnight tracebacks, we want to remember the last possible resting place and time
        if self.config.overnight_trace_back:
            # get some data about the hub that was just reached
            reached_hub = self.context.graph.vs.find(name=agent.next_hub)
            has_overnight_hub = 'overnight_hub' in reached_hub.attribute_names()

            # save last possible resting place and time, if new hub is an overnight stay
            if reached_hub['overnight'] or (has_overnight_hub and reached_hub['overnight_hub']):
                # mark hub as overnight hub
                agent.route_data.vs.find(name=agent.next_hub)['overnight'] = True

        # add current hub to visited ones
        agent.visited_hubs.add(agent.next_hub)
        # update current hub
        agent.this_hub = agent.next_hub

        # add to list of agents to proceed
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

        # if tries exceeded, move agent to cancelled list
        if agent.tries > self.config.break_simulation_after:
            agent.is_cancelled = True
            self.results.add_agent(agent) # this will keep the state, so we can use it in the results later
        else:
            # reset forced route data
            agent.forced_route = []

            # traceback to last possible resting place, if needed
            if self.config.overnight_trace_back and self.context.graph.vs.find(name=agent.this_hub)['overnight'] is not True:
                # get hub ids that start from the last possible resting place to the current hub
                hubs_to_delete: set[int] = set()

                # iterate over all vertices to find last resting place (since our graph is only a list, we can use dfs, might be a bit faster)
                for v in agent.route_data.dfsiter(agent.route_data.vs.find(agent.this_hub).index, mode='in'):
                    # search for first overnight hub
                    if v['overnight']:
                        # ok, this is our resting place, delete departure time
                        v['departure'] = None
                        agent.current_time = v['arrival']
                        agent.this_hub = v['name']
                        break

                    # check route to this vertex
                    in_edges = v.in_edges()
                    if len(in_edges) == 0:
                        # if we are back at the start of the day, stop, we start with the starting hub again...
                        # TODO: should we add this to the failed agents list?
                        # print('cancelled')
                        # agent.is_cancelled = True
                        # self.results.add_agent(agent)  # this will keep the state, so we can use it in the results later
                        break

                    # mark index for deletion
                    hubs_to_delete.add(v.index)
                    # prepend forced route
                    agent.forced_route.insert(0, in_edges[0]['name'])
                    # delete from visited hubs
                    agent.visited_hubs.remove(v['name'])

                # actually delete hubs from graph
                agent.route_data.delete_vertices(list(hubs_to_delete))

                # delete rest history that is more than or same as the maximum last resting time
                for i in range(len(agent.rest_history)):
                    if agent.rest_history[i][0] >= agent.current_time:
                        agent.rest_history = agent.rest_history[:i]
                        break

                # # decrease tries a bit
                # agent.tries -= 1

            if not agent.is_cancelled:
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

        for edge in self.context.routes.incident(agent.this_hub, mode='out'):
            e = self.context.routes.es[edge]
            route_name = e['name']
            target_hub = e.target_vertex['name']

            # do we have a forced route?
            if len(agent.forced_route) > 0:
                # skip if names do not match
                if route_name != agent.forced_route[0]:
                    continue
                # if forced route is defined, shorten in for the next step
                agent.forced_route = agent.forced_route[1:]

            # Does the target exist in our route data? If yes, skip, we will not visit the same place twice!
            if target_hub in agent.visited_hubs:
                continue

            # add target hub and route name to possible routes
            possible_routes.append((route_name, target_hub,))

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

        # if no possible routes, we can't move forward'
        if len(possible_routes) == 0:
            # add to failed routes
            agent.is_cancelled = True
            return [], [agent]

        # contains routes: clone agent for each possible route
        agents: list[Agent] = []

        for i, (route_name, target_hub) in enumerate(possible_routes):
            # first route - use original agent
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
        # aggregate results into groups, because it is likely we will have multiple agents finishing on the same hub
        agents_proceeding_tomorrow: dict[tuple[str, str], Agent] = {}

        for agent in agents:
            # move finished and cancelled agents to appropriate lists
            if agent.is_finished or agent.is_cancelled:
                # add to global list
                self.results.add_agent(agent)
                continue

            # now we will see, if we can split this agent
            possible_routes = self._get_possible_routes_for_agent_on_hub(agent)
            # if no possible routes, we can't move forward'
            if len(possible_routes) == 0:
                # add to failed routes
                agent.is_cancelled = True
                self.results.add_agent(agent)
                continue

            # if only one possible route, we (hopefully) continue with this agent
            if len(possible_routes) == 1:
                # does next agent exist already in the map?
                if possible_routes[0] in agents_proceeding_tomorrow:
                    # retire this agent
                    self.results.add_agent(agent)
                    # set agent as parent
                    agents_proceeding_tomorrow[possible_routes[0]].parents.append(agent.uid)
                else:
                    # does not exist: continue with this agent
                    # set new route
                    agent.route_key = possible_routes[0][0]
                    agent.next_hub = possible_routes[0][1]

                    agents_proceeding_tomorrow[possible_routes[0]] = agent
            else:
                # multiple routes, retire this agent and create new ones
                self.results.add_agent(agent)

                for possible_route in possible_routes:
                    if possible_route in agents_proceeding_tomorrow:
                        # next agent in this direction exists already in the map?
                        # just set agent as parent
                        agents_proceeding_tomorrow[possible_route].parents.append(agent.uid)
                    else:
                        # create new agent, copy it and create new uid
                        new_agent = copy.deepcopy(agent)
                        new_agent.generate_uid()

                        # set new targets
                        new_agent.next_hub = possible_route[1]
                        new_agent.route_key = possible_route[0]

                        agents_proceeding_tomorrow[possible_route] = new_agent

        return list(agents_proceeding_tomorrow.values())

    def _end_simulation(self):
        """
        Run end simluation tasks
        """
        max_time: float = 0
        min_time: float = float('inf')

        # determine boundaries
        for agent in self.results.agents.vs['agent']:
            if agent.current_time > max_time:
                max_time = agent.current_time
            if agent.current_time < min_time:
                min_time = agent.current_time

        self.results.max_dt = max_time
        self.results.min_dt = min_time


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
