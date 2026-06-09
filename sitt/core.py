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
        """
        Determines whether a given module should be skipped based on its attributes and conditions.

        This function evaluates the module's `skip` attribute, if present, and the conditions
        defined in its `conditions` attribute. If the `skip` attribute is set to True, the module
        is skipped. If conditions exist, they are evaluated, and the module is skipped if any
        condition is unmet.

        :param module: An object representing the module under evaluation.
        :param context: A context object to provide additional information for evaluating conditions.
        :return: A boolean indicating whether the module should be skipped.
        :rtype: bool
        """
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
        """
        Evaluates a condition based on a given key and associated data.

        This method validates a specific condition by interpreting the `key` parameter
        and applying the associated logic. The validation may check the existence of
        a file, the presence of data in an object, or specific boolean values. If the key
        is not recognized, the method logs a warning.

        :param key: The condition key indicating the type of validation to be performed.
        :param condition: A descriptive string for the condition being evaluated.
        :param data: The data or context required for condition evaluation. This can
            include file paths, objects, or other parameters depending on the key.
        :param module: The module where the condition evaluation takes place. Used for
            context and error logging.
        :param context: Additional execution context. Defaults to None.
        :return: A boolean indicating whether the condition was successfully validated.
        :rtype: bool
        """
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
        """
        Resolves and returns an instance or object based on the provided name, optionally considering
        module and context for the resolution. If the name matches predefined keys, the corresponding
        object is returned.

        :param name: The identifier used to determine which object to return.
        :type name: str
        :param module: The module object to be evaluated when the name is 'module'.
        :type module: object
        :param context: The context object to be returned when the name is 'context'.
        :type context: Context
        :return: The resolved object based on the name or None if no match is found.
        :rtype: object | None
        """
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
        """
        Checks the simulation configuration and context for validity.

        This method verifies the presence of required simulation start hubs,
        end hubs, and defined routes. Logs errors if any of these configurations
        are missing. Informational logs are emitted when the logger level is set
        to `INFO` or lower.

        :return: Returns a boolean value indicating if the simulation configuration
            and context are valid.
        :rtype: bool
        """
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
        Executes the main simulation loop for a specified model, managing the initialization,
        execution, and finalization stages of the simulation. The process involves preparing
        initial states, iterating through simulation days, and calculating statistics or history
        once the simulation concludes. **This is the entry point to perform the core simulation.**

        :return: Aggregate results of the simulation execution.
        :rtype: SetOfResults
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
        """
        Initializes the simulation by setting up the simulation state and generating
        a list of agents to execute the simulation. This function sets the starting
        simulation day, iterates over the defined starting hubs, and creates agents
        based on available transport routes and means of transport.

        :return: A list of `Agent` instances representing the initial set of agents
                for the simulation.
        :rtype: list[Agent]
        """
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
        Executes the simulation logic for a **single day** (outer loop), managing agent states, executing preparation
        steps, day hooks, and logging progress at each step of the day. The function iterates through agents and
        handles processing until they complete their tasks for the day. At the end, it determines which
        agents will proceed to the next day and increments the simulation day counter.

        :param agents:
            A list of active agents for the current day. Each agent represents an independent
            entity in the simulation, capable of performing tasks and transitioning
            through various states as part of the simulation.
        :returns:
            A list of agents that will continue to the next simulation day.
        :rtype:
            list[Agent]
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
            hubs = set()
            for agent in agents:
                hubs.add(agent.this_hub)

            logger.info(f"Running day {self.current_day} with {len(agents)} active agent(s) from {len(hubs)} hub(s).")
            logger.info("Hubs: " + ", ".join(hubs))

        step = 1
        # do a single day loop - this is the inner loop for the simulation (per day)
        while len(agents):
            agents_proceed: list[Agent] = []
            """keeps list of agents that proceed today"""

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
        Executes a single simulation step for a given agent. It calculates the agent's current state, determines
        the next route based on agent conditions, applies any relevant simulation hooks for state and step updates,
        and determines whether the agent proceeds, ends the day, or completes the simulation.
        **All parameters will be mutated in this method!**

        :param agent: An instance of Agent, representing the entity for which the simulation step
            is being executed.
        :param agents_proceed: A list containing agents that are ready to proceed to the next hub.
        :param agents_finished_for_today: A list containing agents that have completed their actions
            for the current simulation day.
        :return: None
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
    def _agent_finish(agent: Agent, agents_finished_for_today: list[Agent]):
        """
        Marks an agent as finished for the day by updating its state and adding it to the list
        of finished agents. The method updates the agent's visited hubs, resets its route-related
        attributes, sets its status to finished, and appends it to the list of agents that have
        completed their activities for the day.

        :param agent: The agent to mark as finished.
        :type agent: Agent
        :param agents_finished_for_today: The list of agents that have finished their tasks
            for the day. The given agent will be appended to this list.
        :type agents_finished_for_today: list[Agent]
        """
        agent.visited_hubs.add(agent.next_hub)
        agent.this_hub = agent.next_hub
        agent.next_hub = ''
        agent.route_key = ''
        agent.is_finished = True
        # set arrival time
        agents_finished_for_today.append(agent)


    def _agent_proceed(self, agent: Agent, agents_proceed: list[Agent], agents_finished_for_today: list[Agent]):
        """
        Handles the processing of an agent reaching a hub and performs necessary updates related
        to routing and overnight tracking. The method determines if the agent has reached an
        overnight hub, updates the last possible resting place, manages visited hubs, and adds
        the agent to appropriate processing lists.

        :param agent: The agent being processed.
        :type agent: Agent
        :param agents_proceed: A list to collect agents that are ready to proceed further in their route.
        :type agents_proceed: list[Agent]
        :param agents_finished_for_today: A list to collect agents whose processing for the day is complete.
        :type agents_finished_for_today: list[Agent]
        :return: None
        """
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

        # add to the list of agents to proceed
        agents_ok, agents_cancelled = self._split_agent_on_hub(agent)
        agents_proceed.extend(agents_ok)
        agents_finished_for_today.extend(agents_cancelled)

    def _agent_end_day(self, agent: Agent, agents_finished_for_today: list[Agent]):
        """
        End the simulation day for a given agent by updating its state and taking necessary actions
        like handling retries, cancellations, and route tracebacks.

        :param agent: Agent object for which the day's simulation is being ended
        :param agents_finished_for_today: List of Agent objects that have completed their day
        :return: None
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
        """
        Processes a day's end for agents, grouping them by hubs, checking if their journey should end,
        and determining the agents to proceed to the next day.

        :param agents: List of Agent objects to be processed at the end of the simulation day.
        :type agents: list[Agent]
        :return: A list of Agent objects that are assigned to proceed to the next day of the simulation.
        :rtype: list[Agent]
        """
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
            agent_ids = set()
            for agent in agent_list:
                if not agent.is_cancelled:
                    agent_ids.add(agent.uid)
            agent = Agent(hub, '', '', do_not_generate_uid=True)
            has_agents_to_proceed = False

            for agent in agent_list:
                if agent.is_cancelled or agent.is_finished:
                    if self.config.keep_agent_data_in_results:
                        self.results.add_agent(agent)
                else:
                    has_agents_to_proceed = True

                    agent.visited_hubs.update(agent.visited_hubs)
                    agent_ids.add(agent.uid)

            if has_agents_to_proceed:
                means_of_transport = self.config.means_of_transport if len(self.config.means_of_transport) > 0 else [None]

                # get all possible routes for this hub
                for route in self._get_possible_routes_for_agent_on_hub(agent):
                    for mean_of_transport in means_of_transport:
                        new_agent = Agent(hub, route[1], route[0])
                        new_agent.visited_hubs = copy.deepcopy(agent.visited_hubs)
                        new_agent.parents = copy.deepcopy(list(agent_ids))
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
