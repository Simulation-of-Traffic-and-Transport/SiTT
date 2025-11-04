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
        self.results = SetOfResults(self.context.routes)
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

    def create_agents_on_node(self, hub: str, agent_to_clone: Agent | None = None, first_day: bool = False,
                              current_time: float = 8., max_time: float = 16.) -> list[Agent]:
        """
        Create a number of virtual agents on a given node.

        :param hub: Hub to create agents on.
        :param agent_to_clone: Clone this agent.
        :param first_day: First day of simulation?
        :param current_time: Current time
        :param max_time: Maximum time this day
        :return:
        """
        agents: list[Agent] = []
        is_dummy_agent = False

        # create new agent if none is defined
        if agent_to_clone is None:
            agent_to_clone = Agent(hub, '', '', current_time=current_time, max_time=max_time)
            is_dummy_agent = True

        # add current hub to visited ones
        agent_to_clone.visited_hubs.add(hub)
        # set current hub
        agent_to_clone.this_hub = hub

        # create new agent for each outbound edge
        for edge in self.context.routes.incident(hub):
            e = self.context.routes.es[edge]
            target = e.target_vertex['name']

            # do we have a forced route?
            if len(agent_to_clone.forced_route) > 0:
                # skip if names do not match
                if e['name'] != agent_to_clone.forced_route[0]:
                    continue
                # ok, forced route is defined, shorten in for the next step
                agent_to_clone.forced_route = agent_to_clone.forced_route[1:]

            # Does the target exist in our route data? If yes, skip, we will not visit the same place twice!
            if target not in agent_to_clone.visited_hubs:
                # create new agent for each option
                new_agent = copy.deepcopy(agent_to_clone)
                new_agent.route_data = ig.Graph(directed=True)
                new_agent.next_hub = target
                new_agent.route_key = e['name']  # name of edge

                agents.append(new_agent)

        # create new uids, if agents have split
        if len(agents) > 1:
            # save old agent to history
            if not is_dummy_agent:
                self.results.add_agent(agent_to_clone)
            for agent in agents:
                agent.generate_uid()
                # set parent, if not a dummy agent
                if not is_dummy_agent:
                    agent.parent = agent_to_clone.uid  # parent uid

        return agents

    def run(self) -> SetOfResults:
        """
        Run the simulation

        :return: created set of results object
        """
        logger.info("******** Simulation: started ********")

        # check settings
        if not self.check():
            return self.results

        # create initial set of agents to run
        agents = []
        for hub in self.config.simulation_starts:
            agents.extend(self.create_agents_on_node(hub, first_day=True))
        # reset day counter
        self.current_day = 1

        # do the loop - this is the outer loop for the whole simulation
        # it will run until there are no agents left
        while len(agents):
            agents = self._run_single_day(agents)

        # end simulation - do some history and statistics
        self._end_simulation()

        logger.info("******** Simulation: finished ********")

        return self.results

    def _run_single_day(self, agents: list[Agent]) -> list[Agent]:
        """
        Run single day - called in the outer loop of run

        :param agents: list of agents
        :return: new list of agents (can be empty list at the end)
        """
        agents_finished_for_today: list[Agent] = []
        """keeps finished agents for this day"""

        # prepare context for single day - run pre functions
        for pre_prep_day in self.config.pre_simulation_prepare_day:
            agents = pre_prep_day.prepare_for_new_day(self.config, self.context, agents, self.results)

        # prepare context for single day - run for each agent
        for agent in agents:
            agent.prepare_for_new_day(current_day=self.current_day)
            # run SimulationPrepareDayInterfaces
            for prep_day in self.config.simulation_prepare_day:
                prep_day.prepare_for_new_day(self.config, self.context, agent)

        # prepare context for single day - run post functions
        for post_prep_day in self.config.post_simulation_prepare_day:
            agents = post_prep_day.prepare_for_new_day(self.config, self.context, agents, self.results)

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

        # increase day
        self.current_day += 1

        return agents_finished_for_today

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
        # and module calls
        for def_state in self.config.simulation_define_state:
            agent.state = def_state.define_state(self.config, self.context, agent)

        # get the next leg from context
        last_key = agent.route_key
        next_leg: ig.Edge = self.context.get_path_by_id(last_key)

        # run the actual state update loop
        for sim_step in self.config.simulation_step:
            # conditions are met?
            if sim_step.check_conditions(self.config, self.context, agent, next_leg):
                # traverse in reversed order?
                is_reversed = False
                if agent.this_hub != next_leg['from']:
                    is_reversed = True
                    if agent.next_hub != next_leg['from']:
                        print("error!")

                # run state update
                agent.state = sim_step.update_state(self.config, self.context, agent, next_leg, is_reversed)

        # calculate times
        start_time = agent.current_time
        end_time = agent.current_time + agent.state.time_taken

        # proceed or stop here?
        if not agent.state.signal_stop_here and agent.state.time_taken >= 0 and end_time <= agent.max_time:
            # proceed..., first add time
            # add hub and vertex history (this will add the vertex to the agent's history)
            agent.set_hub_departure(agent.this_hub, (agent.current_day, start_time))
            agent.set_hub_arrival(agent.next_hub, (agent.current_day, end_time))
            agent.add_vertex_history(agent.route_key, agent.this_hub, agent.next_hub, self.current_day, start_time, self.current_day, end_time, agent.state.time_for_legs)

            agent.current_time = end_time
            agent.last_route = last_key

            # finished?
            next_hub = self.context.graph.vs.find(name=agent.next_hub)
            has_overnight_hub = 'overnight_hub' in next_hub.attribute_names()

            if agent.next_hub in self.config.simulation_ends:
                agent.this_hub = agent.next_hub
                agent.next_hub = ''
                agent.route_key = ''
                agent.day_finished = self.current_day
                self.results.add_agent(agent)
            elif next_hub['overnight'] or (has_overnight_hub and next_hub['overnight_hub']):
                # proceed to new hub -> it is an overnight stay
                if has_overnight_hub and next_hub['overnight_hub'] and agent.next_hub != next_hub['overnight_hub']:
                    agent.last_possible_resting_place = agent.next_hub
                    agent.last_possible_resting_time = agent.current_time
                else:
                    agent.last_possible_resting_place = agent.next_hub
                    agent.last_possible_resting_time = agent.current_time

                next_hub_agents = self.create_agents_on_node(agent.next_hub, agent)

                if agent.current_time == agent.max_time:
                    agents_finished_for_today.extend(next_hub_agents)
                else:
                    agents_proceed.extend(next_hub_agents)
            else:
                # proceed, but this is not an overnight stay
                if agent.state.signal_stop_here or agent.current_time == agent.max_time:
                    # very special case that should not occur often: we arrive at the node exactly on maximum
                    # time, end day - this will increase test timer
                    self._end_day(agent, agents_finished_for_today)
                else:
                    # normal case just proceed
                    agents_proceed.extend(self.create_agents_on_node(agent.next_hub, agent))
        else:
            # time exceeded, end day
            self._end_day(agent, agents_finished_for_today)

    def _end_day(self, agent: Agent, agents_finished_for_today: list[Agent]):
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
            agent.day_cancelled = self.current_day - self.config.break_simulation_after
            self.results.add_agent(agent)
        else:
            # reset forced route data
            agent.forced_route = []

            # traceback to last possible resting place, if needed
            if self.config.overnight_trace_back and self.context.graph.vs.find(name=agent.this_hub)['overnight'] is not True:
                # get hub ids that start from the last possible resting place to the current hub
                hubs_to_delete: set[int] = set()

                for edge_id in agent.route_day.get_shortest_path(agent.last_possible_resting_place, agent.this_hub, output="epath"):
                    e = agent.route_day.es[edge_id]
                    agent.forced_route.append(e['name'])
                    hubs_to_delete.add(e.target)
                    # delete from visited hubs
                    agent.visited_hubs.remove(e.target_vertex['name'])

                # actually delete hubs from graph
                agent.route_day.delete_vertices(list(hubs_to_delete))

                # delete departure time in target hub
                hub = agent.route_day.vs.find(name=agent.last_possible_resting_place)
                hub['departure'] = None

                # delete rest history that is more than or same as the maximum last resting time
                for i in range(len(agent.rest_history)):
                    if agent.rest_history[i][0] >= agent.last_possible_resting_time:
                        agent.rest_history = agent.rest_history[:i]
                        break

                agents_finished_for_today.extend(
                    self.create_agents_on_node(agent.last_possible_resting_place, agent))
            else:
                agents_finished_for_today.append(agent)

            # set this hub and reset tries
            if agent.last_resting_place != agent.this_hub:
                agent.last_resting_place = agent.this_hub
                agent.tries = 0

        # save to statistics
        self._save_statistics(agent)


    def _save_statistics(self, agent: Agent):
        """Saves the agent's travel statistics to the main results object.

        This method iterates through the agent's personal route data (`agent.route_data`)
        and appends the arrival and departure times for each visited hub (vertex) and
        traversed route (edge) to the corresponding elements in the global `results.route`
        graph. This aggregates the data from a single agent into the overall simulation
        results.

        Args:
            agent (Agent): The agent whose statistics are to be saved.
        """
        # traverse the route
        for v in agent.route_day.vs:
            reason = v['reason'] if v['reason'] is not None else ""

            hub = self.results.route.vs.find(name=v['name'])
            if v['arrival'] is not None:
                hub['arrival'].append((v['arrival'], agent.uid, reason,))
            if v['departure'] is not None:
                hub['departure'].append((v['departure'], agent.uid, reason,))
            # should only be one outbound edge...
            for e in v.incident(mode='out'):
                edge = self.results.route.es.find(name=e['name'])
                edge['arrival'].append((e['arrival'], agent.uid, reason,))
                edge['departure'].append((e['departure'], agent.uid, reason,))
                for i in range(len(edge['leg_times'])):
                    if i == 0:
                        edge['leg_times'][0].append((e['departure'], agent.uid))
                    else:
                        edge['leg_times'][i].append((e['leg_times'][i-1], agent.uid))


    def _end_simulation(self):
        """
        Run end simluation tasks
        """
        max_day: int = 0
        max_time: float = 0
        min_day: int = 9999999
        min_time: float = float('inf')

        # first, determine max_time and max_day
        for agent in self.results.agents.vs['agent']:
            if agent.current_day > max_day:
                max_day = agent.current_day
                max_time = agent.current_time
            elif agent.current_day == max_day and agent.current_time > max_time:
                max_time = agent.current_time

            if agent.current_day < min_day:
                min_day = agent.current_day
                min_time = agent.current_time
            elif agent.current_day == min_day and agent.current_time < min_time:
                min_time = agent.current_time

        self.results.max_dt = (max_day, max_time)
        self.results.min_dt = (min_day, min_time)


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
