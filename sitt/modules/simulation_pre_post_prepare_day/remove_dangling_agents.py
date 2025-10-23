# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Removes agents that represent failed paths if a successful alternative path exists.
"""
import logging

from sitt import SimulationPrePostPrepareDayInterface, Configuration, Context, Agent, SetOfResults

logger = logging.getLogger()


class RemoveDanglingAgents(SimulationPrePostPrepareDayInterface):
    """
    Removes agents that represent failed paths if a successful alternative path exists.
    """

    def prepare_for_new_day(self, config: Configuration, context: Context, agents: list[Agent],
                            results: SetOfResults) -> list[Agent]:
        """
        Removes agents that represent failed paths if a successful alternative path exists. This will prevent agents
        from "hanging back" and trying to travel the same path again and again if other agent variants have been
        successful.

        This function is used to prune the list of agents at the beginning of a day.
        It identifies agents that failed on the previous day (tries > 0) and checks
        if another agent that did not fail (tries == 0) provides an alternative route.
        If a successful alternative exists, the "dangling" agent representing the
        failed attempt is removed to avoid re-exploring dead ends.

        :param agents: The list of agents to be pruned.
        :return: A new list of agents with dangling ones removed.
        """
        if len(agents[0].route_data.es) == 0:
            return agents

        kept_agents: list[Agent] = []
        ok_agents: set[str] = set()

        # gather agents that have proceeded in the last day and take their route data
        for agent in agents:
            if agent.tries == 0:
                kept_agents.append(agent)
                for v in agent.route_data.vs:
                    if 'agents' in v.attribute_names():
                        for uid in v['agents']:
                            ok_agents.add(uid)

        # check if this agent has an alternative route
        removed = 0
        for agent in agents:
            if agent.tries > 0:
                has_alternative_route = False
                # get key of this hub - these are all the keys that led to this agent, so we check if it is singular
                for key in agent.route_data.vs.find(name=agent.this_hub)['agents'].keys():
                    if key in ok_agents:
                        has_alternative_route = True
                        break
                if has_alternative_route:
                    removed += 1
                else:
                    kept_agents.append(agent)

        if removed > 0 and logger.level <= logging.DEBUG:
            logger.debug(f"Removed {removed} danging agent(s).")

        return kept_agents
