# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
SimpleLoadingState will add a fixed amount of time to the agent's current time when the route type changes, e.g. from
road to river.
"""
import logging

from sitt import SimulationDefineStateInterface, Configuration, Context, State, Agent

logger = logging.getLogger()

class SimpleLoadingState(SimulationDefineStateInterface):
    """
    SimpleLoadingState will add a fixed amount of time to the agent's current time when the route type changes, e.g.
    from road to river.
    """
    def __init__(self, add_time: float = 0.5):
        super().__init__()
        self.add_time: float = add_time
        """add this amount of hours when route type changes"""

    def define_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        state = agent.state

        # ignore, if there is no last route
        if agent.last_route is None or agent.last_route == '':
            return state

        # get route types
        last_route_type = context.get_path_by_id(agent.last_route)['type']
        current_route_type = context.get_path_by_id(agent.route_key)['type']

        # types are not equal => add time to consider reloading goods from one vehicle type to another
        if last_route_type != current_route_type:
            # add rest point
            agent.add_rest(self.add_time)

            # calculate new time
            new_time = agent.current_time + self.add_time
            # update history
            agent.set_hub_departure(agent.this_hub, (agent.current_day, new_time), reason='loading/unloading')
            # update current time
            agent.current_time = new_time

            if not self.skip and logger.level <= logging.DEBUG:
                logger.debug(f"SimpleLoadingState: reloading due to route type change, new time = {agent.current_time:.2f} hours")

        return state