# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
SimpleLoadingState will add a fixed number of time to the agent's current time when the route type changes, e.g. from
road to river.
"""
import logging

from sitt import SimulationDefineStateInterface, Configuration, Context, State, Agent

logger = logging.getLogger()

class SimpleLoadingState(SimulationDefineStateInterface):
    """
    SimpleLoadingState will add a fixed number of time to the agent's current time when the route type changes, e.g.
    from road to river.
    """
    def __init__(self, add_time: float = 0.5):
        """
        Initialize the SimpleLoadingState module.

        This constructor sets up the loading state handler that adds a fixed time penalty
        when an agent transitions between different route types (e.g., from road to river).

        Args:
            add_time (float, optional): The number of hours to add to the agent's current time
                when the route type changes. Defaults to 0.5 hours.

        Returns:
            None
        """
        super().__init__()
        self.add_time: float = add_time
        """add this number of hours when route type changes"""

    def define_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        """
        Define the state for an agent, adding loading/unloading time when route types change.

        This method checks if the agent is transitioning between different route types (e.g., from
        road to river) and adds a fixed time penalty to account for loading and unloading goods
        when switching between vehicle types. If no route type change occurs, the agent's state
        is returned unchanged.

        Args:
            config (Configuration): The simulation configuration object containing global settings
                and parameters for the simulation run.
            context (Context): The simulation context providing access to route and path information,
                including route types and other contextual data.
            agent (Agent): The agent whose state is being defined. The agent contains information
                about current and previous routes, timing, and travel state.

        Returns:
            State: The agent's state, either unchanged if no route type transition occurred, or
                after applying the loading/unloading time penalty if the route type changed.
        """
        state = agent.state

        # ignore, if there is no last route
        if agent.last_route is None or agent.last_route == '':
            return state

        # get route types
        last_route_type = context.get_path_by_id(agent.last_route)['type']
        if agent.is_finished:
            print("--- " + agent.route_key, agent.this_hub, "to", agent.next_hub, "---")
        current_route_type = context.get_path_by_id(agent.route_key)['type']

        # types are not equal => add time to consider reloading goods from one vehicle type to another
        if last_route_type != current_route_type:
            # add rest point
            agent.add_rest(self.add_time, reason='loading/unloading')

            # calculate new time
            new_time = agent.current_time + self.add_time
            # update current time
            agent.current_time = new_time

            if not self.skip and logger.level <= logging.DEBUG:
                logger.debug(f"SimpleLoadingState: reloading due to route type change, new time = {agent.current_time:.2f} hours")

        return state