"""
Simple stepper will have a constant speed and will have a certain slowdown factor for ascending and descending slopes.
Other than that, it does not take into account weather or other factors.
"""
import logging

import yaml

from sitt import Configuration, Context, SimulationStepInterface, State, Agent, is_truthy

logger = logging.getLogger()


class Simple(SimulationStepInterface):
    """
    Simple stepper will have a constant speed and will have a certain slowdown factor for ascending and descending slopes.
    Other than that, it does not take into account weather or other factors.
    """

    def __init__(self, speed: float = 5.0, ascend_slowdown_factor: float = 0.05, descend_slowdown_factor: float = 0.025):
        super().__init__()
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_slowdown_factor: float = ascend_slowdown_factor
        """time taken is modified by slope in degrees multiplied by this number when ascending"""
        self.descend_slowdown_factor: float = descend_slowdown_factor
        """time taken is modified by slope in degrees multiplied by this number when descending"""

    def update_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        # precalculate next hub
        path_id = (agent.this_hub, agent.next_hub, agent.route_key)
        leg = context.get_directed_path_by_id(path_id, agent.route_key)
        if not leg:
            logger.error( "SimulationInterface SimpleRunner error, path not found ", str(path_id))
            # state.status = Status.CANCELLED
            return agent.state

        # create range to traverse
        if leg['is_reversed']:
            r = range(len(leg['legs']) - 1, -1, -1)
        else:
            r = range(len(leg['legs']))

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.

        for i in r:
            length = leg['legs'][i]
            slope = leg['slopes'][i]
            if leg['is_reversed']:
                slope *= -1

            if slope < 0:
                slope_factor = slope * self.descend_slowdown_factor * -1
            else:
                slope_factor = slope * self.ascend_slowdown_factor

            # calculate time taken in units (hours) for this part
            time_taken += length / self.speed / 1000 * (1 + slope_factor)

        agent.state.time_taken = time_taken

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface Simple run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "Simple"