# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is a variation if the SimpleDAV stepper including river flow for downstream movement. It works exactly the same as
the SimpleDAV stepper, but if a river section downstream is faster than the minimum speed, we use the river's flow for
downstream movement.
"""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, SimulationStepInterface, State, Agent

logger = logging.getLogger()


class SimpleDAVRiver(SimulationStepInterface):
    """
    This is a variation if the SimpleDAV stepper including river flow for downstream movement. It works exactly the same
    as the SimpleDAV stepper, but if a river section downstream is faster than the minimum speed, we use the river's
    flow for downstream movement.
    """

    def __init__(self, speed: float = 4.0, ascend_per_hour: float = 300, descend_per_hour: float = 400,
                 min_speed_down: float = 4.0, consider_sailing: bool = False, sailing_speed_down: float = 3.71,
                 sailing_speed_up: float = 3.71):
        super().__init__()
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_per_hour: float = ascend_per_hour
        """m of height per hour while ascending"""
        self.descend_per_hour: float = descend_per_hour
        """m of height per hour while descending"""
        self.min_speed_down: float = min_speed_down
        """minimum speed per hour at which we do *not* tow downstream (instead we use the river flow)"""
        self.consider_sailing: bool = consider_sailing
        """whether to consider sailing speeds when calculating downstream speed"""
        self.sailing_speed_down: float = sailing_speed_down
        """kph of sailing speed while moving downstream (minimum, can be faster if river is faster)"""
        self.sailing_speed_up: float = sailing_speed_up
        """kph of sailing speed while moving upstream or on a slow-moving river"""

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        """Updates the agent's state after traversing a river leg.

        This method calculates the time it takes for an agent to traverse a given
        river leg. When moving downstream, if the river's flow speed is above a
        minimum threshold, the agent moves with the river's current. Otherwise,
        (e.g., moving upstream or on a slow-moving river), the agent's speed is
        calculated based on a fixed speed, adjusted for ascent or descent, similar
        to hiking.

        The method updates the agent's state with the total time taken for the
        leg and handles stopping the agent if the daily maximum travel time is
        exceeded.

        Args:
            config: The simulation configuration.
            context: The simulation context, containing shared data like the graph.
            agent: The agent that is moving.
            next_leg: The graph edge representing the river leg to be traversed.

        Returns:
            The updated state of the agent after the simulation step.
        """
        # precalculate next hub
        path_id = agent.route_key
        if not next_leg:
            logger.error("SimulationInterface SimpleDAVRiver error, path not found ", str(path_id))
            # state.status = Status.CANCELLED
            return agent.state

        # not a river?
        if next_leg['type'] != 'river':
            logger.error(f"SimulationInterface SimpleRiver error, path {agent.route_key} is not a river")
            agent.state.signal_stop_here = True
            return agent.state

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.
        time_for_legs: list[float] = []

        # create range to traverse - might be reversed
        r = range(len(next_leg['legs']))
        flows = next_leg['flow'].copy()

        if agent.state.is_reversed:
            r = reversed(r)
            flows.reverse()  # also reverse flow

        used_flow = False
        sailing = False
        if self.consider_sailing and 'sailing' in next_leg.attribute_names() and next_leg['sailing']:
            sailing = True

        for i in r:
            coords = next_leg['geom'].coords[i]
            # run hooks
            (time_taken, cancelled) = self.run_hooks(config, context, agent, next_leg, coords, time_taken)
            if cancelled:
                if logger.level <= logging.DEBUG:
                    logger.debug(f"SimulationInterface hooks run, cancelled state")
                return agent.state

            length = next_leg['legs'][i]  # length is in meters

            # now check if river runs downwards
            calculated_time = -1.
            is_downwards = 'direction' in next_leg.attribute_names() and next_leg['direction'] == 'downwards'

            # check sailing speed
            if sailing:
                if is_downwards:
                    calculated_time, used_flow = self.calculate_with_flow(self.sailing_speed_down, flows[i], length)
                else:
                    calculated_time = length / (self.sailing_speed_up * 1000)
            else:
                if is_downwards:
                    calculated_time, used_flow = self.calculate_with_flow(self.min_speed_down, flows[i], length)
                    # reset if flow was not used, so we can calculate time below
                    if not used_flow:
                        calculated_time = -1

                # all other cases -> so upriver, or downriver, if river is too slow
                if calculated_time <= 0:
                    m_asc_desc = next_leg['slopes'][i] * length  # m asc/desc over this length
                    if agent.state.is_reversed:
                        m_asc_desc = -m_asc_desc  # reverse m_asc_desc for descending part

                    if m_asc_desc < 0:
                        up_down_time = (m_asc_desc * -1) / self.descend_per_hour if self.descend_per_hour > 0 else 0.
                    else:
                        up_down_time = m_asc_desc / self.ascend_per_hour if self.ascend_per_hour > 0 else 0.

                    # calculate time taken in units (hours) for this part
                    calculated_time = length / self.speed / 1000 + up_down_time

            time_for_legs.append(calculated_time)
            time_taken += calculated_time

            # check if time taken exceeds max_time - should finish today
            if agent.current_time + time_taken > agent.max_time:
                agent.state.last_coordinate_after_stop = coords
                agent.state.signal_stop_here = True
                break

        # save things in state
        agent.state.time_taken = time_taken

        if config.keep_leg_times:
            agent.state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleDAVRiver run, from {agent.this_hub} to {agent.next_hub} "
                "via {agent.route_key}, time taken = {state.time_taken:.2f}, used flow = {used_flow}")

        return agent.state

    @staticmethod
    def calculate_with_flow(speed: float, flow_ms: float, length: float) -> tuple[float, bool]:
        """Calculates travel time considering river flow and determines if flow was used.

        This method computes the time required to traverse a river segment by comparing
        the agent's base speed with the river's flow speed. It uses the faster of the
        two speeds to calculate the travel time. The method also indicates whether the
        river's flow speed exceeded the agent's base speed.

        Args:
            speed: The base speed of the agent in kilometers per hour (kph).
            flow_ms: The river's flow rate in meters per second (m/s).
            length: The length of the river segment to traverse in meters.

        Returns:
            A tuple containing:
                - float: The time taken to traverse the segment in hours.
                - bool: True if the river flow speed (in kph) was greater than or equal
                  to the agent's base speed, False otherwise.
        """
        # river speed - we take this point's flow rate to calculate the speed
        kph = flow_ms * 3.6
        # take the maximum speed between given speed and river speed
        actual_speed = max(speed, kph)
        # calculate time taken in units (hours) for this part
        return length / (actual_speed * 1000), kph >= speed

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleDAVRiver"
