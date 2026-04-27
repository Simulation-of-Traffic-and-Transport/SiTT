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

from sitt import Configuration, Context, State, Agent
from sitt.modules.simulation_step import SimpleDAV

logger = logging.getLogger()


class SimpleDAVRiver(SimpleDAV):
    """
    This is a variation if the SimpleDAV stepper including river flow for downstream movement. It works exactly the same
    as the SimpleDAV stepper, but if a river section downstream is faster than the minimum speed, we use the river's
    flow for downstream movement.
    """

    def __init__(self, speed: float = 4.0, ascend_per_hour: float = 300, descend_per_hour: float = 400,
                 min_speed_down: float = 4.0, consider_sailing: bool = False, sailing_speed_down: float = 3.71,
                 sailing_speed_up: float = 3.71):
        """Initializes a SimpleDAVRiver simulation stepper with river flow and optional sailing parameters.

        This constructor extends the SimpleDAV stepper by adding river flow considerations
        for downstream movement. When moving downstream, if the river's flow speed exceeds
        the minimum speed threshold, the agent uses the river's current instead of towing.
        Optionally, sailing speeds can be considered for both upstream and downstream movement.

        Args:
            speed: The base walking/towing speed of the agent in kilometers per hour (kph).
                Defaults to 4.0 kph.
            ascend_per_hour: The maximum vertical ascent rate in meters per hour when moving
                uphill. Defaults to 300 meters/hour.
            descend_per_hour: The maximum vertical descent rate in meters per hour when moving
                downhill. Defaults to 400 meters/hour.
            min_speed_down: The minimum speed threshold in kph below which the agent will not
                tow downstream and will instead use the river's flow. Defaults to 4.0 kph.
            consider_sailing: Whether to use sailing speeds instead of base speeds when
                calculating travel times. Defaults to False.
            sailing_speed_down: The sailing speed in kph when moving downstream. The actual
                speed used will be the maximum of this value and the river's flow speed.
                Defaults to 3.71 kph.
            sailing_speed_up: The sailing speed in kph when moving upstream or on slow-moving
                rivers. Defaults to 3.71 kph.
        """
        super().__init__(speed, ascend_per_hour, descend_per_hour)
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

        The method handles both sailing and non-sailing scenarios. When sailing is
        enabled and the river leg supports it, sailing speeds are used instead of
        base speeds. The method also processes each segment of the river leg
        individually, running hooks at each coordinate and accumulating travel time.

        The method updates the agent's state with the total time taken for the
        leg and handles stopping the agent if the daily maximum travel time is
        exceeded. If the agent needs to stop mid-leg, the last coordinate before
        stopping is recorded.

        Args:
            config: The simulation configuration object containing settings such as
                whether to keep individual leg times.
            context: The simulation context, containing shared data like the graph
                and other global simulation state.
            agent: The agent that is moving along the river. Contains the agent's
                current state, route information, and timing constraints.
            next_leg: The graph edge representing the river leg to be traversed.
                Must have type 'river' and contain attributes such as 'legs' (segment
                lengths), 'flow' (flow rates), 'geom' (coordinates), and optionally
                'direction' and 'sailing'.

        Returns:
            State: The updated state of the agent after the simulation step. The state
                includes the total time taken, optionally the time for each individual
                leg segment, and signals if the agent should stop (e.g., if maximum
                daily travel time is exceeded or if the path is invalid).
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
            # temporarily add propulsion to the agent's state
            agent.additional_data['propulsion'] = 'sailing'

        attrs = next_leg.attribute_names()

        for i in r:
            coords = next_leg['geom'].coords[i]
            # run hooks
            (time_taken, cancelled) = self.run_hooks(config, context, agent, next_leg, i, coords, time_taken)
            if cancelled:
                if logger.level <= logging.DEBUG:
                    logger.debug(f"SimulationInterface hooks run, cancelled state")
                return agent.state

            length = next_leg['legs'][i]  # length is in meters

            # now check if river runs downwards
            calculated_time = -1.
            is_downwards = 'direction' in attrs and next_leg['direction'] == 'downwards'

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
                    # apply DAV formula
                    calculated_time = self._calculate_time_for_step(agent, next_leg, i, attrs)

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

        # delete temporary propulsion data
        if sailing:
            del agent.additional_data['propulsion']

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
