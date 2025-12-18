# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Simple stepper using DAV (Deutscher Alpenverein, German Alpine Association) formula. The formula is based on experience
and used to denote times for trails through the mountains. The formula is pretty simple:

* 4 km of flat terrain = 1h
* 300 m ascending = 1h
* 400 m descending = 1h

Total absolute time taken is km in flat terrain plus total ascension in m plus total descension in m.

More information: https://services.alpenverein.de/Gehzeitrechner/

Moreover, this stepper will not care for the type of path (river, etc.).
Other than that, it does not take into account weather or other factors.
"""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, SimulationStepInterface, State, Agent

logger = logging.getLogger()


class SimpleDAVRiver(SimulationStepInterface):
    """
    Simple stepper using DAV (Deutscher Alpenverein, German Alpine Association) formula. The formula is based on
    experience and used to denote times for trails through the mountains. The formula is pretty simple:

    * 4 km of flat terrain = 1h
    * 300 m ascending = 1h
    * 400 m descending = 1h

    Total absolute time taken is km in flat terrain plus total ascension in m plus total descension in m.

    More information: https://services.alpenverein.de/Gehzeitrechner/

    Moreover, this stepper will not care for the type of path (river, etc.).
    Other than that, it does not take into account weather or other factors.
    """

    def __init__(self, speed: float = 4.0, ascend_per_hour: float = 300, descend_per_hour: float = 400,
                 min_speed_down: float = 4.0):
        super().__init__()
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_per_hour: float = ascend_per_hour
        """m of height per hour while ascending"""
        self.descend_per_hour: float = descend_per_hour
        """m of height per hour while descending"""
        self.min_speed_down: float = min_speed_down
        """minimum speed per hour at which we do *not* tow downstream (instead we use the river flow)"""

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        # precalculate next hub
        path_id = agent.route_key
        if not next_leg:
            logger.error("SimulationInterface SimpleRunner error, path not found ", str(path_id))
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
            if 'direction' in next_leg.attribute_names() and next_leg['direction'] == 'downwards':
                # river speed - we take this and the next point's flow rate to calculate the speed
                kph = (flows[i] + flows[i + 1]) / 2 * 3.6
                if kph >= self.min_speed_down:
                    # calculate time taken in units (hours) for this part
                    calculated_time = length / (kph * 1000)
                    used_flow = True

            # all other cases -> so upriver, or downriver, if river is too slow
            if calculated_time <= 0:
                m_asc_desc = next_leg['slopes'][i] * length  # m asc/desc over this length
                if agent.state.is_reversed:
                    m_asc_desc = -m_asc_desc  # reverse m_asc_desc for descending part

                if m_asc_desc < 0:
                    up_down_time = (m_asc_desc * -1) / self.descend_per_hour
                else:
                    up_down_time = m_asc_desc / self.ascend_per_hour

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
        agent.state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleDAVRiver run, from {agent.this_hub} to {agent.next_hub} "
                "via {agent.route_key}, time taken = {state.time_taken:.2f}, used flow = {used_flow}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleDAVRiver"
