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
import math

import yaml

from sitt import Configuration, Context, SimulationStepInterface, State, Agent

logger = logging.getLogger()


class SimpleDAV(SimulationStepInterface):
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

    def __init__(self, speed: float = 4.0, ascend_per_hour: float = 300, descend_per_hour: float = 400):
        super().__init__()
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_per_hour: float = ascend_per_hour
        """m of height per hour while ascending"""
        self.descend_per_hour: float = descend_per_hour
        """m of height per hour while descending"""

    def update_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        state = agent.state

        # precalculate next hub
        path_id = agent.route_key
        leg = context.get_path_by_id(path_id)
        if not leg:
            logger.error("SimulationInterface SimpleRunner error, path not found ", str(path_id))
            # state.status = Status.CANCELLED
            return state

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.
        time_for_legs: list[float] = []

        for i in range(len(leg['legs'])):
            length = leg['legs'][i]  # length is in meters
            m_asc_desc = leg['slopes'][i] * length  # m asc/desc over this length

            if m_asc_desc < 0:
                up_down_time = (m_asc_desc * -1) / self.descend_per_hour
            else:
                up_down_time = m_asc_desc / self.ascend_per_hour

            # calculate time taken in units (hours) for this part
            calculated_time = length / self.speed / 1000

            time_for_legs.append(calculated_time + up_down_time)
            time_taken += calculated_time

        # save things in state
        state.time_taken = time_taken
        state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleDAV run, from {agent.this_hub} to {agent.next_hub} "
                "via {agent.route_key}, time taken = {state.time_taken:.2f}")

        return state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleDAV"
