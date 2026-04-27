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

More information: https://www.alpenverein.de/files/DAV_Gehzeitrechner_acrobat_8_1_19002.pdf

Moreover, this stepper will not care for the type of path (river, etc.).
Other than that, it does not take into account weather or other factors.
"""
import logging

import igraph as ig
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
        """
        Initialize a SimpleDAV simulation stepper with DAV formula parameters.

        This constructor sets up the parameters used in the DAV (Deutscher Alpenverein) formula
        for calculating travel times based on horizontal distance, elevation gain, and elevation loss.
        The default values reflect the standard DAV formula: 4 km/h horizontal speed, 300 m/h ascending
        rate, and 400 m/h descending rate.

        Args:
            speed (float, optional): The horizontal travel speed in kilometers per hour (km/h).
                Defaults to 4.0 km/h, which is the standard DAV formula value for flat terrain.
            ascend_per_hour (float, optional): The rate of elevation gain in meters per hour (m/h)
                when ascending. Defaults to 300 m/h, the standard DAV formula value.
            descend_per_hour (float, optional): The rate of elevation loss in meters per hour (m/h)
                when descending. Defaults to 400 m/h, the standard DAV formula value.

        Returns:
            None: This is a constructor method that initializes the instance.
        """
        super().__init__()
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_per_hour: float = ascend_per_hour
        """m of height per hour while ascending"""
        self.descend_per_hour: float = descend_per_hour
        """m of height per hour while descending"""

    def _calculate_time_for_step(self, agent: Agent, next_leg: ig.Edge, i: int, attrs: set) -> float:
        """
        Calculate the time required for an agent to traverse a single step (segment) of a leg using the DAV formula.

        This method computes the time taken based on horizontal distance, elevation gain, and elevation loss
        for a specific segment of a leg. It uses the DAV (Deutscher Alpenverein) formula which accounts for:
        - Horizontal distance at a given speed (default 4 km/h)
        - Ascending elevation at a given rate (default 300 m/h)
        - Descending elevation at a given rate (default 400 m/h)

        The method handles both detailed elevation data (legs_up/legs_down) and slope-based calculations,
        and accounts for reversed travel direction.

        Args:
            agent (Agent): The agent traversing the step. Used to determine if the direction is reversed
                via agent.state.is_reversed.
            next_leg (ig.Edge): The graph edge representing the leg being traversed. Must contain 'legs'
                attribute with distance data, and either 'legs_up'/'legs_down' or 'slopes' for elevation data.
            i (int): The index of the current segment within the leg's list of segments.
            attrs (set): A set of attribute names available on the next_leg edge, used to determine
                which elevation calculation method to use.

        Returns:
            float: The time taken to traverse this step in hours, calculated as the sum of horizontal
                travel time, ascending time, and descending time.
        """
        length = next_leg['legs'][i]  # length is in meters

        # create up and down meters
        # if we have detailed legs up and down
        if 'legs_up' in attrs and next_leg['legs_up'] is not None:
            up_m = next_leg['legs_up'][i]  # m uphill over this length
            down_m = next_leg['legs_down'][i]  # m downhill over this length
        else:
            # otherwise use slope
            up_m = next_leg['slopes'][i] * length  # m asc/desc over this length
            down_m = 0.
            if agent.state.is_reversed:
                up_m, down_m = down_m, up_m  # reverse up_m and down_m

        if agent.state.is_reversed:
            up_m, down_m = down_m, up_m  # reverse up_m and down_m

        # calculate time taken for this part in hours
        up_time = abs(up_m) / self.ascend_per_hour if self.ascend_per_hour > 0 else 0.
        down_time = abs(down_m) / self.descend_per_hour if self.descend_per_hour > 0 else 0.

        # calculate time taken in units (hours) for this part
        return length / self.speed / 1000 + up_time + down_time

    def _calculate_time_for_leg(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge):
        """
        Calculate the time taken for an agent to traverse a leg of their journey using the DAV formula.

        This method iterates through each segment of the leg, calculating the time required based on
        horizontal distance, elevation gain, and elevation loss. It accounts for reversed travel direction,
        runs hooks at each coordinate, and stops if the agent's maximum time is exceeded.

        The calculated time is stored in the agent's state, along with optional per-segment timing data.

        Args:
            config (Configuration): The simulation configuration object containing settings such as
                whether to keep individual leg times.
            context (Context): The current simulation context providing environmental and state information.
            agent (Agent): The agent traversing the leg. The agent's state is updated with timing information,
                stop signals, and last coordinates if the maximum time is exceeded.
            next_leg (ig.Edge): The graph edge representing the leg to traverse. Must contain 'legs',
                'legs_up', and 'legs_down' attributes for distance and elevation data, and optionally
                'leg_points' or 'geom' for coordinate information.

        Returns:
            None: This method modifies the agent's state in place and does not return a value.
        """
        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.
        time_for_legs: list[float] = []

        # create range to traverse - might be reversed
        r = range(len(next_leg['legs']))
        if agent.state.is_reversed:
            r = reversed(r)

        # some definitions for leg points and other attributes
        attrs = set(next_leg.attribute_names())
        leg_coords = next_leg['leg_points'] if 'leg_points' in attrs and next_leg['leg_points'] is not None else next_leg['geom'].coords

        for i in r:
            coords = leg_coords[i]
            # run hooks
            (time_taken, cancelled) = self.run_hooks(config, context, agent, next_leg, i, coords, time_taken)
            if cancelled:
                if logger.level <= logging.DEBUG:
                    logger.debug(f"SimulationInterface hooks run, cancelled state")
                return

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
                f"SimulationInterface SimpleDAV run, from {agent.this_hub} to {agent.next_hub} "
                "via {agent.route_key}, time taken = {state.time_taken:.2f}")


    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        """
        Update the agent's state by calculating travel time for the next leg of their journey.

        This method serves as the main entry point for updating an agent's state during simulation.
        It validates that a valid leg exists for the agent's current route, then delegates to the
        time calculation method to determine how long the leg will take using the DAV formula.
        If no valid leg is found, an error is logged but the agent's state is still returned.

        Args:
            config (Configuration): The simulation configuration object containing settings such as
                whether to keep individual leg times and other simulation parameters.
            context (Context): The current simulation context providing environmental and state
                information for the simulation run.
            agent (Agent): The agent whose state is being updated. Contains the current position,
                route information, and state that will be modified with timing calculations.
            next_leg (ig.Edge): The graph edge representing the next leg of the journey to traverse.
                Should contain 'legs', elevation data ('legs_up'/'legs_down' or 'slopes'), and
                coordinate information ('leg_points' or 'geom'). May be None if no valid path exists.

        Returns:
            State: The updated state of the agent after calculating travel time for the leg.
                The state will contain timing information if a valid leg was processed, or remain
                unchanged if no valid leg was found.
        """
        # precalculate next hub
        path_id = agent.route_key
        if not next_leg:
            logger.error("SimulationInterface SimpleDAV error, path not found ", str(path_id))
            # state.status = Status.CANCELLED
        else:
            self._calculate_time_for_leg(config, context, agent, next_leg)

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleDAV"
