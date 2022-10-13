"""
Simple runner will have a constant speed and will have a certain slowdown factor for ascending and descending slopes.
Other than that, it does not take into account weather or other factors.
"""
import logging

from sitt import Configuration, Context, SimulationInterface, State, Status, is_truthy

logger = logging.getLogger()


class SimpleRunner(SimulationInterface):
    """
    Simple runner will have a constant speed and will have a certain slowdown factor for ascending and descending
    slopes. Other than that, it does not take into account weather or other factors.
    """

    def __init__(self):
        super().__init__()
        self.speed = 5.0
        """kph of this agent"""
        self.ascend_slowdown_factor = 0.03
        """time taken is modified by slope in degrees multiplied by this number when ascending"""
        self.descend_slowdown_factor = 0.01
        """time taken is modified by slope in degrees multiplied by this number when descending"""

    def run_before(self, config: Configuration, context: Context, state: State) -> State:
        if not self.skip and logger.level <= logging.INFO:
            logger.info(
                state.uid + " SimulationInterface SimpleRunner start day " + str(
                    state.step) + " at " + state.get_current_start_hub())

        return state

    def run(self, config: Configuration, context: Context, state: State) -> State:
        # get current leg id - note to take the step data information
        leg_id = state.step_data.current_leg
        # get path id, a tuple
        path_id = state.path[leg_id]
        # get the start id
        start_hub = state.hubs[leg_id]

        path = context.get_directed_path_by_id(path_id, start_hub)
        if not path:
            logger.error(state.uid + " SimulationInterface SimpleRunner error, path not found ", str(path_id))
            state.status = Status.CANCELLED
            return state

        # create range to traverse
        if path['is_reversed']:
            r = range(len(path['legs']) - 1, -1, -1)
        else:
            r = range(len(path['legs']))

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.

        for i in r:
            length = path['legs'][i]
            slope = path['slopes'][i]
            if path['is_reversed']:
                slope *= -1

            if slope < 0:
                slope_factor = slope * self.descend_slowdown_factor * -1
            else:
                slope_factor = slope * self.ascend_slowdown_factor

            # calculate time taken in units (hours) for this part
            time_taken += length / self.speed / 1000 * (1 + slope_factor)

        # will we advance a step?
        total_time = state.step_data.time_used + time_taken

        # advance one step
        if total_time <= state.step_data.time_available:
            state.step_data.current_leg += 1
            state.step_data.next_leg = state.step_data.current_leg

            if not self.skip and logger.level <= logging.INFO:
                logger.info(
                    f"{state.uid} SimulationInterface SimpleRunner run, advancing {path['length_m'] / 1000:.3f} km to {state.hubs[state.step_data.current_leg]} after {time_taken:.2f} time units (time units up to this step: {total_time:.2f})")
        else:
            logger.info(
                f"{state.uid} SimulationInterface SimpleRunner run, stopping at {state.hubs[state.step_data.current_leg]}, next leg would have taken total of {total_time:.2f} time units.")

            # now, let us see if we have to retrace the steps a bit and return to a previous step
            # get current leg id - note to take the step data information
            leg_id = state.step_data.current_leg
            # get path id, a tuple
            hub_id = state.hubs[leg_id]

            hub = context.get_hub_by_id(hub_id)
            if hub and 'overnight' in hub and not is_truthy(hub['overnight']):
                # can we stay overnight?
                logger.info(
                    state.uid + " SimulationInterface SimpleRunner run_after: cannot stay overnight at " + hub_id + ", retracing...")

                # retracing hubs
                while leg_id >= 0 and not is_truthy(hub['overnight']):
                    leg_id -= 1
                    hub_id = state.hubs[leg_id]
                    hub = context.get_hub_by_id(hub_id)

                logger.info(state.uid + " SimulationInterface SimpleRunner run_after: went back to " + hub_id + ".")

                # resetting state
                state.step_data.current_leg = leg_id
                state.step_data.next_leg = leg_id

        # calculate time taken anyway
        state.step_data.time_used = total_time

        # did we reach our goal?
        if state.step_data.current_leg >= len(state.path):
            state.status = Status.FINISHED

        return state

    def run_after(self, config: Configuration, context: Context, state: State) -> State:
        return state

    def __str__(self):
        return "SimpleRunner"
