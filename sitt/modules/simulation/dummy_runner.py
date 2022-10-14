"""Dummy runner that just advances one step per iteration. Useful for testing."""
import logging

import yaml

from sitt import Configuration, Context, SimulationInterface, State

logger = logging.getLogger()


class DummyRunner(SimulationInterface):
    """Dummy runner that just advances one step per iteration. Useful for testing."""

    def __init__(self):
        super().__init__()

    def run_before(self, config: Configuration, context: Context, state: State) -> State:
        return state

    def run(self, config: Configuration, context: Context, state: State) -> State:
        # advance one step
        state.step_data.next_leg = state.current_leg + 1
        state.step_data.time_used = state.step_data.time_available

        if not self.skip and logger.level <= logging.INFO:
            logger.info(
                state.uid + " SimulationInterface DummyRunner run, advancing to " + state.hubs[state.step_data.next_leg])

        return state

    def run_after(self, config: Configuration, context: Context, state: State) -> State:
        return state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "DummyRunner"
