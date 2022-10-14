"""Dummy module for testing"""
import logging

import yaml

from sitt import Configuration, Context, SimulationInterface, State

logger = logging.getLogger()


class Dummy(SimulationInterface):
    """Dummy class for testing - this is an empty class that can be taken as template for custom modules."""

    def __init__(self, test: str = 'Default value'):
        super().__init__()
        self.test: str = test

    def run_before(self, config: Configuration, context: Context, state: State) -> State:
        if not self.skip and logger.level <= logging.INFO:
            logger.info(
                state.uid + " SimulationInterface Dummy run_before: " + self.test)

        return state

    def run(self, config: Configuration, context: Context, state: State) -> State:
        if not self.skip and logger.level <= logging.INFO:
            logger.info(
                state.uid + " SimulationInterface Dummy run: " + self.test)

        return state

    def run_after(self, config: Configuration, context: Context, state: State) -> State:
        if not self.skip and logger.level <= logging.INFO:
            logger.info(
                state.uid + " SimulationInterface Dummy run_after: " + self.test)

        return state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "Dummy: " + self.test
