"""Dummy module for testing"""
import logging

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class Dummy(PreparationInterface):
    """Dummy class for testing - this is an empty class that can be taken as template for custom modules."""

    def __init__(self):
        super().__init__()
        self.test: str = 'Default value'

    def run(self, config: Configuration, context: Context) -> Context:
        if not self.skip:
            logger.info("PreparationInterface Dummy run: " + self.test)

        return context

    def __str__(self):
        return "Dummy: " + self.test
