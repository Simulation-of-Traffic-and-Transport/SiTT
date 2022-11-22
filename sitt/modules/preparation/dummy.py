# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Dummy module for testing"""
import logging

import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class Dummy(PreparationInterface):
    """Dummy class for testing - this is an empty class that can be taken as template for custom modules."""

    def __init__(self, test: str = 'Default value'):
        super().__init__()
        self.test: str = test

    def run(self, config: Configuration, context: Context) -> Context:
        if not self.skip:
            logger.info("PreparationInterface Dummy run: " + self.test)

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "Dummy: " + self.test
