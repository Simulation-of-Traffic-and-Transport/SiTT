# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""General conditional module that runs submodules as a group on a certain condition."""
import logging

import yaml

from sitt import BaseClass, Configuration, Context, PreparationInterface

logger = logging.getLogger()


class ConditionalModule(BaseClass, PreparationInterface):
    """General conditional module that runs submodules as a group on a certain condition."""

    def __init__(self, submodules: list[PreparationInterface] = []):
        super().__init__()
        self.submodules: list[PreparationInterface] = submodules

    def run(self, config: Configuration, context: Context) -> Context:
        # set config before run - might be needed for recursive stuff
        self.config = config

        # run modules
        for module in self.submodules:
            if not self.is_skipped(module, context):
                context = module.run(self.config, context)

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'ConditionalModule'

    def __getstate__(self):
        state = self.__dict__.copy()

        # remove config from state on outputs
        if 'config' in state:
            del state['config']

        return state
