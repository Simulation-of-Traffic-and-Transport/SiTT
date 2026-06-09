# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""General conditional module that runs submodules as a group on a certain condition."""
import logging

import yaml

from sitt import BaseClass, Configuration, Context, PreparationInterface

logger = logging.getLogger()


class ConditionalModule(BaseClass, PreparationInterface):
    """
    Represents a module that conditionally executes a series of submodules based
    on a given configuration and context.

    The class is designed to iterate through a series of submodules, executing
    only those that are not set to be skipped. It allows dynamic configuration
    and state management during the modular execution process, enhancing
    flexibility and reusability in complex workflows.

    :ivar submodules: A list of submodules that implement the `PreparationInterface`.
        Each submodule is processed sequentially unless specified otherwise.
    :type submodules: list[PreparationInterface]
    """

    def __init__(self, submodules: list[PreparationInterface] = []):
        super().__init__()
        self.submodules: list[PreparationInterface] = submodules

    def run(self, config: Configuration, context: Context) -> Context:
        """
        Executes the process involving a series of modules and returns the updated
        context. The method iterates over submodules and invokes their `run` method,
        passing the provided configuration and context. Modules that are marked to
        be skipped are not executed.

        :param config: The configuration object to be utilized during execution.
        :type config: Configuration
        :param context: The context object containing the current state and data.
        :type context: Context
        :return: The updated context after all applicable submodules are executed.
        :rtype: Context
        """
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
