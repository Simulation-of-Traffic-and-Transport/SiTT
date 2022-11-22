# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Load graph from a file"""

import logging
import pickle

import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class GraphLoad(PreparationInterface):
    """Load graph from a file"""

    def __init__(self, filename: str = 'saved_graph.pkl'):
        super().__init__()
        self.filename: str = filename

    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info(
                "Loading graph from: " + self.filename)

        file = open(self.filename, 'rb')

        context.graph = pickle.load(file)

        file.close()

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'GraphLoad'
