# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Save graph to a file"""

import logging
import pickle

import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class GraphSave(PreparationInterface):
    """Save graph to a file"""

    def __init__(self, filename: str = 'saved_graph.pkl'):
        super().__init__()
        self.filename: str = filename

    def run(self, config: Configuration, context: Context) -> Context:
        if context.graph:
            if logger.level <= logging.INFO:
                logger.info(
                    "Saving graph to: " + self.filename)

            file = open(self.filename, 'wb')

            pickle.dump(context.graph, file)

            file.close()
        else:
            logger.info("GraphSave: Not saving graph, because it does not exist.")

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'GraphSave'
