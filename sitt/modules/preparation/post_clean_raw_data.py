# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Cleaner for raw data, so context is a bit smaller

Example configuration:
preparation:
  - class: PostCleanRawData
    module: preparation_modules.post_clean_raw_data
    args:
      hubs_and_roads: true
      force_gc: false
"""

import gc
import logging

import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class PostCleanRawData(PreparationInterface):
    """Cleaner for raw data, so context is a bit smaller"""
    def __init__(self, hubs_and_roads: bool = True, force_gc: bool = False):
        super().__init__()
        self.hubs_and_roads: bool = hubs_and_roads
        """Clean raw hubs and road data"""
        self.force_gc: bool = force_gc
        """Force garbage collection run"""

    def run(self, config: Configuration, context: Context) -> Context:
        if self.skip:
            logger.info("Skipping PostCleanRawData due to setting")
            return context

        logger.info("Cleaning raw data")

        if self.hubs_and_roads:
            context.raw_roads = None
            context.raw_hubs = None

        # run gc
        if self.force_gc:
            collected = gc.collect()

            if logger.level <= logging.INFO:
                logger.info("Garbage collector: collected %d objects." % collected)

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "PostCleanRawData"
