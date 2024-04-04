# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Load graph from database or file.

Example configuration:
preparation:
  - class: GraphLoad
    module: modules.preparation
    args:
      filename: 'saved_graph.pkl'
      save: false
      server: !Env "${PSQL_SERVER}"
      port: !Env ${PSQL_PORT}
      db: !Env "${PSQL_DB}"
      user: !Env "${PSQL_USER}"
      password: !Env "${PSQL_PASSWORD}"
      schema: sitt

You can also use the connection setting defined in config.yaml to connect to a common database:
preparation:
  - class: GraphLoad
    module: modules.preparation
    args:
      filename: 'saved_graph.pkl'
      save: false
      connection: psql_default
"""

import logging
import os
import pickle

import yaml

from sitt import Configuration, Context
from sitt.modules.preparation import PSQLBase

logger = logging.getLogger()


class GraphLoad(PSQLBase):
    """Load graph from database or file."""

    def __init__(self, filename: str = 'saved_graph.pkl', save: bool = False, server: str = 'localhost',
                 port: int = 5432, db: str = 'sitt', user: str = 'postgres', password: str = 'postgres',
                 schema: str = 'sitt', connection: str | None = None):
        # connection data - should be set/overwritten by config
        super().__init__(server, port, db, user, password, schema, connection)
        self.filename: str = filename
        self.save: bool = save

    def run(self, config: Configuration, context: Context) -> Context:
        # graph exists in pickled form - load it!
        if self.filename != '' and os.path.exists(self.filename):
            if logger.level <= logging.INFO:
                logger.info(
                    "Loading graph from: " + self.filename)

            file = open(self.filename, 'rb')

            context.graph = pickle.load(file)

            file.close()
        else:
            # load from database
            if logger.level <= logging.INFO:
                logger.info(
                    "Loading graph from: " + self._create_connection_string(True))

            context.graph = self.load_graph_from_database()

            if self.save:
                if logger.level <= logging.INFO:
                    logger.info(
                        "Saving graph to: " + self.filename)

                file = open(self.filename, 'wb')

                pickle.dump(context.graph, file)

                file.close()

        if logger.level <= logging.INFO:
            logger.info("Loaded graph with %d nodes and %d edges." %
                        (context.graph.vcount(), context.graph.ecount()))

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return 'GraphLoad'
