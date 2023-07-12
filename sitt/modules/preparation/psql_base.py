# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Abstract base class for all PSQL models."""
from abc import ABC
from urllib import parse

from sqlalchemy import create_engine, MetaData

from sitt import PreparationInterface


class PSQLBase(PreparationInterface, ABC):
    """Abstract base class for all PSQL models."""

    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', connection: str | None = None):
        """Initialize the PSQL model."""
        super().__init__()
        self.server = server
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        # runtime settings
        self.connection: str | None = connection
        self.conn: create_engine | None = None
        self.metadata_obj: MetaData = MetaData()

    def _create_connection_string(self, for_printing=False):
        """
        Create DB connection string

        :param for_printing: hide password, so connection can be printed
        """
        if for_printing:
            return 'postgresql://' + self.user + ':***@' + self.server + ':' + str(
                self.port) + '/' + self.db
        else:
            return 'postgresql://' + self.user + ':' + parse.quote_plus(
                self.password) + '@' + self.server + ':' + str(
                self.port) + '/' + self.db
