# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Abstract base class for all PSQL models."""
from abc import ABC
from urllib import parse

import geopandas as gpd
import igraph as ig
from geoalchemy2 import Geography
from sqlalchemy import create_engine, Connection, MetaData, Column, Table, String, Float, Boolean, Enum, Sequence, Date, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB

from sitt import PreparationInterface


class PSQLBase(PreparationInterface, ABC):
    """Abstract base class for all PSQL models."""

    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', schema: str = 'sitt', connection: str | None = None):
        """Initialize the PSQL model."""
        super().__init__()
        self.server = server
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.schema = schema
        # runtime settings
        self.connection: str | None = connection # this is used to automatically load the connection from the config
        self.conn: Connection | None = None
        self.metadata_obj: MetaData = MetaData(schema=self.schema)

    def _create_connection_string(self, for_printing=False):
        """
        Create DB connection string

        :param for_printing: hide password, so connection can be printed
        """
        if for_printing:
            return 'postgresql://' + self.user + ':***@' + self.server + ':' + str(
                self.port) + '/' + self.db + ' (schema:' + self.schema + ')'
        else:
            return 'postgresql://' + self.user + ':' + parse.quote_plus(
                self.password) + '@' + self.server + ':' + str(
                self.port) + '/' + self.db

    def get_connection(self) -> Connection:
        """
        Load or initialize the connection to the database.
        """
        if self.conn is None or self.conn.closed:
            # create connection string and connect to db
            db_string: str = self._create_connection_string()
            self.conn = create_engine(db_string).connect()

        return self.conn

    def get_hubs_table(self) -> Table:
        """Get the hubs (vertices) table."""
        schema_key = self.schema + '.hubs'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        id_col = Column('id', String, primary_key=True)
        geom_col = Column('geom', Geography('POINTZ'))
        data_col = Column('data', JSONB)
        return Table("hubs", self.metadata_obj, id_col, geom_col, data_col, schema=self.schema)

    def get_edges_table(self) -> Table:
        """Get the edges table."""
        schema_key = self.schema + '.edges'
        if schema_key in self.metadata_obj.tables:
            return self.metadata_obj.tables[schema_key]

        id_col = Column('id', String, primary_key=True)
        geom_col = Column('geom', Geography('LINESTRINGZ'))
        hub_id_a = Column('hub_id_a', String)
        hub_id_b = Column('hub_id_b', String)
        edge_type = Column('type', Enum('road', 'river', 'lake'))
        data_col = Column('data', JSONB)
        directions_col = Column('directions', JSONB)
        return Table("edges", self.metadata_obj, id_col, geom_col, hub_id_a, hub_id_b, edge_type, data_col,
                     directions_col, schema=self.schema)

    def load_graph_from_database(self) -> ig.Graph:
        g: ig.Graph = ig.Graph()

        # create connection
        conn = self.get_connection()

        # load hubs into Geopandas dataframe
        hubs_data = gpd.GeoDataFrame.from_postgis(self.get_hubs_table().select(), conn, index_col='id')
        # iterate frame and add vertices to graph
        for hub in hubs_data.itertuples():
            attrs = {"name": hub.Index, "geom": hub.geom}
            if hub.data is not None:
                for key, value in hub.data.items():
                    attrs[key] = value
            g.add_vertex(**attrs)

        # load edges into Geopandas dataframe
        edges_data = gpd.GeoDataFrame.from_postgis(self.get_edges_table().select(), conn, index_col='id')
        # iterate frame and add edges to graph
        for edge in edges_data.itertuples():
            attrs = {"name": edge.Index, "geom": edge.geom, "type": edge.type, "from": edge.hub_id_a, "to": edge.hub_id_b}
            if edge.data is not None:
                for key, value in edge.data.items():
                    attrs[key] = value
            if edge.directions is not None:
                attrs["directions"] = edge.directions
            g.add_edge(edge.hub_id_a, edge.hub_id_b, **attrs)

        # close connection
        conn.close()

        return g
