# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Abstract base class for all PSQL models."""
from abc import ABC
from urllib import parse

import igraph as ig
from geoalchemy2 import Geography
from sqlalchemy import create_engine, select, MetaData, Column, Table, Boolean, String, Float, JSON, Enum
import geopandas as gpd

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
        self.connection: str | None = connection
        self.conn: create_engine | None = None
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

    def get_connection(self) -> create_engine:
        """
        Load or initialize the connection to the database.
        """
        if self.conn is None:
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
        overnight = Column('overnight', Boolean)
        harbor = Column('harbor', Boolean)
        market = Column('market', Boolean)
        data_col = Column('data', JSON)
        return Table("hubs", self.metadata_obj, id_col, geom_col, overnight, harbor, market, data_col,
                     schema=self.schema)

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
        cost_a_b = Column('cost_a_b', Float)
        cost_b_a = Column('cost_b_a', Float)
        data_col = Column('data', JSON)
        return Table("edges", self.metadata_obj, id_col, geom_col, hub_id_a, hub_id_b, edge_type, cost_a_b,
                     cost_b_a, data_col, schema=self.schema)

    def load_graph_from_database(self) -> ig.Graph:
        g: ig.Graph = ig.Graph()

        # create connection
        conn = self.get_connection()

        # load hubs into Geopandas dataframe
        hubs_data = gpd.GeoDataFrame.from_postgis(self.get_hubs_table().select(), conn, index_col='id')
        # iterate frame and add vertices to graph
        for hub in hubs_data.itertuples():
            attrs = {"name": hub.Index, "geom": hub.geom, "overnight": hub.overnight, "harbor": hub.harbor,
                     "market": hub.market}
            if hub.data is not None:
                for key, value in hub.data.items():
                    attrs[key] = value
            g.add_vertex(**attrs)

        # load edges into Geopandas dataframe
        edges_data = gpd.GeoDataFrame.from_postgis(self.get_edges_table().select(), conn, index_col='id')
        # iterate frame and add edges to graph
        for edge in edges_data.itertuples():
            attrs = {"name": edge.Index, "geom": edge.geom, "type": edge.type, "cost_a_b": edge.cost_a_b,
                     "cost_b_a": edge.cost_b_a, "from": edge.hub_id_a, "to": edge.hub_id_b}
            if edge.data is not None:
                for key, value in edge.data.items():
                    attrs[key] = value
            g.add_edge(edge.hub_id_a, edge.hub_id_b, **attrs)

        # close connection
        conn.close()

        return g
