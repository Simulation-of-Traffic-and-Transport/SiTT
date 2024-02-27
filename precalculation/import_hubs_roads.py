# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import hubs and roads from another database."""

from urllib import parse
import igraph as ig
import pyproj
import shapely.ops as sp_ops
from extremitypathfinder import PolygonEnvironment
from geoalchemy2 import Geometry
from pyproj import Transformer
from shapely import wkb, get_parts, prepare, destroy_prepared, is_ccw, \
    delaunay_triangles, contains, overlaps, intersection, STRtree, LineString, Polygon, MultiPolygon, Point, \
    relate_pattern, centroid, shortest_line
from sqlalchemy import Connection, create_engine, Table, Column, literal_column, insert, schema, MetaData, \
    Integer, Boolean, String, Float, select, text, func, delete

# This is an example file - we will interpret data created by archeologists and put it into out sitt schema.

# --------------- settings ---------------
db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'
db_port = 5432
db_name = 'sitt'
# ----------------------------------------


def parse_yes_no_entry(s: str) -> bool:
    """Parse a yes/no entry."""
    v = s.lower()
    if v == 'y' or v == 'yes' or v == 'p':  # p == probably, we take it as true
        return True
    return False


conn = create_engine('postgresql://' + db_user + ':' + parse.quote_plus(db_password) + '@' + db_host + ':' + str(db_port) + '/' + db_name).connect()

# --------------- get hub data from source ---------------
for result in conn.execute(text("SELECT rechubid, geom, harbor, overnight FROM topology.rechubs")):
    # get column data
    hub_id = result[0]
    geom = result[1]
    harbor = parse_yes_no_entry(result[2])
    overnight = parse_yes_no_entry(result[3])
    market = False  # just take fixed value for now

    conn.execute(text(f"INSERT INTO sitt.hubs (id, geom, overnight, harbor, market) VALUES ('{hub_id}', '{geom}', {overnight}, {harbor}, {market});"))

conn.commit()

# -------------- get road data from source ---------------
for result in conn.execute(text("SELECT recroadid, hubaid, hubbid, geom FROM topology.recroads")):
    # get column data
    road_id = result[0]
    hubaid = result[1]
    hubbid = result[2]
    geom = result[3]
    market = False  # just take fixed value for now

    conn.execute(text(f"INSERT INTO sitt.roads (id, geom, hub_id_a, hub_id_b) VALUES ('{road_id}', '{geom}', '{hubaid}', '{hubbid}');"))

conn.commit()
