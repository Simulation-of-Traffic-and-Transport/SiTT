# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import hubs and roads from another database."""

from urllib import parse

from sqlalchemy import create_engine, text
from shapely import wkb, LineString

# This is an example file - we will interpret data created by archeologists and put it into out sitt schema.

# --------------- settings ---------------
db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'
db_port = 5432
db_name = 'sitt'
crs_no = 4326


# ----------------------------------------


def parse_yes_no_entry(s: str) -> bool:
    """Parse a yes/no entry."""
    if s is None:
        return False  # default to False if no value is provided
    v = s.lower()
    if v == 'y' or v == 'yes' or v == 'p':  # p == probably, we take it as true
        return True
    return False


def clean_coords(coords: list[tuple[float, float]]) -> list[tuple[float, float, float]]:
    """Clean coordinates by weeding out duplicate values (zero length legs)."""
    new_coords = []
    last_coord = None
    for coord in coords:
        if last_coord is None or last_coord != coord:
            new_coords.append((coord[0], coord[1], 0.))
        last_coord = coord
    return new_coords


conn = create_engine('postgresql://' + db_user + ':' + parse.quote_plus(db_password) + '@' + db_host + ':' + str(
    db_port) + '/' + db_name).connect()

# --------------- get hub data from source ---------------
for result in conn.execute(text("SELECT rechubid, geom, harbor, overnight FROM topology.rechubs")):
    # get column data
    hub_id = result[0]
    geom = result[1]
    if not geom:
        continue  # skip empty geometries

    # check recroads - is hub connected at all?
    roads = conn.execute(text(f"SELECT COUNT(*) FROM topology.recroads WHERE hubaid = '{hub_id}' OR hubbid = '{hub_id}'")).one()
    if roads[0] == 0:
        print(f"Warning: Hub {hub_id} is not connected to any roads (skipping)")
        continue  # skip hubs that do not connect to any roads

    harbor = parse_yes_no_entry(result[2])
    overnight = parse_yes_no_entry(result[3])
    market = False  # just take fixed value for now

    conn.execute(text(
        f"INSERT INTO sitt.hubs (id, geom, overnight, harbor, market) VALUES ('{hub_id}', '{geom}', {overnight}, {harbor}, {market});"))

conn.commit()

# -------------- get road data from source ---------------
for result in conn.execute(text("SELECT recroadid, hubaid, hubbid, geom FROM topology.recroads")):
    # get column data
    road_id = result[0]
    hubaid = result[1]
    hubbid = result[2]
    geom = result[3]
    if not geom or not hubaid or not hubbid:
        continue  # skip empty geometries and ids
    # check hub existence
    huba_exists = conn.execute(text(f"SELECT COUNT(*) FROM sitt.hubs WHERE id = '{hubaid}'")).one()
    hubb_exists = conn.execute(text(f"SELECT COUNT(*) FROM sitt.hubs WHERE id = '{hubbid}'")).one()

    missing_ids = []
    if huba_exists[0] == 0:
        missing_ids.append(hubaid)
    if hubb_exists[0] == 0:
        missing_ids.append(hubbid)

    if len(missing_ids) > 0:
        print(f"Warning: Road {road_id} connects to non-existing hub(s): {missing_ids} (skipping)")
        continue  # skip roads that do not connect to existing hubs

    geom = wkb.loads(geom)
    market = False  # just take fixed value for now

    # weed out duplicate values
    coords = clean_coords(list(geom.coords))
    line = LineString(coords)
    line_str = f"SRID=" + str(crs_no) + ";" + str(line.wkt)

    conn.execute(text(
        f"INSERT INTO sitt.roads (id, geom, hub_id_a, hub_id_b) VALUES ('{road_id}', ST_GeographyFromText('{line_str}'), '{hubaid}', '{hubbid}');"))

conn.commit()
