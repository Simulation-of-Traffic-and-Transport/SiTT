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

# --------------- truncate data first ---------------

conn.execute(text("TRUNCATE sitt.roads"))
conn.execute(text("TRUNCATE sitt.edges"))
conn.execute(text("TRUNCATE sitt.rivers"))
conn.execute(text("DELETE FROM sitt.hubs"))
conn.commit()

# --------------- get hub data from source ---------------
for result in conn.execute(text("SELECT rechubid, geom, harbor, overnight FROM topology.rechubs")):
    # get column data
    hub_id = result[0]
    geom = result[1]
    if not geom:
        continue  # skip empty geometries

    # check recroads and recrivers - is hub connected at all?
    roads = conn.execute(text(f"SELECT COUNT(*) FROM topology.recroads WHERE hubaid = '{hub_id}' OR hubbid = '{hub_id}'")).one()
    rives = conn.execute(text(f"SELECT COUNT(*) FROM topology.recrivers WHERE hubaid = '{hub_id}' OR hubbid = '{hub_id}'")).one()
    if roads[0] == 0 and rives[0] == 0:
        print(f"Warning: Hub {hub_id} is not connected to any roads or rivers (skipping)")
        continue  # skip hubs that do not connect to any roads or rivers

    harbor = parse_yes_no_entry(result[2])
    overnight = parse_yes_no_entry(result[3])
    market = False  # just take fixed value for now

    conn.execute(text(
        f"INSERT INTO sitt.hubs (id, geom, overnight, harbor, market) VALUES ('{hub_id}', '{geom}', {overnight}, {harbor}, {market});"))

conn.commit()

# -------------- get road data from source ---------------
road_hub_id_missing = []
road_hub_too_far = []

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
        road_hub_id_missing.append(road_id)
        print(f"Warning: Road {road_id} connects to non-existing hub(s): {missing_ids} (skipping)")
        continue  # skip roads that do not connect to existing hubs

    for hub_distance in conn.execute(text(f"SELECT id, st_distance(geom, '{geom}') FROM sitt.hubs WHERE id IN ('{hubaid}', '{hubbid}')")):
        if hub_distance[1] > 50.: # distance too high, we consider this as a problem
            road_hub_too_far.append(road_id)
            print(f"Warning: River {road_id} connects to hub {hub_distance[0]} which is too far away: {hub_distance[1]}m")

    geom = wkb.loads(geom)

    # weed out duplicate values
    coords = clean_coords(list(geom.coords))
    line = LineString(coords)
    line_str = f"SRID=" + str(crs_no) + ";" + str(line.wkt)

    conn.execute(text(
        f"INSERT INTO sitt.roads (id, geom, hub_id_a, hub_id_b) VALUES ('{road_id}', ST_GeographyFromText('{line_str}'), '{hubaid}', '{hubbid}');"))

conn.commit()

# Print problems as list, so we can copy this to SQL select
if len(road_hub_id_missing):
    print("Warning: The following roads connect to non-existing hubs:")
    print(road_hub_id_missing)

if len(road_hub_too_far):
    print("Warning: The following roads connect to hubs which are too far away:")
    print(road_hub_too_far)

# -------------- get river data from source ---------------
river_hub_id_missing = []
river_hub_too_far = []

for result in conn.execute(text("SELECT recroadid, hubaid, hubbid, geom, direction FROM topology.recrivers")):
    # get column data
    river_id = result[0]
    hubaid = result[1]
    hubbid = result[2]
    geom = result[3]
    # check if river is going upwards (i.e. towing)
    is_tow = 'true' if result[4] == 'upwards' else 'false'

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
        river_hub_id_missing.append(river_id)
        print(f"Warning: River {river_id} connects to non-existing hub(s): {missing_ids} (skipping)")
        continue  # skip rivers that do not connect to existing hubs

    for hub_distance in conn.execute(text(f"SELECT id, st_distance(geom, '{geom}') FROM sitt.hubs WHERE id IN ('{hubaid}', '{hubbid}')")):
        if hub_distance[1] > 50.: # distance too high, we consider this as a problem
            river_hub_too_far.append(river_id)
            print(f"Warning: River {river_id} connects to hub {hub_distance[0]} which is too far away: {hub_distance[1]}m")

    geom = wkb.loads(geom)

    # weed out duplicate values
    coords = clean_coords(list(geom.coords))
    line = LineString(coords)
    line_str = f"SRID=" + str(crs_no) + ";" + str(line.wkt)

    conn.execute(text(
        f"INSERT INTO sitt.rivers (id, geom, hub_id_a, hub_id_b, target_hub, is_tow) VALUES ('{river_id}', ST_GeographyFromText('{line_str}'), '{hubaid}', '{hubbid}', '{hubbid}', {is_tow});"))

conn.commit()

# Print problems as list, so we can copy this to SQL select
if len(river_hub_id_missing):
    print("Warning: The following rivers connect to non-existing hubs:")
    print(river_hub_id_missing)

if len(river_hub_too_far):
    print("Warning: The following rivers connect to hubs which are too far away:")
    print(river_hub_too_far)