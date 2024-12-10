# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import hubs and roads from another database."""

from urllib import parse

from sqlalchemy import create_engine, text
from shapely import wkb, LineString, MultiPoint

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
conn.execute(text("TRUNCATE sitt.lakes"))
conn.execute(text("DELETE FROM sitt.hubs"))
conn.commit()

print("---------------------------------------------------------------------------------------------------------------")

def print_closest_hub(path_id: str, hub_id: str, hubaid: str, hubbid: str, geom: str, table: str):
    # find the two closest hubs
    geo = wkb.loads(geom)
    endpoints = MultiPoint([geo.coords[0], geo.coords[-1]])

    closest_ids = []
    for result in conn.execute(text(f"SELECT id, st_distance(geom, '{endpoints.wkt}') as dist FROM sitt.hubs ORDER BY dist LIMIT 2")):
        if result[1] <= 50.:
            closest_ids.append(result[0])
    hubaid_contained = hubaid in closest_ids
    hubbid_contained = hubbid in closest_ids
    if hubaid_contained and hubbid_contained:
        # Both not the closest hubs?
        print(f"Hub A and Hub B are both the closest hubs for {path_id} - check manually!")
    elif not hubaid_contained and not hubbid_contained or len(closest_ids) != 2:
        print(f"Hub A and Hub B are both NOT the closest hubs for {path_id} - check manually!")
    elif hubaid_contained: # a contained -> replace hubbid with closer hub
        closest_ids.remove(hubaid)
        print(f"UPDATE topology.rec{table}s SET hubbid ='{closest_ids[0]}' WHERE recroadid = '{path_id}';")
    else: # b contained -> replace hubaid with closer hub
        print(f"UPDATE topology.rec{table}s SET hubaid ='{closest_ids[0]}' WHERE recroadid = '{path_id}';")


# --------------- get hub data from source ---------------
for result in conn.execute(text("SELECT rechubid, geom, harbor, overnight FROM topology.rechubs")):
    # get column data
    hub_id = result[0]
    geom = result[1]
    if not geom:
        continue  # skip empty geometries
    harbor = parse_yes_no_entry(result[2])

    if not harbor:  # always import harbors, they might be connected by lakes
        # check recroads and recrivers - is hub connected at all?
        counter = 0
        counter += conn.execute(text(f"SELECT COUNT(*) FROM topology.recroads WHERE hubaid = '{hub_id}' OR hubbid = '{hub_id}'")).one()[0]
        if counter == 0:
            counter += conn.execute(text(f"SELECT COUNT(*) FROM topology.recrivers WHERE hubaid = '{hub_id}' OR hubbid = '{hub_id}'")).one()[0]
        if counter == 0:
            counter += conn.execute(text(f"SELECT COUNT(*) FROM topology.reclakes WHERE hubaid = '{hub_id}' OR hubbid = '{hub_id}'")).one()[0]
        if counter == 0:
            print(f"Warning: Hub {hub_id} is not connected to any roads, rivers, or lakes (skipping)")
            continue  # skip hubs that do not connect to any roads or rivers

    overnight = parse_yes_no_entry(result[3])
    market = False  # just take fixed value for now

    conn.execute(text(
        f"INSERT INTO sitt.hubs (id, geom, overnight, harbor, market) VALUES ('{hub_id}', '{geom}', {overnight}, {harbor}, {market});"))

conn.commit()

print("---------------------------------------------------------------------------------------------------------------")

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
            print(f"Warning: Road route {road_id} connects to hub {hub_distance[0]} which is too far away: {hub_distance[1]}m")
            print_closest_hub(road_id, hub_distance[0], hubaid, hubbid, geom, "road")

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

print("---------------------------------------------------------------------------------------------------------------")

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
            print(f"Warning: River route {river_id} connects to hub {hub_distance[0]} which is too far away: {hub_distance[1]}m")
            print_closest_hub(river_id, hub_distance[0], hubaid, hubbid, geom, "river")

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

print("---------------------------------------------------------------------------------------------------------------")

# -------------- get lake data from source ---------------
lake_hub_id_missing = []
lake_hub_too_far = []

for result in conn.execute(text("SELECT recroadid, hubaid, hubbid, geom FROM topology.reclakes")):
    # get column data
    lake_id = result[0]
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
        lake_hub_id_missing.append(lake_id)
        print(f"Warning: Lake {lake_id} connects to non-existing hub(s): {missing_ids} (skipping)")
        continue  # skip lakes that do not connect to existing hubs

    for hub_distance in conn.execute(text(f"SELECT id, st_distance(geom, '{geom}') FROM sitt.hubs WHERE id IN ('{hubaid}', '{hubbid}')")):
        if hub_distance[1] > 50.: # distance too high, we consider this as a problem
            lake_hub_too_far.append(lake_id)
            print(f"Warning: Lake route {lake_id} connects to hub {hub_distance[0]} which is too far away: {hub_distance[1]}m")
            print_closest_hub(lake_id, hub_distance[0], hubaid, hubbid, geom, "lake")

    geom = wkb.loads(geom)

    # weed out duplicate values
    coords = clean_coords(list(geom.coords))
    line = LineString(coords)
    line_str = f"SRID=" + str(crs_no) + ";" + str(line.wkt)

    conn.execute(text(
        f"INSERT INTO sitt.lakes (id, geom, hub_id_a, hub_id_b) VALUES ('{lake_id}', ST_GeographyFromText('{line_str}'), '{hubaid}', '{hubbid}');"))

conn.commit()

# Print problems as list, so we can copy this to SQL select
if len(lake_hub_id_missing):
    print("Warning: The following lakes connect to non-existing hubs:")
    print(lake_hub_id_missing)

if len(lake_hub_too_far):
    print("Warning: The following lakes connect to hubs which are too far away:")
    print(lake_hub_too_far)

print("---------------------------------------------------------------------------------------------------------------")
