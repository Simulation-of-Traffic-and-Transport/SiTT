# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This shows how to separate raw river data into shores. This will expect helper points in a table named
"water_wip.raw_river_separators". With help of these points, the exterior of the relevant raw river shapes will be
split into a left and right shore.

The following tables are needed:

create table water_wip.raw_river_separators
(
    id     serial
        constraint raw_river_separators_pk
            primary key,
    ref_id integer not null,
    geom   geometry(Point, 4326)
);

alter table water_wip.raw_river_separators
    owner to postgres;

create index raw_river_separators_geom_index
    on water_wip.raw_river_separators using gist (geom);

create index raw_river_separators_ref_id_index
    on water_wip.raw_river_separators (ref_id);


create table water_wip.raw_river_shores
(
    id     serial
        constraint raw_river_shores_pk
            primary key,
    ref_id integer,
    geom   geometry(LineString, 4326)
);

alter table water_wip.raw_river_shores
    owner to postgres;

create index raw_river_shores_geom_index
    on water_wip.raw_river_shores using gist (geom);

create index raw_river_shores_ref_id_index
    on water_wip.raw_river_shores (ref_id);

"""

import argparse

import psycopg2
from shapely import wkb, Polygon, LineString

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="This is an example how to create river segments in the database.",
        exit_on_error=False)

    # Postgis settings
    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password,
                            port=args.port)
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur_upd = conn.cursor()  # update cursor

    # truncate shores
    cur_upd.execute("truncate table water_wip.raw_river_shores restart identity")
    conn.commit()

    # select points from separator table
    cur.execute("SELECT ref_id, geom FROM water_wip.raw_river_separators ORDER BY ref_id")
    for data in cur:
        river_id = data[0]
        p1 = wkb.loads(data[1])
        # fetch the next point
        data = cur.fetchone()
        if river_id != data[0]:
            print(f"Error: Unexpected river_id {data[0]} - expected river_id {river_id}!")
            exit(1)
        p2 = wkb.loads(data[1])

        cur2.execute(f"SELECT wkb_geometry FROM water_wip.raw_rivers WHERE ogc_fid = %s", (river_id,))
        shape: Polygon = wkb.loads(cur2.fetchone()[0])
        # now create a list of line segments to find the two lines closest to the two points
        d1, d2 = float("inf"), float("inf")
        l1, l2 = None, None
        for i in range(len(shape.exterior.coords) - 1):
            l = LineString([shape.exterior.coords[i], shape.exterior.coords[i + 1]])
            # ignore empty lines
            if l.length == 0:
                continue
            dist = l.distance(p1)
            if dist < d1:
                d1 = dist
                l1 = l
            dist = l.distance(p2)
            if dist < d2:
                d2 = dist
                l2 = l

        # we have the closest line segments, take their endpoints

        # get nearest points
        nearest_points = [
            l1.coords[0],
            l1.coords[1],
            l2.coords[0],
            l2.coords[1],
        ]

        # this will hold the target line coordinates
        coords = [
            [], # first line
            [], # second line
            [], # prepend this to first line below
        ]
        current_idx = 0
        last_point_was_a_near_point = False

        # split river shape into two parts
        for coord in shape.exterior.coords[:-1]:
            # check if current part is one of the nearest points
            if coord in nearest_points:
                if last_point_was_a_near_point:
                    current_idx += 1
                    last_point_was_a_near_point = False
                else:
                    last_point_was_a_near_point = True

            coords[current_idx].append(coord)

        # glue correctly
        coords = [
            coords[2] + coords[0],
            coords[1]
        ]

        # insert into shores table
        for c in coords:
            cur_upd.execute("INSERT INTO water_wip.raw_river_shores (ref_id, geom) VALUES (%s, %s)", (river_id, wkb.dumps(LineString(c))))

    conn.commit()