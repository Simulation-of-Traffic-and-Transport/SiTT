# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example of how to set the heights of the hubs using GeoTIFF or the Google Geolocation API.
"""
import argparse
from os.path import abspath

import psycopg2
import rasterio
import requests
from pyproj import Transformer
from shapely import wkb, Point

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="This is an example of how to set the heights of the hubs using GeoTIFF or the Google Geolocation API.",
        exit_on_error=False)

    # Postgis settings
    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    # table settings
    parser.add_argument('-t', '--table', dest='table', default='topology.rechubs', type=str, help='table to update')
    parser.add_argument('-c', '--id-column', dest='id_column', default='rechubid', type=str, help='id column')
    parser.add_argument('-g', '--geo-column', dest='geo_column', default='geom', type=str, help='geometry column')
    parser.add_argument('--height-column', dest='height_column', default='height_m', type=str, help='if non-empty, update column with height, too')

    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection source')
    parser.add_argument('-k', '--keep', dest='keep_existing', default=True, type=bool,
                        help='keep existing coordinates (overwrite otherwise)')

    match_group = parser.add_mutually_exclusive_group(required=True)

    # GeoTIFF settings
    match_group.add_argument('-i', '--input-file', dest='file', type=str, help='input file (GeoTIFF)')
    parser.add_argument('-b', '--band', dest='band', default=1, type=int, help='band to use from GeoTIFF')

    # Google Geolocation API settings
    match_group.add_argument('--google-api-key', dest='google_api_key', default='', type=str, help='Google API key for elevation data (if needed)')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password, port=args.port)
    cur = conn.cursor()
    cur_upd = conn.cursor() # update cursor

    # load GeoTIFF, if defined
    if args.file:
        rds: rasterio.io.DatasetReader = rasterio.open(abspath(args.file))
        rds_transformer = Transformer.from_crs(args.crs_no, rds.crs, always_xy=True)
        # get relevant band
        band = rds.read(args.band)

    # prepare Google Aggregation
    if args.google_api_key:
        # this will hold a list of hub ids - since Google returns coordinates in order, this is safe
        coordinate_hubs: list[str] = []
        # this will hold a list of coordinates to be submitted to the Google API
        coordinates: list[str] = []

    # Get hubs to work with
    if args.height_column:
        cur.execute(f"SELECT {args.id_column}, {args.geo_column}, {args.height_column} FROM {args.table}")
    else:
        cur.execute(f"SELECT {args.id_column}, {args.geo_column} FROM {args.table}")
    for data in cur:
        hub_geom: Point = wkb.loads(data[1])

        # skip, if set already and if we want to keep existing coordinates
        if args.keep_existing:
            if args.height_column:
                if data[2] > 0.:
                    continue
            elif hub_geom.z > 0.:
                continue

        # transform to height using GeoTIFF
        if args.file:
            xx, yy = rds_transformer.transform(hub_geom.x, hub_geom.y)
            x, y = rds.index(xx, yy)
            height = band[x, y]

            # update height, if defined
            if height > 0.:
                print(f"Updating hub {data[0]} with height {height}...")
                if args.height_column:
                    cur_upd.execute(
                        f"UPDATE {args.table} SET {args.geo_column} = ST_MakePoint(ST_X({args.geo_column}),ST_Y({args.geo_column}), %s), {args.height_column} = %s WHERE {args.id_column} = %s",
                        (float(height), float(height), data[0]))
                else:
                    cur_upd.execute(
                        f"UPDATE {args.table} SET {args.geo_column} = ST_MakePoint(ST_X({args.geo_column}),ST_Y({args.geo_column}), %s) WHERE {args.id_column} = %s",
                        (float(height), data[0]))

        if args.google_api_key:
            # add to coordinate map
            coordinate_hubs.append(data[0])
            coordinates.append(f"{hub_geom.y},{hub_geom.x}") # lat/lng!

    # We will submit all the coordinates to the Google API in order to lower costs - Google charges per request...
    # see: https://developers.google.com/maps/documentation/elevation/requests-elevation
    # for more
    if args.google_api_key and len(coordinates) > 0:
        if len(coordinates) > 512:
            # TODO: split coordinates into smaller chunks and make multiple requests
            print("Google API request limit exceeded (>512). Please reduce the number of coordinates.")
            exit(1)

        response = requests.get(
            f"https://maps.googleapis.com/maps/api/elevation/json?locations={'|'.join(coordinates)}&key={args.google_api_key}")
        resp = response.json()
        if resp and 'results' in resp:
            idx = 0
            for result in resp['results']:
                hub = coordinate_hubs[idx]
                print(f"Updating hub {hub} with height {result['elevation']}...")
                if args.height_column:
                    cur_upd.execute(
                        f"UPDATE {args.table} SET {args.geo_column} = ST_MakePoint(ST_X({args.geo_column}),ST_Y({args.geo_column}), %s), {args.height_column} = %s WHERE {args.id_column} = %s",
                        (result['elevation'], result['elevation'], hub))
                else:
                    cur_upd.execute(
                        f"UPDATE {args.table} SET {args.geo_column} = ST_MakePoint(ST_X({args.geo_column}),ST_Y({args.geo_column}), %s) WHERE {args.id_column} = %s",
                        (result['elevation'], hub))
                idx += 1


    conn.commit()
