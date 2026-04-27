#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Create temperature deviation for edges

import argparse
import os
from urllib import parse

import numpy as np
import rasterio
import xarray as xr
from pyproj import Transformer
from shapely import wkb
from sqlalchemy import create_engine, text

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="Create temperature deviation for edges.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--schema', dest='schema', default='sitt', type=str, help='schema name')

    parser.add_argument('-i', '--input-file', dest='file', required=True, type=str, help='input file (GeoTIFF)')
    parser.add_argument('--band', dest='band', default=1, type=int, help='GeoTIFF band to use')

    parser.add_argument('--mean-heights-data-file', dest='mean_heights_data', default='mean_heights_data.nc', type=str, help='median heights file (nc)')
    parser.add_argument('--mean-heights-data-variable', dest='mean_heights_data_variable', default='mean', type=str, help='median heights variable name')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # check file exists
    if not os.path.exists(args.file):
        print("File does not exist: " + args.file)
        parser.exit(1)

    # connect to database
    conn = create_engine('postgresql://' + args.user + ':' + parse.quote_plus(args.password) + '@' + args.server + ':' +
                         str(args.port) + '/' + args.database).connect()

    # load geo
    rds: rasterio.io.DatasetReader = rasterio.open(os.path.abspath(args.file))
    transformer = Transformer.from_crs(4326, rds.crs, always_xy=True)
    # get relevant band
    band = rds.read(args.band)

    # load median heights
    ds = xr.open_dataset(os.path.abspath(args.mean_heights_data))[args.mean_heights_data_variable + '_height']

    def get_height(lng, lat) -> float:
        xx, yy = transformer.transform(lng, lat)
        x, y = rds.index(xx, yy)
        return band[x, y]


    for result in conn.execute(text(f"SELECT id, geom, data, type FROM {args.schema}.edges")):
        geom = wkb.loads(result.geom)

        # depending on the edge type and data structure, determine heights
        if result.type == 'lake':
            # let's assume the middle coordinate of the lake is the height, should be pretty accurate
            middle_xy = geom.coords[len(geom.coords) // 2]
            lake_height = get_height(middle_xy[0], middle_xy[1])
            ref_height = np.float64(ds.sel(longitude=middle_xy[0], latitude=middle_xy[1], method="nearest").values)

            deviation_height = lake_height - ref_height

            # create a numpy array with this height deviation
            temperature_deviations = np.full(len(geom.coords), deviation_height)

        elif result.type == 'road':
            temperature_deviations = np.zeros(len(result.data['leg_points']))

            for i, leg_point in enumerate(result.data['leg_points']):
                temperature_deviations[i] = get_height(leg_point[0], leg_point[1]) - ds.sel(longitude=leg_point[0], latitude=leg_point[1], method="nearest").values

        elif result.type == 'river':
            temperature_deviations = np.zeros(len(geom.coords))

            for i, xy in enumerate(geom.coords):
                temperature_deviations[i] = get_height(xy[0], xy[1]) - ds.sel(longitude=xy[0], latitude=xy[1], method="nearest").values

        else:
            raise ValueError(f"Unknown edge type: {result.type}")

        values = '{"temperature_deviation":' + str(temperature_deviations.tolist()) + '}'
        conn.execute(text(f"UPDATE {args.schema}.edges SET data = data || '{values}' WHERE id = '{result.id}'"))

    conn.commit()