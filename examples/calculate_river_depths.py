# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to calculate river depths per segment of the river route. As default, will use the more segmented
shapes created by create_river_segments.py, but you can set your own column to define where the LineStrings come from.
"""

import argparse

import geopandas as gpd
import numpy as np
from matplotlib.tri import Triangulation, LinearTriInterpolator
from shapely import wkb, LineString
from sqlalchemy import create_engine, text

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="This is an example how to calculate river depths per segment of the river route.",
        exit_on_error=False)

    # Postgis settings
    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    # table settings
    parser.add_argument('-rt', '--river-table', dest='river_table', default='topology.recrivers', type=str, help='river table to check')
    parser.add_argument('-rc', '--river-id-column', dest='river_id_column', default='recroadid', type=str, help='river id column')
    parser.add_argument('-rg', '--river-geo-column', dest='river_geo_column', default='geom_segments', type=str, help='river geometry column (default is the segmented geometry column created by create_river_segments.py)')
    parser.add_argument('-rd', '--river-depths-column', dest='river_depths_column', default='depths', type=str, help='river depths column (will be created, if not existing)')

    parser.add_argument('-dt', '--depths-table', dest='depths_table', default='topology.river_depths', type=str, help='river depths table to check against')
    parser.add_argument('-ds', '--depths-shape-column', dest='depths_shape_column', default='shape', type=str, help='depths shape column (to distinguish layers)')
    parser.add_argument('-dg', '--depths-geo-column', dest='depths_geo_column', default='geom', type=str, help='depths geometry column')
    parser.add_argument('-dd', '--depths-depth-column', dest='depths_depth_column', default='depth', type=str, help='depths depth value column')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # cache for shape data
    shape_cache: dict[str,LinearTriInterpolator] = {}

    # Connect to the database
    conn = create_engine(f"postgresql://{args.user}:{args.password}@{args.server}:{args.port}/{args.database}").connect()

    # add column, if needed
    # check if depth exists, create column otherwise
    schema, table = args.river_table.split('.')
    if not conn.execute(text(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table}' AND table_schema= '{schema}' AND column_name = '{args.river_depths_column}')")).fetchone()[0]:
            conn.execute(text(
                f"ALTER TABLE {args.river_table} ADD {args.river_depths_column} double precision[]"))
            conn.commit()

    # Load depths data
    gdf = gpd.GeoDataFrame.from_postgis(
        f"SELECT * FROM topology.river_depths", conn, geom_col='geom')
    totalPointsArray = np.zeros([gdf.shape[0], 3])

    for index, p in gdf.iterrows():
        pointArray = np.array([p.geom.coords.xy[0][0], p.geom.coords.xy[1][0], p['depth']])
        totalPointsArray[index] = pointArray

    # triangulation function
    triFn = Triangulation(totalPointsArray[:, 0], totalPointsArray[:, 1])
    linTriFn = LinearTriInterpolator(triFn, totalPointsArray[:, 2])

    # traverse river data
    for data in conn.execute(text(f"select {args.river_id_column}, {args.river_geo_column} from {args.river_table} WHERE {args.river_depths_column} IS NULL")):
        # load wkb data
        geom = wkb.loads(data[1])
        # structure to add data to
        coords = np.array(geom.coords)
        # structure to add data to
        new_geom = np.array(geom.coords)
        # z column exists?
        if new_geom.shape[1] < 3:
            new_geom = np.c_[new_geom, np.zeros(len(new_geom))]

        # this will hold the depths for each point
        heights = np.zeros(len(coords))

        try:
            # interpolate depths for each point in the river line
            for idx, coord in enumerate(coords):
                tempZ = linTriFn(coord[0],coord[1])
                if np.isnan(float(tempZ)):
                    # nans are converted to 0s
                    tempZ = np.float64(0.)
                heights[idx]=tempZ
                new_geom[idx, 2] = tempZ

            print("OK:", data[0])
        except:
            print("FAIL:", data[0])

        heights_str = "{" + list(heights).__str__()[1:-1] + "}"
        updated_geom = LineString(new_geom).wkt
        conn.execute(text(f"UPDATE {args.river_table} SET {args.river_depths_column} = '{heights_str}', {args.river_geo_column} = ST_GeomFromText('{updated_geom}') WHERE {args.river_id_column} = '{data[0]}'"))

    # persist changes to the database
    conn.commit()
