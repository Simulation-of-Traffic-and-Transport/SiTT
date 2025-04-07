# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

"""
Example of how to read a Shape file into SQL.
"""

import psycopg2
import shapefile
from shapely import LineString

conn = psycopg2.connect("host=localhost dbname=sitt user=postgres password=12345")
conn.set_isolation_level(0)
cur = conn.cursor()

r = shapefile.Reader("/home/mkalus/Kaernten/Kaerntner Flussrouten/Routen_Fluesse.shp").shapeRecords()

for i in range(len(r)):
    rec: shapefile.ShapeRecord = r[i]
    print(rec.record)
    geom = LineString(rec.shape.__geo_interface__['coordinates'])
    print(geom.wkt)

    cur.execute("INSERT INTO topology.recrivers (id, geom, recroadid, hubaid, hubbid, direction, dimensions,"
                "explanationfileid) VALUES (%s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, '', '')",
                (i, geom.wkt, rec.record[1], rec.record[2], rec.record[3], rec.record[4]))
