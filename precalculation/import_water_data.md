# Water Data

The tables water_bodies and water_lines contain the principle data on water. Data should be in normalized form in order
to work properly.

## Importing from Shapefiles

If you have shapefiles, you can import the data using `ogr2ogr`. Like this - we assume that you have created a
schema `water_wip` in your PostgreSQL/PostGIS database:

```postgresql
-- create a schema to work with the data
CREATE SCHEMA IF NOT EXISTS water_wip;
```

This will be our temporary schema to hold some tables we can delete after we have finished our import.

Import data:

```shell
ogr2ogr -f "PostgreSQL" PG:"host=localhost dbname=sitt user=postgres password=supersecret" -nln "water_wip.all_water_body" Shapefile.shp 
```

## Rivers, Islands, and Lakes

If you have different shapefiles, like river, islands, and lakes, you can do the following:

```shell
ogr2ogr -f "PostgreSQL" PG:"host=localhost dbname=sitt user=postgres password=12345" -nln "water_wip.raw_rivers" Rivers.shp
ogr2ogr -f "PostgreSQL" PG:"host=localhost dbname=sitt user=postgres password=12345" -nln "water_wip.raw_lakes" Lakes.shp
ogr2ogr -f "PostgreSQL" PG:"host=localhost dbname=sitt user=postgres password=12345" -nln "water_wip.raw_islands" Islands.shp
```

You can now create proper data by joining, cutting and splitting the geo data. First, create a union of water bodies and islands.

```postgresql
-- create a schema to work with the data
CREATE SCHEMA IF NOT EXISTS water_wip;

SELECT 1 as id, ST_Union(st_makevalid(i.wkb_geometry)) as geom INTO water_wip.all_islands FROM water_wip.raw_islands i WHERE i.wkb_geometry IS NOT NULL;
SELECT 1 as id, ST_Union(st_makevalid(w.wkb_geometry)) as geom INTO water_wip.all_rivers FROM water_wip.raw_rivers w WHERE w.wkb_geometry IS NOT NULL;
SELECT 1 as id, ST_Union(st_makevalid(w.wkb_geometry)) as geom INTO water_wip.all_lakes FROM water_wip.raw_lakes w WHERE w.wkb_geometry IS NOT NULL;
```

Now select into very large multipolygons:

```postgresql
select w.id, ST_CollectionExtract(st_difference(w.geom, (SELECT i.geom FROM water_wip.all_islands i))) as geom into water_wip.all_river_body from water_wip.all_rivers w;
select w.id, ST_CollectionExtract(st_difference(w.geom, (SELECT i.geom FROM water_wip.all_islands i))) as geom into water_wip.all_lake_body from water_wip.all_lakes w;
```

Finally, create separate water body entities in the normalized table:

```postgresql
-- rivers
SELECT (wb.dump).path[1] as id, (wb.dump).geom as geom, true as is_river INTO water_wip.rivers_for_import FROM (SELECT ST_DUMP(geom) as dump FROM water_wip.all_river_body) as wb;
INSERT INTO sitt.water_bodies SELECT * FROM water_wip.rivers_for_import;
DROP TABLE water_wip.rivers_for_import;

-- now from lakes
SELECT (wb.dump).path[1] + (SELECT MAX(id) FROM sitt.water_bodies) as id, (wb.dump).geom as geom, false as is_river INTO water_wip.lakes_for_import FROM (SELECT ST_DUMP(geom) as dump FROM water_wip.all_lake_body) as wb;
INSERT INTO sitt.water_bodies SELECT * FROM water_wip.lakes_for_import;
DROP TABLE water_wip.lakes_for_import;
```

## Check for Touching Rings

To finalize our data, we want to check if there are touching rings within the polygons of water. Run this procedure
to find touching rings within your polygons:

```postgresql
DO
$$
    DECLARE
        wb_iter RECORD;
        iter    RECORD;
    BEGIN
        DROP TABLE IF EXISTS water_wip.touches;
        CREATE TABLE water_wip.touches
        (
            id            SERIAL PRIMARY KEY,
            water_body_id INTEGER,
            geom          GEOMETRY(POINT, 4326)
        );
        CREATE INDEX sidx_touches_geom ON water_wip.touches USING gist (geom);

        FOR wb_iter IN
            SELECT id FROM sitt.water_bodies where st_numinteriorrings(geom) > 0
            LOOP
                RAISE NOTICE 'Checking water body id: %', wb_iter.id;
                -- create rings table
                DROP TABLE IF EXISTS water_wip.rings;
                CREATE TABLE water_wip.rings
                (
                    id   SERIAL PRIMARY KEY,
                    geom GEOMETRY(LINESTRING, 4326)
                );
                CREATE INDEX sidx_rings_geom ON water_wip.rings USING gist (geom);
                -- fill rings table
                FOR iter IN
                    SELECT (ST_DumpRings(geom)).path[1] as id, ST_ExteriorRing((ST_DumpRings(geom)).geom) as geom
                    FROM sitt.water_bodies
                    where id = wb_iter.id
                    LOOP
                        INSERT INTO water_wip.rings (id, geom) VALUES (iter.id, iter.geom);
                    END LOOP;

                -- find closest points for touching rings
                FOR iter IN
                    SELECT st_astext(st_closestpoint(ra.geom, rb.geom)) as geom
                    FROM (SELECT DISTINCT LEAST(a.id, b.id) as least_id, GREATEST(a.id, b.id) as greatest_id
                          FROM water_wip.rings a,
                               water_wip.rings b
                          WHERE ST_Intersects(a.geom, b.geom)
                            AND a.id <> b.id) AS touch_ids,
                         water_wip.rings ra,
                         water_wip.rings rb
                    WHERE ra.id = touch_ids.least_id
                      AND rb.id = touch_ids.greatest_id
                    LOOP
                        RAISE NOTICE 'Found touching point: %', ST_AsText(iter.geom);
                        INSERT INTO water_wip.touches (water_body_id, geom) VALUES (wb_iter.id, iter.geom);
                    END LOOP;
            END LOOP;
        DROP TABLE IF EXISTS water_wip.rings;
        DROP TABLE IF EXISTS water_wip.touches;
    END ;
$$
```

Here is an expanded script, if you want to automatically fix any problems by creating a small circle around the touching
points.

```postgresql
DO
$$
    DECLARE
        wb_iter RECORD;
        iter    RECORD;
    BEGIN
        DROP TABLE IF EXISTS water_wip.touches;
        CREATE TABLE water_wip.touches
        (
            id            SERIAL PRIMARY KEY,
            water_body_id INTEGER,
            geom          GEOMETRY(POINT, 4326)
        );
        CREATE INDEX sidx_touches_geom ON water_wip.touches USING gist (geom);

        FOR wb_iter IN
            SELECT id FROM sitt.water_bodies where st_numinteriorrings(geom) > 0
            LOOP
                RAISE NOTICE 'Checking water body id: %', wb_iter.id;
                -- create rings table
                DROP TABLE IF EXISTS water_wip.rings;
                CREATE TABLE water_wip.rings
                (
                    id   SERIAL PRIMARY KEY,
                    geom GEOMETRY(LINESTRING, 4326)
                );
                CREATE INDEX sidx_rings_geom ON water_wip.rings USING gist (geom);
                -- fill rings table
                FOR iter IN
                    SELECT (ST_DumpRings(geom)).path[1] as id, ST_ExteriorRing((ST_DumpRings(geom)).geom) as geom
                    FROM sitt.water_bodies
                    where id = wb_iter.id
                    LOOP
                        INSERT INTO water_wip.rings (id, geom) VALUES (iter.id, iter.geom);
                    END LOOP;

                -- find closest points for touching rings
                FOR iter IN
                    SELECT st_astext(st_closestpoint(ra.geom, rb.geom)) as geom
                    FROM (SELECT DISTINCT LEAST(a.id, b.id) as least_id, GREATEST(a.id, b.id) as greatest_id
                          FROM water_wip.rings a,
                               water_wip.rings b
                          WHERE ST_Intersects(a.geom, b.geom)
                            AND a.id <> b.id) AS touch_ids,
                         water_wip.rings ra,
                         water_wip.rings rb
                    WHERE ra.id = touch_ids.least_id
                      AND rb.id = touch_ids.greatest_id
                    LOOP
                        RAISE NOTICE 'Found touching point: %', ST_AsText(iter.geom);
                        INSERT INTO water_wip.touches (water_body_id, geom) VALUES (wb_iter.id, iter.geom);
                    END LOOP;

                -- update water body by creating a union of the touching point and some small buffer
                FOR iter IN
                    SELECT geom, water_body_id
                    FROM water_wip.touches
                    WHERE water_body_id = wb_iter.id
                    LOOP
                        UPDATE sitt.water_bodies
                        SET geom = ST_Union(water_bodies.geom, ST_Buffer(iter.geom, 0.00005, 'quad_segs=8'))
                        WHERE id = iter.water_body_id;
                    END LOOP;
            END LOOP;
        DROP TABLE IF EXISTS water_wip.rings;
        DROP TABLE IF EXISTS water_wip.touches;
    END ;
$$
```

## Create lines from water body

```postgresql
DROP TABLE IF EXISTS sitt.water_lines;
SELECT (d.dump_set).path[1] as id, (d.dump_set).geom as geom into sitt.water_lines FROM (SELECT ST_Dump(ST_Boundary(geom)::geometry) as dump_set from sitt.water_bodies) as d;
CREATE INDEX sidx_water_lines_geom ON sitt.water_lines USING gist (geom);
```

## Clean data

```postgresql
DROP SCHEMA water_wip;

-- recommended by postgis https://postgis.net/workshops/postgis-intro/indexing.html#vacuuming
VACUUM;
```