# Water Data

The tables water_bodies and water_lines contain the principle data on water. Data should be in normalized form in order
to work properly.

## Importing from Shapefiles

If you have shapefiles, you can import the data using `ogr2ogr`. Like this:

```shell
ogr2ogr -f "PostgreSQL" PG:"host=localhost dbname=sitt user=postgres password=supersecret" Shapefile.shp 
```

After this, move to topology and rename table into water_bodies.

## Rivers and Islands

If you have split data of rivers and islands, you can create proper data by joining, cutting and splitting the geo data.
First, create a union of water bodies and islands.

```postgresql
-- create a schema to work with the data
CREATE SCHEMA water_wip;

SELECT 1 as id, ST_Union(st_makevalid(i.wkb_geometry)) as geom INTO water_wip.all_islands FROM islands i WHERE i.wkb_geometry IS NOT NULL;
SELECT 1 as id, ST_Union(st_makevalid(w.wkb_geometry)) as geom INTO water_wip.all_water FROM water w WHERE w.wkb_geometry IS NOT NULL;
```

Now select into one very large multipolygon:

```postgresql
select w.id, ST_CollectionExtract(st_difference(w.geom, (SELECT i.geom FROM water_wip.all_islands i))) as geom into water_wip.all_water_body from water_wip.all_water w;
```

Finally, create separate water body entities in the normalized table:

```postgresql
SELECT (wb.dump).path[1] as id, (wb.dump).geom as geom INTO topology.water_body FROM (SELECT ST_DUMP(geom) as dump FROM water_wip.all_water_body) as wb;
CREATE INDEX sidx_water_body_geom ON topology.water_body USING gist (geom);
```

## Check for Touching Rings

To finalize our data, we want to check if there are touching rings within the polygons of water. Run this procedure
to find touching rings within your polygons and fix those by creating a small circle around the touching points.

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
            SELECT id FROM topology.water_body where st_numinteriorrings(geom) > 0
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
                    FROM topology.water_body
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
                        UPDATE topology.water_body
                        SET geom = ST_Union(water_body.geom, ST_Buffer(iter.geom, 0.00005, 'quad_segs=8'))
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
SELECT (d.dump_set).path[1] as id, (d.dump_set).geom as geom into topology.water_lines FROM (SELECT ST_Dump(ST_Boundary(geom)::geometry) as dump_set from topology.water_body) as d;
```

## Indexes

Don't forget to create indexes:

```postgresql
CREATE INDEX sidx_water_lines_geom ON topology.water_lines USING gist (geom);
CREATE INDEX sidx_water_body_geom ON topology.water_body USING gist (geom);
```
