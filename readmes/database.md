# Database Reference

We will persist simulation data in the database. In Precalculation, we will convert the data into a common schema which
makes it easier to handle stuff during the simulation. The database cannot hold all possible data, though. There are
some special formats for climate data etc. which must be handled during the simulation.

Moreover, we assume the following:
* Postgis is recent and supports GEOS >= 3.11.0.
* All of our geo data will be xyz based. The precalculation has to make sure of this. 

## Schema

### Base Data

The whole SQL query for the schema can be found in the file [database_schema.sql](database_schema.sql).

Please make sure, Postgis module is installed on the database. We presume that all data is contained in a
schema called `sitt`.

### Hubs

Hubs contain the nodes of our network - it does not matter if a hub is connected to roads, water bodies, both or
neither.

Fields:
* `id` - unique name of the hub. Can be any string.
* `geom` - PointZ of the hub.
* `overnight` - Can an agent stay overnight in this hub?
* `harbor` - Is this hub an exchange to a waterbody? Can be true for bridges or other things that are not really harbors.
* `market` - Is this hub a marketplace?

### Roads

Roads connect hubs and are line strings.

Fields:
* `id` - unique name of the road. Can be any string.
* `geom` - LineStringZ of the road.
* `hub_id_a` - ID of "from hub", order or hubs is not important, but start linestring has to be `hub_id_a`.
* `hub_id_b` - ID of "to hub", order or hubs is not important, but end linestring has to be `hub_id_b`.
* `roughness` -  Roughness of road. Can be taken into account when travelling. Default is 1.

### Water Bodies

Water bodies are much more convoluted than roads - they possess a specific width, may be flowing into one direction,
are variable in height and velocity, etc. We need a few tables to represent this.

See [Import Water Data](../precalculation/old_files/import_water_data.md) for information on how to create the basic water data
body tables.

The base data consists of two tables:

`water_bodies` fields:
* `id` - ID of the water body, just an integer
* `geom` - generic geometry, should contain polygons and multipolygons only.
* `is_river` - True if the waterbody is a river and not a lake.

`water_lines` fields:
* `geom` - generic geometry, should contain line strings only. Keeps the borders of all water bodies, so we can check
  what lines are shores and what lines are within the river.

Water lines do not need to have an id, we just need this to identify shores.

### Graph Entries (Edges)

Roads and Water bodies are abstracted to graph data, namely edges connecting hubs. Thus, when running the simulation,
the path projection does not need to know in advance, if a particular route is a road, a lake or a river.

`edges` fields:
* `id` - ID of the edge, must be unique, text field
* `geom` - geometry of the edge, can be a lines string or a polygon
* `hub_id_a` - ID of "from hub", order or hubs is not important, but geometry should be capable of handling this.
* `hub_id_b` - ID of "to hub", order or hubs is not important, but geometry should be capable of handling this.
* `type` - type of the edge, can be one of "road", "river", or "lake".
* `cost_a_b` - basic cost variable of this edge from a to b, used for route selection as base cost
* `cost_b_a` - basic cost variable of this edge from b to a, used for route selection as base cost
* `data` - JSON data containing any relevant data about the edge
