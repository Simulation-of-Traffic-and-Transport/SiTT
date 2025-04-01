# River Segmentation

This document describes the segmentation of rivers into graphs.

Reference scripts:

* [convert_water_bodies_to_parts.py](../../precalculation/convert_water_bodies_to_parts.py) or [convert_water_bodies_to_parts_nogeos.py](../../precalculation/convert_water_bodies_to_parts_nogeos.py)
* [create_base_river_networks.py](../../precalculation/create_base_river_networks.py)
* [convert_base_river_networks_to_edges.py](../../precalculation/convert_base_river_networks_to_edges.py)

## Segmenting Water Bodies

Let's look at the following river part:

![01_water_body.png](img%2F01_water_body.png)

The segmenting algorithm will first run a Delaunay operation on the shape to create triangles:

![02_delaunay_triangles.png](img%2F02_delaunay_triangles.png)

These triangles form the base to calculate our river model.

As a first step, we create a network from the segments we created. The algorithm will check neighboring
polygons and create a graph model from them:

![03_graph_model_of_triangles.png](img%2F03_graph_model_of_triangles.png)

We figure out the following data for each part:

* Add harbors from our hub model (= closest hubs with harbor set to true).
* Depth (taken from manually entered data points) - see [prepare_water_depths](../../precalculation/prepare_water_depths.py).
* Height profile - from geotiff data. This will come handy once we try to calculate slopes.

From this model, we have to figure out segments of the water body. Since water bodies can branch into
multiple subsegments, we need to find these branches. This can be done using our graph - branches are
vertices with a degree of 3 or more. We can use this knowledge to compact vertices to segments of the
river by merging vertices with a degree of 2 or less. The following images show this, including one harbor.

![04_river_segments.png](img%2F04_river_segments.png)

This image highlights one of the created segments. Segments actually overlap, since their end vertices are in the center
of one of the river triangles we have created above.

![05_river_segment_highlight.png](img%2F05_river_segment_highlight.png)

For reference, we have been inspired by the
[IGraph interface for Mathematica](http://szhorvat.net/mathematica/IGDocumentation/#igsmoothen), but needed to preserve
the geometry. There is a
[Stackoverflow question](https://stackoverflow.com/questions/68499507/reduce-number-of-nodes-edges-of-a-graph-in-nedworkx)
that outlines what we did, although we use igraph.

So we finished the hardest step - creating river segments. At this stage, we can calculate the following data:

* Calculate flow direction (taken from heights of elements) and downhill gradient of each section
* Calculate width (taken from triangle shapes and the shortest and longest lines to the shores - we need the water_lines
  table for this).
* Calculate the average water speed using the following flow formula:
  ![river_formula.svg](img%2Friver_formula.svg)
  (see https://de.wikipedia.org/wiki/Flie%C3%9Fformel)

As a last step, we need to reduce the number of lines between our harbors. We have developed a path weeder class for
this task. It utilizes the A* algorithm and an increasing cost function for edges in order to diversify routes a bit.
We take the best *n* routes (default is 5) between each harbor and create a final graph from this which is added to
our "master graph" for the simulation.

![06_final_river_path.png](img%2F06_final_river_path.png)

As one can see, the routes are not ideal, but good enough to start the simulation and test our data.
