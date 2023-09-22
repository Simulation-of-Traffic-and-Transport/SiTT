# River Segmentation

This document describes the segmentation of rivers into graphs.

## Preparation of data

Refer to [create-database.sql](..%2Fexamples%2Fcreate-database.sql) for information.


## Segmenting Water Bodies

Let's assume we have the following shape for a river:

![01_water_body.png](img%2F01_water_body.png)

The segmenting algorithm will first run a Delaunay operation on the shape to create triangles:

![02_delaunay_triangles.png](img%2F02_delaunay_triangles.png)

Many of the triangles will lie outside the shape, so we have to cut them into shape in the next step:

![03_delaunay_cut_to_shape.png](img%2F03_delaunay_cut_to_shape.png)

After segmentation, we have a dataset of triangles for each water body in the database.


## Network Creation

In the next step, we create a network from the segments we created. The algorithm will check neighboring
polygons and create a graph model from them:

![04_graph_model_of_triangles.png](img%2F04_graph_model_of_triangles.png)

From this model, we have to figure out segments of the water body. Since water bodies can branch into
multiple subsegments, we need to figure out where the water body brances. This is easy to do within
a network - we just have to check the degree of each node - if a node's degree is greater than two
(more than two edges connecting to other nodes), it is a branch. In our example, these are the branches:

![05_connecting_vertices.png](img%2F05_connecting_vertices.png)

In a first step, we connect single ("dead-end" vertcices) and interconnected branches with single lines:

![06_connecting_and_dangling_vertices.png](img%2F06_connecting_and_dangling_vertices.png)

Now we need to connect all remaining elements with a single line (end create a union of shapes):

![07_merging_vertices.png](img%2F07_merging_vertices.png)

TODOs to go on:

* One more simplification step to erradicate  elements that are still with degree 2.
* Unify shapes
* For each shape:
  * Calculate flow direction (taken from heights of elements) and downhill gradient
  * Calculate medians, so we get a better path through each shape
  * Calculate minimum river distance (take shape and parent's outline, so we know the banks - check distances)
  * Calculate average water speed using the following flow formula (https://de.wikipedia.org/wiki/Flie%C3%9Fformel):
    ![river_formula.svg](img%2Friver_formula.svg)  

---

Wir brauchen folgende Werte pro Flussabschnitt:

* k<st> = Rauheitsbeiwert in m^1/3, d.h. wie viel Widerstand ist im Fluss - hier müssen wir plausible Werte annehmen (nach Wikipedia sind typische Werte für mäandrierendes Gewässer 20-30, Wildbach mit Geröll 10-20, Wildbach mit Unterholz < 10, oder auch gerades Gewässer 30-40). Das müsste man vermutlich per Hand pro Abschnitt definieren, bzw. einen plausiblen Einheitswert annehmen und bestimmte Abschnitte dann als schneller oder langsamer definieren, bei denen man die Topographie kennt.
* R = hydraulischer Radius - hier kann man wohl annhäherungsweise die Wassertiefe verwenden, ansonsten wäre das durchflossener Querschnitt (m²)/benetzter Umfang in m - das klingt nicht so, also ob wir das haben, weil wir ja keine 3D-Shapes des Flussbetts berechnen können.
* I = Fließgefälle - das kann ich berechnen, ist ja Höhe/Stecke

Das klingt eigentlich ganz gut, denn wir müssen lediglich Wassertiefen und Rauheit definieren. Bei Regentagen/Trockenperioden könnten wir noch einen Faktor einbeziehen, der die Wasserhöhe in Abhängigkeit zur Breite verändert. So könnten wir berücksichtigen, dass Flüsse nach Regeneintrag schneller und im Sommer bei anhaltender Trockenheit langsamer fließen. Das wäre aber ein zweiter Schritt.


