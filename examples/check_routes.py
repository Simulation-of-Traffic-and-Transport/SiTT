# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to check routes - it will collect all possible routes and check if they make sense. Each route
must have a start and end point in the network, for example (only outgoing or incoming edges).
"""
import argparse

import psycopg2
import shapefile
from shapely import wkb, Point, LineString
import igraph as ig

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="This is an example how to validate river directions and hub heights - are rivers flowing upwards?",
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

    # load possible columns
    possible_columns = {}

    cur.execute("select column_name from INFORMATION_SCHEMA.COLUMNS where table_name IN ('recroads', 'reclakes', 'recrivers') and table_schema = 'topology' and data_type = 'smallint'")
    for data in cur:
        col_name = data[0]
        if col_name not in possible_columns:
            possible_columns[col_name] = 0
        possible_columns[col_name] += 1

    columns = []
    for col_name, count in possible_columns.items():
        if count >= 3:
            columns.append(col_name)

    for col_name in columns:
        vertices = {}

        # the graph to check
        g = ig.Graph(directed=True)

        # load all paths for this column
        for table in ["recroads", "reclakes", "recrivers"]:
            cur.execute(f"select recroadid, hubaid, hubbid, {col_name} from topology.{table} WHERE {col_name} <> 0 AND {col_name} IS NOT NULL")
            for data in cur:
                # check vertices
                if data[1] not in vertices:
                    vertices[data[1]] = True
                    g.add_vertex(data[1])
                if data[2] not in vertices:
                    g.add_vertex(data[2])
                    vertices[data[2]] = True

                # A -> B
                if data[3] == 1 or data[3] == 2:
                    g.add_edge(data[1], data[2], name=data[0])
                # B -> A
                if data[3] == -1 or data[3] == 2:
                    g.add_edge(data[2], data[1], name=data[0] + '_r')

        # calculate incoming and outgoing edges
        g.vs['degree_in'] = g.degree(mode='in')
        g.vs['degree_out'] = g.degree(mode='out')

        # find all vertices with only outgoing edges
        out_nodes = ", ".join(g.vs.select(degree_in_eq=0)['name'])
        in_nodes = ", ".join(g.vs.select(degree_out_eq=0)['name'])

        # do we have disjunct graphs?
        components = len(g.connected_components(mode='weak'))

        # print
        print(col_name)
        if out_nodes:
            print(f"OUT: {out_nodes}")
        else:
            print(f"OUT: *MISSING*")
        if in_nodes:
            print(f"IN:  {in_nodes}")
        else:
            print(f"IN:  *MISSING*")
        if components > 1:
            print(f"**** Graph contains disjunct components: {components}")
        print('--------------------------------------------------')
