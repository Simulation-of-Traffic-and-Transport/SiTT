# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Get all routes and create statistics for them

This script connects to a PostGIS database, extracts all available routes from the sitt.edges table,
and generates comprehensive statistics for each route. The statistics are saved to a CSV file
containing information about hubs, edges, lengths, slopes, and route variations.

The script performs the following operations:
1. Connects to a PostgreSQL/PostGIS database
2. Extracts unique route directions from the sitt.edges table
3. Loads the graph data into the SITT context
4. For each route, creates route data and generates detailed statistics
5. Saves all statistics to an 'all_statistics.csv' file

Command line arguments allow configuration of database connection parameters.
"""

import argparse

import psycopg2

from sitt import Configuration, Context
from sitt.modules.preparation import GraphLoad, CreateRoutes, DebugStatistics

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="Get all routes and create statistics for them.",
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

    # ################################
    # get the routes
    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password,
                            port=args.port)
    cur = conn.cursor()

    routes = set()

    cur.execute("SELECT directions from sitt.edges")
    for res in cur:
        for direction in res[0]:
            routes.add(direction)

    cur.close()
    conn.close()

    # load graph into context
    ctx = Context()
    GraphLoad(server=args.server, port=args.port, db=args.database, user=args.user, password=args.password).run(Configuration(), ctx)

    # overwrite old file, if it exists
    f = open("all_statistics.csv", 'w', newline='')
    f.write("Route,Hubs,Overnight hubs,Edges,Road edges,River edges,Lake edges,Total length (km),Road length (km),River length (km),Lake length (km),Max slope up (%),Lon max slope up,Lat max slope up,Max slope down (%),Lon max slope down,Lat max slope down,Total up (m),Total down (m),Minimum route length (km),Min route start,Min route end,Maximum route length (km),Max route start,Max route end\n")
    f.close()

    # ################################
    # Load each route and print statistics
    for route in routes:
        # run CreateRoutes for this route
        cfg = Configuration()
        cfg.simulation_route = route
        ctx = CreateRoutes(check_graph=True).run(cfg, ctx)

        # print statistics
        DebugStatistics(save=True, append=True, filename="all_statistics.csv").run(cfg, ctx)
