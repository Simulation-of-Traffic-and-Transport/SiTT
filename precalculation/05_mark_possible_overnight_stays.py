# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Mark hubs that are close to overnight stays in a transportation network. It helps agents in the simulation find
overnight stays more easily, even if they're slightly off the main route. Maximum distance can be set as a command line
argument, default is 1 kilometer.

This is a command line script, taking several arguments that can be accessed by executing the script using the `-h` or
`--help` option.

The script does the following:
* Load the graph from the database.
* Reset `overnight_hub` and `overnight_hub_distance` attributes for all vertices (defined in the JSON data field).
* For each overnight hub:
  * Mark itself as an overnight hub.
  * Search for neighboring hubs within the specified distance (using igraphs neighborhood function recursively to find
    nearby hubs).
  * Mark found hubs with the overnight hub's name and distance.
* Update the database with the new overnight hub information.

Note: The script takes kilometers distance and does not consider the "difficulty" reaching an overnight stay (might be
on a very steep hill just off the road). This might be changed in the future.
"""

import argparse

import igraph as ig
from sqlalchemy import text, update, func

from sitt import Configuration, Context
from sitt.modules.preparation import PSQLBase


# concrete implementation of PSQLBase
class GraphLoader(PSQLBase):
    def __init__(self, server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', schema: str = 'sitt'):
        super().__init__(server, port, db, user, password, schema)

    def run(self, config: Configuration, context: Context) -> Context:
        pass


if __name__ == "__main__":
    """Mark hubs that are close to overnight stays."""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Mark hubs that are close to overnight stays.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--schema', dest='schema', default='sitt', type=str, help='schema name')

    parser.add_argument('-d', '--distance', dest='distance', default=1., type=float,
                        help='maximum distance in kilometers to check for overnight stays')
    parser.add_argument('--dead-ends', dest='dead_ends', default=True, type=bool,
                        help='automatically include dead end overnight stays, no matter the distance')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    max_distance = args.distance * 1000  # convert to meters

    # create object to load graph from database
    loader = GraphLoader(server=args.server, port=args.port, db=args.database, user=args.user, password=args.password,
                         schema=args.schema)
    g: ig.Graph = loader.load_graph_from_database()

    # reset distances
    g.vs['overnight_hub'] = None
    g.vs['overnight_hub_distance'] = None

    # traverse overnight hubs
    for hub in g.vs.select(overnight=True):
        # mark myself as overnight hub
        hub['overnight_hub'] = hub['name']
        hub['overnight_hub_distance'] = 0.

        found_hubs = []
        order = 1  # check neighbors in this distance

        # check dead ends
        if args.dead_ends and hub.degree() == 1:
            neighbor = hub.neighbors()[0]
            # only if this is not an overnight hub
            if neighbor['overnight'] is False:
                distance = g.distances(hub.index, neighbor.index, weights='length_m')[0][0]
                # add it, if distance is greater than maximum distance (otherwise, it will be marked below)
                if distance > max_distance:
                    found_hubs.append({'vertex': neighbor.index, 'distance': distance})

        # search neighbors in increasing order of distance
        while True:
            found_any = False

            neighbors = g.neighborhood(hub.index, order=order, mindist=order)
            distances = g.distances(hub.index, neighbors, weights='length_m')[0]

            # check if neighbors are within maximum distance
            for i, n in enumerate(neighbors):
                if distances[i] <= max_distance:
                    found_hubs.append({'vertex': n, 'distance': distances[i]})
                    found_any = True

            # break loop if no neighbors found within current distance
            if not found_any:
                break
            order += 1

        # mark hub if it is close to other overnight hubs
        for found_hub in found_hubs:
            h = g.vs[found_hub['vertex']]
            if h['overnight_hub_distance'] is None or found_hub['distance'] < h['overnight_hub_distance']:
                h['overnight_hub'] = hub['name']
                h['overnight_hub_distance'] = found_hub['distance']

    # reopen/get db connection
    conn = loader.get_connection()
    hubs_table = loader.get_hubs_table()

    # update all hubs in database
    for hub in g.vs:
        hub_value = "'null'" if hub['overnight_hub'] is None else f"'\"{hub['overnight_hub']}\"'"
        hub_distance = "'null'" if hub['overnight_hub_distance'] is None else f"'{hub['overnight_hub_distance']}'"

        # update database with new data - use JSONB
        stmt = update(hubs_table).values(data=func.jsonb_strip_nulls(
            func.jsonb_set(func.jsonb_set(text("data"), '{overnight_hub_distance}', text(hub_distance), text("true")),
                           '{overnight_hub}', text(hub_value), text("true")))).where(hubs_table.c.id == hub['name'])
        conn.execute(stmt)

    conn.commit()
