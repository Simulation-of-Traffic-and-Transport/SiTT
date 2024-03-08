# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
TODO
"""

import argparse
import pickle
from os.path import exists
from urllib import parse

import igraph as ig
from sqlalchemy import create_engine, text

if __name__ == "__main__":
    """TODO"""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="TODO",  # TODO
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # connect to database
    conn = create_engine('postgresql://' + args.user + ':' + parse.quote_plus(args.password) + '@' + args.server + ':' +
                         str(args.port) + '/' + args.database).connect()

    print("Loading pickled graphs and working on them...")

    # consider every water body in water_parts
    for water_body_id in conn.execute(text("SELECT DISTINCT water_body_id FROM sitt.water_parts WHERE is_river = true")):
        water_body_id = water_body_id[0]
        print("Loading network for water body", water_body_id)

        if not exists('graph_dump_' + str(water_body_id) + '.pickle'):
            print("Graph file graph_dump_" + str(water_body_id) + ".pickle does not exist, skipping...")
            continue

        print("Loading graph from pickle file graph_dump_" + str(water_body_id) + ".pickle")

        g: ig.Graph = pickle.load(open('graph_dump_' + str(water_body_id) + '.pickle', 'rb'))

        # TODO
        print(g.es[0])

        # ----------------------- towroping
        # TODO: add harbor data
        # TODO get riverside lines from harbor to harbor to see if we can tow ships from harbor to harbor

        # ----------------------- river network
        # TODO: add harbor data
        # TODO: create segments
        # TODO: calculate base data of segments (width, depth, flow speed)
        # TODO: get optimal routes between harbors
