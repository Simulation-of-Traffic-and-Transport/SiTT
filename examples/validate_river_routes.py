# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to check river routes. The script will collect all possible routes and check if they make sense,
i.e. if they are going into the right direction. Imported river routes should have hubaid set to the starting point of
the route and hubbid to its ending one. Consequently, river routes going downwards should go in flow direction, upward
routes should travel in reverse flow direction. If a river route is not in the right direction, it will be printed to
be copied into a CSV file for manual review.
"""
import argparse

import psycopg2

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="This is an example how to check river route directions, i.e. are downward routes traveled in"
                    "reverse flow direction, or upward ones in flow direction?",
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

    unique_routes = set()

    for col_name in columns:
        # find routes that might be against the direction of the route
        cur.execute(
            f"select recroadid, hubaid, hubbid, direction, {col_name} from topology.recrivers WHERE {col_name} NOT IN (0, 1) AND {col_name} IS NOT NULL")
        for data in cur:
            print(f"{data[0]},{col_name},{data[1]},{data[2]},{data[3]},{data[4]}")

            unique_routes.add(data[0])

    print(f"Total unique routes: {len(unique_routes)}")