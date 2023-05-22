# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

import geopandas as gpd
import psycopg2

from sitt import Configuration, Context
from sitt.modules.preparation import PsqlSavePathsAndHubs
from tests.helpers.prepare_test_database import prepare_test_database


def test_psql_save_paths_and_hubs_init():
    entity = PsqlSavePathsAndHubs()

    assert entity.server == 'localhost'
    assert entity.port == 5432
    assert entity.db == 'sitt'
    assert entity.user == 'postgres'
    assert entity.password == 'postgres'
    assert entity.roads_table_name == 'topology.recroads'
    assert entity.roads_geom_col == 'geom'
    assert entity.roads_index_col == 'id'
    assert entity.roads_coerce_float is True
    assert entity.roads_hub_a_id == 'hubaid'
    assert entity.roads_hub_b_id == 'hubbid'
    assert entity.rivers_table_name == 'topology.recrivers'
    assert entity.rivers_geom_col == 'geom'
    assert entity.rivers_index_col == 'id'
    assert entity.rivers_coerce_float is True
    assert entity.rivers_hub_a_id == 'hubaid'
    assert entity.rivers_hub_b_id == 'hubbid'
    assert entity.hubs_table_name == 'topology.rechubs'
    assert entity.hubs_geom_col == 'geom'
    assert entity.hubs_index_col == 'id'
    assert entity.hubs_coerce_float is True
    assert entity.hubs_overnight == 'overnight'
    assert entity.hubs_extra_fields == []
    assert entity.connection is None


def test_psql_save_paths_and_hubs_run():
    if prepare_test_database() is False:
        print("Skipping test test_psql_save_paths_and_hubs_run because we cannot prepare database.")
        return

    # create module
    entity = PsqlSavePathsAndHubs(db='sitt_test', password='12345')
    assert entity.db == 'sitt_test'

    # run module
    # run with no data...
    context = entity.run(Configuration(), Context())

    # Test empty db
    conn = psycopg2.connect("host=localhost dbname=sitt_test user=postgres password=12345")
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM topology.rechubs")
    assert cur.fetchone()[0] == 3

    cur.execute("SELECT COUNT(*) FROM topology.recroads")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT COUNT(*) FROM topology.recrivers")
    assert cur.fetchone()[0] == 1

    # Now run again with test data
    context.raw_hubs = gpd.GeoDataFrame.from_postgis("SELECT * FROM topology.rechubs",
                                                     conn, geom_col='geom',
                                                     index_col='id',
                                                     coerce_float=True)
    context.raw_roads = gpd.GeoDataFrame.from_postgis("SELECT * FROM topology.recroads",
                                                      conn, geom_col='geom',
                                                      index_col='id',
                                                      coerce_float=True)

    context.raw_rivers = gpd.GeoDataFrame.from_postgis("SELECT * FROM topology.recrivers",
                                                      conn, geom_col='geom',
                                                      index_col='id',
                                                      coerce_float=True)

    # TODO: change some data to make this a bit more interesting and meaningful

    entity.run(Configuration(), Context())

    cur.execute("SELECT COUNT(*) FROM topology.rechubs")
    assert cur.fetchone()[0] == 3

    cur.execute("SELECT COUNT(*) FROM topology.recroads")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT COUNT(*) FROM topology.recrivers")
    assert cur.fetchone()[0] == 1
