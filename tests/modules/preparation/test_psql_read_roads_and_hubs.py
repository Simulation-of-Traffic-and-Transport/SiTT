# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

from sitt import Configuration, Context
from sitt.modules.preparation import PsqlReadRoadsAndHubs
from tests.helpers.prepare_test_database import prepare_test_database


def test_psql_read_roads_and_hubs_init():
    entity = PsqlReadRoadsAndHubs()

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
    assert entity.strategy == 'merge'
    assert entity.connection is None


def test_psql_read_roads_and_hubs_run():
    if prepare_test_database() is False:
        print("Skipping test test_psql_read_roads_and_hubs_run because we cannot prepare database.")
        return

    # create module
    entity = PsqlReadRoadsAndHubs(db='sitt_test', password='12345')
    assert entity.db == 'sitt_test'

    # run module
    context = entity.run(Configuration(), Context())

    # checks
    assert len(context.raw_hubs) == 3
    assert len(context.raw_roads) == 1
    assert len(context.raw_rivers) == 1
    assert len(context.raw_hubs.columns) == 2
    assert len(context.raw_roads.columns) == 3
    assert len(context.raw_rivers.columns) == 3

    assert context.raw_hubs.columns[0] == 'geom'
    assert context.raw_hubs.columns[1] == 'overnight'

    assert context.raw_roads.columns[0] == 'geom'
    assert context.raw_roads.columns[1] == 'hubaid'
    assert context.raw_roads.columns[2] == 'hubbid'

    assert context.raw_rivers.columns[0] == 'geom'
    assert context.raw_rivers.columns[1] == 'hubaid'
    assert context.raw_rivers.columns[2] == 'hubbid'
