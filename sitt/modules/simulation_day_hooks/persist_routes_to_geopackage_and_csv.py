# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Persist agents' routes to a GeoPackage file/database. We will save each day separately, so it is easier to
comprehend the data.
"""
import csv
import datetime as dt
import logging
import math
import os
import shutil

import fiona
from shapely import LineString, MultiLineString, force_2d

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults

logger = logging.getLogger()

class PersistRoutesToGeoPackageAndCSV(SimulationDayHookInterface):
    def __init__(self, delete_existing_folder: bool = True):
        super().__init__()
        self.delete_existing_folder: bool = delete_existing_folder
        """Delete existing folder before running."""
        self.basename: str | None = None
        self.folder: str | None = None
        self.file_gpkg: fiona.Collection | None = None
        self.file_routes_csv = None
        self.csv_writer_routes = None
        self.file_transport_types_csv = None
        self.csv_writer_transport_types = None
        self.min_time: dt.datetime = dt.datetime.now()
        self.route_origins: dict = {}
        """Keep track of routes of agents."""

    def _initialize(self, config: Configuration):
        # set min time
        self.min_time = dt.datetime.combine(config.start_date, dt.datetime.min.time())

        # create folder name
        start_date = config.start_date.strftime('%Y-%m-%d')
        self.basename = f"{config.simulation_route}_{start_date}"
        self.folder = f"simulation_{self.basename}"

        # remove old data if it exists
        if self.delete_existing_folder and os.path.exists(self.folder):
            shutil.rmtree(self.folder)

        # create folder
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)

        filename = os.path.join(self.folder, f"{self.basename}_routes.")
        self.file_gpkg = fiona.open(filename + 'gpkg', 'w', driver='GPKG', crs='EPSG:4326', schema={'geometry': 'MultiLineString',
                                                                                      'properties': {'id': 'str',
                                                                                                     'last_transport_type': 'str',
                                                                                                     'variant_paths': 'int',
                                                                                                     'length_hrs': 'int',
                                                                                                     'arrival_day': 'int',
                                                                                                     'arrival_hour': 'int',
                                                                                                     'start_hubs': 'str',
                                                                                                     'start_times': 'str',
                                                                                                     'end_hub': 'str',
                                                                                                     'end_time': 'datetime',
                                                                                                     'overnight_hubs': 'str',
                                                                                                     'count_hubs': 'int',
                                                                                                     'count_edges': 'int',
                                                                                                     'count_foot': 'int',
                                                                                                     'count_donkey': 'int',
                                                                                                     'count_ox': 'int',
                                                                                                     'hubs': 'str',
                                                                                                     'edges': 'str'}})

        self.file_routes_csv = open(filename + 'csv', 'w', newline='')
        self.csv_writer_routes = csv.writer(self.file_routes_csv)
        self.csv_writer_routes.writerow(
            ['ID', 'Last Transport Type', 'Variant Paths', 'Length (hrs)', 'Arrival Day', 'Arrival Hour',
             'Start Hubs', 'Start Times', 'End Hub', 'End Time', 'Overnight Hubs', 'Number of Hubs', 'Number of Edges',
             'Count Foot', 'Count Donkey', 'Count Ox', 'Hubs', 'Edges'])

        filename_transport_types = os.path.join(self.folder, f"{self.basename}_transport_types.csv")
        self.file_transport_types_csv = open(filename_transport_types, 'w', newline='')
        self.csv_writer_transport_types = csv.writer(self.file_transport_types_csv)
        self.csv_writer_transport_types.writerow(['ID', 'Edge', 'Foot', 'Donkey', 'Ox'])

        logger.info(f"Saving route data to {filename}gpkg, {filename}csv, and {filename_transport_types}.")

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        if self.skip:
            return agents_finished_for_today

        # initialize output
        if self.folder is None:
            self._initialize(config)

        self._persist_agents(agents_finished_for_today, config, context, current_day)

        return agents_finished_for_today

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context,
                          current_day: int) -> None:
        self.file_gpkg.close()
        self.file_routes_csv.close()
        self.file_transport_types_csv.close()

    def _persist_agents(self, agents: list[Agent], config: Configuration, context: Context, current_day: int):
        # aggregate the agents and save their data into a GeoPackage file
        agent_data = []

        for agent in agents:
            # ignore cancelled agents
            if agent.is_cancelled:
                continue

            # define finished status
            is_finished = agent.is_finished
            if not is_finished:
                if config.simulation_ends and agent.this_hub in config.simulation_ends:
                    is_finished = True

            node = self._save_route_origins(agent)

            # add to data, if finished
            if is_finished:
                data = self._get_agent_data(context, agent, node, current_day)
                agent_data.append(data)
                self.csv_writer_routes.writerow(data['properties'].values())

        self.file_gpkg.writerecords(agent_data)

    def _get_agent_data(self, context: Context, agent: Agent, node: dict, current_day: int):
        start_hub, end_hub, start_delta, end_delta = agent.get_start_end()

        edges = node['edges'].keys()

        lines: list[MultiLineString] = []
        count_foot = 0
        count_donkey = 0
        count_ox = 0
        for edge_name in edges:
            foot_flag = 0
            donkey_flag = 0
            ox_flag = 0

            lines.append(context.routes.es.find(name=edge_name)['geom'])
            if 'foot' in node['edges'][edge_name]:
                count_foot += 1
                foot_flag = 1
            if 'cart_donkey' in node['edges'][edge_name]:
                count_donkey += 1
                donkey_flag = 1
            if 'cart_oxen' in node['edges'][edge_name]:
                count_ox += 1
                ox_flag = 1

            # also write data to transport types csv
            self.csv_writer_transport_types.writerow([agent.uid, edge_name, foot_flag, donkey_flag, ox_flag])

        return {'geometry': MultiLineString(lines), 'properties': {
            'id': agent.uid,
            'last_transport_type': agent.transport_type,
            'variant_paths': node['count'],
            'length_hrs': int(math.floor(end_delta - start_delta)),
            'arrival_day': current_day,
            'arrival_hour': math.floor(agent.current_time % 24),
            'start_hubs': ', '.join(node['start_hubs']),
            'start_times': ', '.join(node['start_times']),
            'end_hub': end_hub,
            'end_time': str(node['end_time'].strftime('%Y-%m-%d %H:%M')),
            'overnight_hubs': ','.join(node['overnight_hubs']),
            'count_hubs': len(node['hubs']),
            'count_edges': len(edges),
            'count_foot': count_foot,
            'count_donkey': count_donkey,
            'count_ox': count_ox,
            'hubs': ','.join(list(node['hubs'])),
            'edges': ','.join(edges),
        }}

    def _save_route_origins(self, agent: Agent) -> dict:
        start_hub, _, start_delta, end_delta = agent.get_start_end()

        start_time = self.min_time + dt.timedelta(hours=start_delta)
        end_time = self.min_time + dt.timedelta(hours=end_delta)

        start_hubs = set()
        overnight_hubs = set()
        overnight_hubs.add(agent.route[0])
        start_times = set()
        min_time = start_time
        count = 0
        hubs = set()
        hubs.update(agent.route[::2])
        edges = {}
        for i, route in enumerate(agent.route[1::2]):
            edges[route] = {agent.transport_types[i]}

        for pid in agent.parents:
            parent = self.route_origins[pid]
            min_time = min(min_time, parent['min_time'])
            start_delta = min(start_delta, parent['start_delta'])
            start_hubs.update(parent['start_hubs'])
            start_times.update(parent['start_times'])
            overnight_hubs.update(parent['overnight_hubs'])
            count += parent['count']
            hubs.update(parent['hubs'])
            for route, transport_types in parent['edges'].items():
                if route in edges:
                    edges[route].update(transport_types)
                else:
                    edges[route] = transport_types.copy()

        # start?
        if len(start_hubs) == 0:
            start_hubs.add(start_hub)
            start_times.add(start_time.strftime('%Y-%m-%d %H:%M'))
        if count == 0:
            count = 1

        node = {
            'start_delta': start_delta,
            'end_delta': end_delta,
            'end_time': end_time,
            'overnight_hubs': overnight_hubs,
            'min_time': min_time,
            'start_hubs': start_hubs,
            'start_times': start_times,
            'count': count,
            'hubs': hubs,
            'edges': edges,
        }


        self.route_origins[agent.uid] = node

        return node