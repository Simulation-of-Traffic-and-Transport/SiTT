# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
River test simulator to check how a hypothetical simple agent would behave in a river network.
"""
import copy
import pickle

import igraph as ig
from sitt import PathWeeder, convert_graph_to_shapefile

# # Variant 1
# from_node = "river-1-142988"  # Lavant, Start recht weit oben (Bärnthal)
# to_node = "river-1-150651"  # St. Gertraud im Lavanttal
# variant_name = "variante1"

# Variant 2
from_node = "river-1-56001"  # Gurk, Ausgang Wörthersee
to_node = "river-1-129452"  # Zufluss Gurk in die Drau
variant_name = "variante2"

graph_file = "graph_dump_1_calculated.pickle"
k_shortest = 100  # try out how many shortest paths to compute

# defined data
base_kph = 3.  # min speed in km/h (base paddle speed)
max_kph = 20.  # max speed in km/h
max_depth = 0.25  # max depth in meters (for ships)
max_length = 5.  # max width/length in meters (for ships)

crs_no = 4326
crs_to = 32633

max_difference_time = 10.  # maximum time difference in seconds to consider when checking agent behavior

base_m_per_s = base_kph * 1000 / 3600  # base speed
max_m_per_s = max_kph * 1000 / 3600

# load graph
graph: ig.Graph = pickle.load(open(graph_file, 'rb'))

# get nodes to consider
edge_set: set[int] = set()
node_set: set[int] = set()

path_weeder: PathWeeder = PathWeeder(graph)
path_weeder.init(crs_no, crs_to)
for best_path in path_weeder.get_k_paths(from_node, to_node, k_shortest).paths:
    edge_set = edge_set.union(set(best_path[1]))

for eid in edge_set:
    node_set.add(graph.es[eid].source)
    node_set.add(graph.es[eid].target)

# for path in graph.get_k_shortest_paths(from_node, to_node, k=k_shortest):
#     for node_id in path:
#         node_set.add(node_id)

# create a subgraph of the nodes and edges in the shortest paths
sub_graph = graph.subgraph(node_set)

convert_graph_to_shapefile(sub_graph, ".", "variante2.shp")

print("Considering", len(sub_graph.vs), "nodes and", len(sub_graph.es), "paths from", from_node, "to", to_node + ".")

id_counter = 0


def get_opposite_node_in_edge(edge: ig.Edge, node: str) -> str:
    """
    Get the opposite node in an edge.

    :param edge: edge
    :param node: node
    :return:
    """
    if edge.target_vertex['name'] == node:
        return edge.source_vertex['name']
    if edge.source_vertex['name'] == node:
        return edge.target_vertex['name']
    raise ValueError('Node not in edge')


def generate_id() -> str:
    """This utility function will generate uids for agents in increasing numerical order, padded with leading zeros."""
    global id_counter

    id_counter += 1
    return str(id_counter).zfill(6)


class Agent:
    def __init__(self, this_hub: str, next_hub: str, route_key: str, current_time: float = 0.):
        self.uid: str = generate_id()
        """unique id"""

        self.this_hub: str = this_hub
        """Current hub"""
        self.next_hub: str = next_hub
        """Destination hub"""
        self.route_key: str = route_key
        """Key id of next/current route between hubs ("name" attribute of edge)"""
        self.last_route: str | None = None
        """Key if of last route taken"""
        self.current_time: float = current_time
        """Current time stamp of agent during this day"""
        self.targets_visited: list[str] = []
        """Visited nodes/hubs"""
        self.length_m: float = 0.
        """length of current route in meters"""

    def __repr__(self) -> str:
        return f'Agent {self.uid} ({self.this_hub}->{self.next_hub} [{self.route_key}]) [{self.current_time:.2f}/{self.length_m:.2f}m)]'


junctions: set[str] = set()


def create_agents_on_node(g: ig.Graph, hub: str, agent_to_clone: Agent | None = None, current_time: float = 0.) -> list[
    Agent]:
    global junctions

    agents: list[Agent] = []

    # create new agent if none is defined
    if agent_to_clone is None:
        agent_to_clone = Agent(hub, '', '', current_time=current_time)

    # create new agent for each outbound edge
    first = True  # flag for first agent (skip deep copy)
    for edge in g.incident(hub):
        e = g.es[edge]
        target = e['flow_to']

        # check depth and width
        if e['depth_m'] < max_depth or e['min_width'] < max_length:
            continue

        # very slow speed?
        if e['flow_rate'] == 0 and target == hub:
            target = get_opposite_node_in_edge(e, hub)

        # skip if wrong direction (unless no speed) or target has been visited before
        if target == hub or target in agent_to_clone.targets_visited:
            continue

        # create new agent for each option
        if first:
            new_agent = agent_to_clone  # shallow copy for first agent
            first = False
        else:
            new_agent = copy.deepcopy(agent_to_clone)
            new_agent.uid = generate_id()  # unique id for each new agent
        new_agent.this_hub = hub
        new_agent.next_hub = target
        new_agent.route_key = e['name']  # name of edge

        agents.append(new_agent)

    # check if this node is a junction (multiple outgoing edges) - for counting purposes
    if len(agents) > 1:
        junctions.add(hub)

    return agents


finished_agents = []


# actual simulation entry
def advance_agent(g: ig.Graph, agent: Agent) -> Agent | None:
    # get next edge based on route key
    edge = g.es.find(name=agent.route_key)
    speed = edge['flow_rate']
    if speed < base_m_per_s:
        speed = base_m_per_s
    if speed > max_m_per_s:
        speed = max_m_per_s

    # advance
    time_delta_h = edge['length'] / speed / 3600
    agent.current_time += time_delta_h

    agent.length_m += edge['length']
    agent.last_route = agent.route_key
    agent.targets_visited.append(agent.this_hub)
    agent.this_hub = agent.next_hub
    agent.next_hub = ''

    # end reached?
    if agent.this_hub == to_node:
        return None

    return agent


# keeps visits per node
node_visits: dict[str, list[float]] = {}


def weed_out_similar_agents(agents: list[Agent]) -> list[Agent]:
    agents_to_return: list[Agent] = []

    for agent in agents:
        # check if node has been visited before and if the difference in time is less than certain time in seconds seconds
        if agent.this_hub in node_visits:
            found = False
            for time in node_visits[agent.this_hub]:
                if abs(agent.current_time - time) * 60 < max_difference_time:
                    found = True
                    break

            if not found:
                agents_to_return.append(agent)
        # add to node visits
        else:
            agents_to_return.append(agent)

    for agent in agents_to_return:
        if agent.this_hub not in node_visits:
            node_visits[agent.this_hub] = [agent.current_time]
        else:
            node_visits[agent.this_hub].append(agent.current_time)

    return agents_to_return


# start agents
agents = create_agents_on_node(sub_graph, from_node)

# sim loop
while len(agents) > 0:
    agents_continuing: list[Agent] = []

    for agent in agents:
        a = advance_agent(sub_graph, agent)
        if a is not None:
            for new_agent in create_agents_on_node(sub_graph, a.this_hub, a, a.current_time):
                agents_continuing.append(new_agent)
        else:
            finished_agents.append(agent)

    # new list of agents for next step
    agents = weed_out_similar_agents(agents_continuing)
    print(len(agents), "agents remaining.")

print("Junctions checked:", len(junctions))

if len(finished_agents) == 0:
    print("********* No agents could finish.")
    exit(0)

finished_agents.sort(key=lambda x: x.current_time)
print(finished_agents[0].current_time * 60, finished_agents[0].length_m)
print(finished_agents[-1].current_time * 60, finished_agents[-1].length_m)

# length differences
finished_agents.sort(key=lambda x: x.length_m)
print(finished_agents[0].length_m)
print(finished_agents[-1].length_m)
