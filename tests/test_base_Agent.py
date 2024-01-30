# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

from sitt.base import Agent
import igraph as ig

def test_agent():
    agent = Agent('A', 'B', 'A-B')
    assert agent.uid is not None
    assert agent.this_hub == 'A'
    assert agent.next_hub == 'B'
    assert agent.route_key == 'A-B'
    assert type(agent.route_data) == ig.Graph
    assert agent.route_data.is_directed()
