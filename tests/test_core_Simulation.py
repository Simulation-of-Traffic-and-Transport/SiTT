# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
import networkx as nx

from sitt import Agent, Configuration, Context, SetOfResults, Simulation
from sitt.modules.simulation_step import DummyForTests


def test_init():
    config = Configuration()
    context = Context()
    sim = Simulation(config, context)

    assert sim.current_day == 1
    assert sim.context == context
    assert sim.config == config


def test_check():
    config = Configuration()
    context = Context()
    sim = Simulation(config, context)

    assert not sim.check()
    config.simulation_start = 'TEST1'
    config.simulation_end = 'TEST2'
    context.routes = nx.MultiDiGraph()
    context.routes.add_nodes_from(['TEST1', 'TEST2'])
    assert sim.check()


def test_create_agents_on_node():
    config: Configuration = Configuration()
    context: Context = Context()
    context.routes = nx.MultiDiGraph()
    context.routes.add_edges_from([('START', 'N1'), ('START', 'N2'), ('N1', 'STOP'), ('N2', 'STOP')])
    sim: Simulation = Simulation(config, context)

    # test initial creation of agents
    assert len(sim.create_agents_on_node('START')) == 2
    assert len(sim.create_agents_on_node('N1')) == 1
    assert len(sim.create_agents_on_node('STOP')) == 0

    # test cloning of existing agents
    agent = Agent('START', '', '', current_time=8., max_time=16.)
    agents = sim.create_agents_on_node('START')
    for a in agents:
        assert a.current_day == agent.current_day
        assert a.current_time == agent.current_time
        assert a.this_hub == agent.this_hub
        assert a.next_hub != agent.next_hub
        assert a.uid != agent.uid


def test_prune_agent_list():
    # TODO: implement!
    pass


def _create_simulation_for_test_runs(time_taken_per_node: float = 8., force_stop_at_node: None | str = None) -> tuple[
    Simulation, SetOfResults, list[Agent], list[Agent]]:
    config: Configuration = Configuration()
    config.simulation_start = 'START'
    config.simulation_end = 'STOP'
    config.simulation_step.append(DummyForTests(time_taken_per_node, force_stop_at_node))
    context: Context = Context()
    context.graph = nx.MultiGraph()
    context.graph.add_nodes_from([
        ('START', {'overnight': 'y'}),
        ('PASS', {'overnight': 'n'}),
        ('STAY', {'overnight': 'y'}),
        ('STOP', {'overnight': 'y'}),
    ])
    context.routes = nx.MultiDiGraph()
    context.routes.add_edges_from([('START', 'PASS'), ('START', 'STAY'), ('PASS', 'STOP'), ('STAY', 'STOP')])

    results: SetOfResults = SetOfResults()
    agents_proceed: list[Agent] = []
    agents_finished_for_today: list[Agent] = []
    sim: Simulation = Simulation(config, context)

    return sim, results, agents_proceed, agents_finished_for_today


def test_run_single_step_start_pass1():
    # this is the simplest test case for the single day loop
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs(4.0)
    agent = Agent('START', 'PASS', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry() # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should be ready to proceed
    assert len(agents_proceed) == 1
    assert not len(agents_finished_for_today)
    assert agents_proceed[0].this_hub == 'PASS'
    assert agents_proceed[0].next_hub == 'STOP'
    assert agent.tries == 0
    assert agent.day_finished == -1
    assert agent.day_cancelled == -1
    assert agent.current_time == 12.0
    assert agent.state.time_taken == 4.0


def test_run_single_step_start_pass2():
    # test exceeds max time so agent should end and be reset to current state
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs(8.1)
    agent = Agent('START', 'PASS', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should be at end of day, and its try counter be increased
    assert not len(agents_proceed)
    assert len(agents_finished_for_today) == 1
    assert agents_finished_for_today[0] == agent
    assert agent.tries == 1
    assert agent.day_finished == -1
    assert agent.day_cancelled == -1


def test_run_single_step_start_pass3():
    # test is exactly max time - agent should end day as special case
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs()
    agent = Agent('START', 'PASS', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should be at end of day, and its try counter be increased
    assert not len(agents_proceed)
    assert len(agents_finished_for_today) == 1
    assert agents_finished_for_today[0] == agent
    assert agent.tries == 1
    assert agent.day_finished == -1
    assert agent.day_cancelled == -1


def test_run_single_step_start_stay1():
    # copy of simple run, only we take the other route and proceed to stay
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs(4.0)
    agent = Agent('START', 'STAY', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should be ready to proceed
    assert len(agents_proceed) == 1
    assert not len(agents_finished_for_today)
    assert agents_proceed[0].this_hub == 'STAY'
    assert agents_proceed[0].next_hub == 'STOP'
    assert agent.tries == 0
    assert agent.day_finished == -1
    assert agent.day_cancelled == -1
    assert agent.current_time == 12.0
    assert agent.state.time_taken == 4.0


def test_run_single_step_start_stay2():
    # test exceeds max time so agent should end and be reset to current state
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs(8.1)
    agent = Agent('START', 'STAY', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should be at end of day, and its try counter be increased
    assert not len(agents_proceed)
    assert len(agents_finished_for_today) == 1
    assert agents_finished_for_today[0] == agent
    assert agent.tries == 1
    assert agent.day_finished == -1
    assert agent.day_cancelled == -1


def test_run_single_step_start_stay3():
    # test is exactly max time - agent should end day as special case, stay overnight, ok
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs()
    agent = Agent('START', 'STAY', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should be ready to proceed
    assert not len(agents_proceed)
    assert len(agents_finished_for_today) == 1
    assert agents_finished_for_today[0].this_hub == 'STAY'
    assert agents_finished_for_today[0].next_hub == 'STOP'
    assert agent.tries == 0
    assert agent.day_finished == -1
    assert agent.day_cancelled == -1
    assert agent.current_time == 16.
    assert agent.state.time_taken == 8.


def test_run_single_step_arrival1():
    # text arrival at end point - agent should not try to continue
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs(4.0)
    agent = Agent('STAY', 'STOP', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should be set to be finished
    assert not len(agents_proceed)
    assert not len(agents_finished_for_today)
    assert agent.tries == 0
    assert agent.current_time == 12.0
    assert agent.state.time_taken == 4.0
    assert agent.day_finished == 1
    assert len(results.agents_finished) == 1
    assert results.agents_finished[0] == agent


def test_run_single_step_arrival2():
    # text arrival at end point - agent should not try to continue - exact match should also work!
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs()
    agent = Agent('STAY', 'STOP', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should be set to be finished
    assert not len(agents_proceed)
    assert not len(agents_finished_for_today)
    assert agent.tries == 0
    assert agent.current_time == 16.
    assert agent.state.time_taken == 8.
    assert agent.day_finished == 1
    assert len(results.agents_finished) == 1
    assert results.agents_finished[0] == agent


def test_run_single_step_forced_stop1():
    # test forced stop
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs(4.0, 'STAY')
    agent = Agent('STAY', 'STOP', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should have stopped here
    assert not len(agents_proceed)
    assert len(agents_finished_for_today) == 1
    assert agents_finished_for_today[0].this_hub == 'STAY'
    assert agents_finished_for_today[0].next_hub == 'STOP'
    assert agent.tries == 1
    assert agent.day_finished == -1
    assert agent.day_cancelled == -1
    assert agent.current_time == 8.
    assert agent.state.time_taken == 0.0


def test_run_single_step_forced_stop2():
    # test forced stop - exact time
    sim, results, agents_proceed, agents_finished_for_today = _create_simulation_for_test_runs(8., 'STAY')
    agent = Agent('STAY', 'STOP', '', current_time=8., max_time=16.)
    agent.add_first_route_data_entry()  # init correct data

    # simply test running the simulation without other definitions
    assert sim._run_single_step(agent, results, agents_proceed, agents_finished_for_today) is None

    # expectations: agent should have stopped here
    assert not len(agents_proceed)
    assert len(agents_finished_for_today) == 1
    assert agents_finished_for_today[0].this_hub == 'STAY'
    assert agents_finished_for_today[0].next_hub == 'STOP'
    assert agent.tries == 1
    assert agent.day_finished == -1
    assert agent.day_cancelled == -1
    assert agent.current_time == 8.
    assert agent.state.time_taken == 0.0


def test_end_day():
    # TODO: implement!
    pass


def test_output():
    # TODO: implement!
    pass
