"""
Unit tests for the ScheduleMesh façade helpers.
"""

from __future__ import annotations

import uuid

import pytest

from schedulemesh.core.agent_actor import AgentActor
from schedulemesh.core.controllers import ScheduleMesh


def _prepare_mesh(scheduler, *, agent_count: int = 2):
    pool_name = f"mesh-pool-{uuid.uuid4().hex[:6]}"
    scheduler.create_pool(
        name=pool_name,
        labels={"stage": "test"},
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=agent_count,
    )
    for index in range(agent_count):
        scheduler.create_agent(
            name=f"mesh-agent-{index}",
            pool=pool_name,
            actor_class=AgentActor,
        )
    mesh = ScheduleMesh(scheduler, default_pool=pool_name)
    return mesh, pool_name


def _create_pool_with_agents(scheduler, *, name: str, labels: dict[str, str], agent_count: int = 2):
    scheduler.create_pool(
        name=name,
        labels=labels,
        resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
        target_agents=agent_count,
    )
    for index in range(agent_count):
        scheduler.create_agent(
            name=f"{name}-agent-{index}",
            pool=name,
            actor_class=AgentActor,
        )


def test_all_broadcasts_to_all_agents(scheduler):
    mesh, _ = _prepare_mesh(scheduler)
    results = mesh.all().process(payload="broadcast")
    assert len(results) == 2
    for item in results:
        assert item["method"] == "process"
        assert item["kwargs"]["payload"] == "broadcast"


def test_choose_uses_round_robin_selection(scheduler):
    """Test round-robin strategy cycles through agents."""
    mesh, _ = _prepare_mesh(scheduler)
    proxy = mesh.choose(strategy="round_robin")

    first = proxy._pick_agent()["name"]
    second = proxy._pick_agent()["name"]
    third = proxy._pick_agent()["name"]
    
    # Round-robin should cycle through all agents
    assert first != second
    assert first == third  # Should return to first agent after cycling

    result = proxy.process(payload="single-call")
    assert result["method"] == "process"
    assert result["kwargs"]["payload"] == "single-call"


def test_choose_uses_label_round_robin_selection(scheduler):
    """验证标签轮询策略能正确处理合并后的标签。"""
    pool_gold = f"mesh-gold-{uuid.uuid4().hex[:6]}"
    pool_silver = f"mesh-silver-{uuid.uuid4().hex[:6]}"

    _create_pool_with_agents(scheduler, name=pool_gold, labels={"tier": "gold"})
    _create_pool_with_agents(scheduler, name=pool_silver, labels={"tier": "silver"})

    mesh = ScheduleMesh(scheduler)

    # 两次选择应严格在goal池内交替轮转。
    gold_proxy = mesh.choose(labels={"tier": "gold"}, strategy="label_round_robin")
    gold_first = gold_proxy._pick_agent()["name"]
    gold_second = gold_proxy._pick_agent()["name"]
    gold_third = gold_proxy._pick_agent()["name"]

    gold_agents = {f"{pool_gold}-agent-0", f"{pool_gold}-agent-1"}
    assert {gold_first, gold_second, gold_third} <= gold_agents
    assert gold_first != gold_second
    assert gold_first == gold_third

    # 独立选择银牌池，验证策略实例能够按 label 过滤。
    silver_proxy = mesh.choose(labels={"tier": "silver"}, strategy="label_round_robin")
    silver_names = {silver_proxy._pick_agent()["name"] for _ in range(4)}
    assert silver_names == {f"{pool_silver}-agent-0", f"{pool_silver}-agent-1"}


def test_choose_uses_least_busy_selection(scheduler):
    """Test least_busy strategy selects agent with lowest queue."""
    mesh, _ = _prepare_mesh(scheduler, agent_count=3)
    
    # Set different queue lengths for agents
    agents_response = scheduler.list_agents()
    agents = agents_response["agents"]
    
    # Simulate different queue lengths
    agents[0]["metrics"] = {"queue_length": 5}
    agents[1]["metrics"] = {"queue_length": 2}  # Should be selected
    agents[2]["metrics"] = {"queue_length": 8}
    
    # Mock the list_agents to return our modified agents
    original_list = scheduler.list_agents
    
    def mock_list_agents(pool=None, *, include_handle=False):
        return {"success": True, "agents": agents}
    
    scheduler.list_agents = mock_list_agents
    
    try:
        proxy = mesh.choose(strategy="least_busy")
        selected = proxy._pick_agent()
        # least_busy should select the agent with queue_length=2
        assert selected["metrics"]["queue_length"] == 2
    finally:
        scheduler.list_agents = original_list


def test_choose_uses_random_selection(scheduler):
    """Test random strategy can select different agents."""
    mesh, _ = _prepare_mesh(scheduler, agent_count=5)
    proxy = mesh.choose(strategy="random")
    
    # Random selection should produce varied results over multiple picks
    selected_names = {proxy._pick_agent()["name"] for _ in range(20)}
    
    # With 5 agents and 20 picks, we should see more than one agent selected
    assert len(selected_names) > 1


def test_choose_uses_power_of_two_selection(scheduler):
    """Test power_of_two strategy samples and picks least busy."""
    mesh, _ = _prepare_mesh(scheduler, agent_count=4)
    
    # Get agents and set metrics
    agents_response = scheduler.list_agents()
    agents = agents_response["agents"]
    
    for i, agent in enumerate(agents):
        agent["metrics"] = {"queue_length": i * 2}
    
    original_list = scheduler.list_agents
    
    def mock_list_agents(pool=None, *, include_handle=False):
        return {"success": True, "agents": agents}
    
    scheduler.list_agents = mock_list_agents
    
    try:
        proxy = mesh.choose(strategy="power_of_two")
        
        # Run multiple selections and check they tend toward lower queue lengths
        selections = [proxy._pick_agent()["metrics"]["queue_length"] for _ in range(30)]
        avg_queue = sum(selections) / len(selections)
        
        # Average should be skewed toward lower values (not uniform across 0,2,4,6)
        # Uniform average would be 3.0, power-of-two should be lower
        assert avg_queue < 3.5
    finally:
        scheduler.list_agents = original_list


def test_choose_with_invalid_strategy_raises(scheduler):
    """Test that invalid strategy name raises ValueError."""
    mesh, _ = _prepare_mesh(scheduler)
    
    with pytest.raises(ValueError, match="Unknown scheduling strategy"):
        mesh.choose(strategy="invalid_strategy")


def test_shard_with_default_dispatch_from_keyword(scheduler):
    mesh, _ = _prepare_mesh(scheduler)
    results = mesh.shard().process(shards=[{"id": 1}, {"id": 2}], suffix="ok")

    assert [entry["args"][0]["id"] for entry in results] == [1, 2]
    assert all(entry["kwargs"]["suffix"] == "ok" for entry in results)


def test_shard_with_custom_dispatch_function(scheduler):
    mesh, pool_name = _prepare_mesh(scheduler)

    def custom_dispatch(agents, args, kwargs):
        return [
            (0, ("first-shard",), {"label": agents[0]["labels"]["pool"]}),
            (agents[1], ("second-shard",), {"label": agents[1]["labels"]["pool"]}),
        ]

    results = mesh.shard(dispatch_fn=custom_dispatch).process()

    assert [entry["args"][0] for entry in results] == ["first-shard", "second-shard"]
    assert {entry["kwargs"]["label"] for entry in results} == {pool_name}


def test_selection_without_agents_raises(scheduler):
    mesh = ScheduleMesh(scheduler)
    with pytest.raises(RuntimeError):
        mesh.all(pool="non-existent").process(payload="fail")


def test_selection_filters_by_pool_and_labels(scheduler):
    pool_gold = f"mesh-gold-{uuid.uuid4().hex[:6]}"
    pool_silver = f"mesh-silver-{uuid.uuid4().hex[:6]}"

    _create_pool_with_agents(scheduler, name=pool_gold, labels={"stage": "test", "tier": "gold"})
    _create_pool_with_agents(scheduler, name=pool_silver, labels={"stage": "test", "tier": "silver"})

    mesh = ScheduleMesh(scheduler)

    gold_proxy = mesh.all(pool=pool_gold)
    gold_agents = {agent["name"] for agent in gold_proxy._agents}
    assert gold_agents == {f"{pool_gold}-agent-0", f"{pool_gold}-agent-1"}

    silver_proxy = mesh.all(labels={"pool": pool_silver})
    silver_agents = {agent["name"] for agent in silver_proxy._agents}
    assert silver_agents == {f"{pool_silver}-agent-0", f"{pool_silver}-agent-1"}

    chooser = mesh.choose(labels={"pool": pool_gold})
    chosen_name = chooser._pick_agent()["name"]
    assert chosen_name in gold_agents

    tier_gold_proxy = mesh.all(labels={"tier": "gold"})
    assert {agent["name"] for agent in tier_gold_proxy._agents} == gold_agents

    tier_silver_proxy = mesh.all(labels={"tier": "silver"})
    assert {agent["name"] for agent in tier_silver_proxy._agents} == {f"{pool_silver}-agent-0", f"{pool_silver}-agent-1"}
