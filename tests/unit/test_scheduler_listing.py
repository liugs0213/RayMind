import ray
import pytest

from schedulemesh.simple import SimpleScheduler


@pytest.mark.parametrize("include_handle", [False, True])
def test_simple_scheduler_listings(include_handle):
    ray.init(local_mode=True, ignore_reinit_error=True, num_cpus=2)
    scheduler = SimpleScheduler("listings-test")

    try:
        # 创建一个简单的 pool 和 agent
        pool_name = "pool-listing"
        scheduler.ensure_pool(
            name=pool_name,
            resources={"cpu": 1.0, "memory": 512.0},
        )

        submission = scheduler.submit(
            task_id="agent-listing",
            pool=pool_name,
            actor_class=SimpleDemoActor,
            resources={"cpu": 1.0, "memory": 512.0},
            auto_register=False,
        )
        assert submission.get("success")
        created_agent_name = submission["agent"]["name"]

        pools = scheduler.list_pools()
        assert pools["success"]
        pool_names = {pool["name"] for pool in pools["pools"]}
        assert pool_name in pool_names

        agents = scheduler.list_agents(pool=pool_name, include_handle=include_handle)
        assert agents["success"]
        agent_names = {agent["name"] for agent in agents["agents"]}
        assert created_agent_name in agent_names

    finally:
        scheduler.shutdown()
        ray.shutdown()


@ray.remote
class SimpleDemoActor:
    def __init__(self, name: str, labels: dict[str, str]):
        self.name = name
        self.labels = labels
