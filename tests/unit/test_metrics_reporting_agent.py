import time

import pytest
import ray

from schedulemesh.core.agents import MetricsReportingAgent


class DummySupervisor:
    """Fake supervisor that records update_agent_metrics calls."""

    def __init__(self):
        self.calls = []

    def update_agent_metrics(self, name, metrics):
        self.calls.append((name, metrics))
        return {"success": True}

    def update_agent_metrics_remote(self, name, metrics):
        return ray.put(self.update_agent_metrics(name, metrics))


@ray.remote
class DummySupervisorActor:
    def __init__(self):
        self.calls = []

    def update_agent_metrics(self, name, metrics):
        self.calls.append((name, metrics))
        return {"success": True}

    def get_calls(self):
        return self.calls


class DummyAgent(MetricsReportingAgent):
    def __init__(self, supervisor=None, **kwargs):
        super().__init__(
            name="dummy",
            labels={"stage": "unit"},
            supervisor=supervisor,
            **kwargs,
        )


@pytest.fixture
def supervisor_actor(ray_runtime):
    actor = DummySupervisorActor.remote()
    try:
        yield actor
    finally:
        ray.kill(actor, no_restart=True)


def test_report_metrics_throttled(ray_runtime, supervisor_actor):
    agent = DummyAgent(supervisor=supervisor_actor, report_interval=1.0)

    # First call should trigger remote update.
    assert agent.report_metrics({"queue_length": 1}) is True
    # Second call within interval should be throttled.
    assert agent.report_metrics({"queue_length": 2}) is False

    time.sleep(1.1)
    assert agent.report_metrics({"queue_length": 3}) is True

    calls = ray.get(supervisor_actor.get_calls.remote())
    # Expect two actual remote updates: initial and post-sleep.
    assert len(calls) == 2
    assert calls[-1][1]["queue_length"] == 3


def test_report_metrics_force(ray_runtime, supervisor_actor):
    agent = DummyAgent(supervisor=supervisor_actor, report_interval=60.0)
    assert agent.report_metrics({"throughput": 100}, force=True) is True

    calls = ray.get(supervisor_actor.get_calls.remote())
    assert len(calls) == 1
    assert calls[0][1]["throughput"] == 100


def test_report_metrics_without_supervisor(ray_runtime):
    agent = DummyAgent(supervisor=None, report_interval=0.0)
    assert agent.report_metrics({"loss": 0.5}) is False


def test_flush_metrics(ray_runtime, supervisor_actor):
    agent = DummyAgent(supervisor=supervisor_actor, report_interval=0.0)
    assert agent.report_metrics({"gpu_utilization": 0.7}) is True

    # At least one remote call should be pending.
    agent.flush_metrics()
    calls = ray.get(supervisor_actor.get_calls.remote())
    assert calls and calls[0][1]["gpu_utilization"] == 0.7
