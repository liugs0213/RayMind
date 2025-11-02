from pathlib import Path
from textwrap import dedent

import pytest

from schedulemesh.core.config import get_scheduling_config, reset_scheduling_config
from schedulemesh.core.scheduling import available_strategies, create_strategy
from schedulemesh.core.scheduling.strategy import SchedulingStrategy


class CustomStrategy(SchedulingStrategy):
    """Simple custom strategy used for config tests."""

    def select(self, agents):
        if not agents:
            raise ValueError("no agents")
        return agents[0]


@pytest.fixture(autouse=True)
def clear_config(monkeypatch):
    monkeypatch.delenv("SCHEDULEMESH_CONFIG", raising=False)
    reset_scheduling_config()
    yield
    reset_scheduling_config()


def test_load_custom_strategy(tmp_path: Path, monkeypatch):
    config_file = tmp_path / "schedulemesh.yaml"
    config_file.write_text(
        dedent(
            f"""
            scheduling:
              default_strategy: custom_strategy
              strategies:
                - name: custom_strategy
                  import: tests.unit.test_strategy_config:CustomStrategy
            """
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("SCHEDULEMESH_CONFIG", str(config_file))
    reset_scheduling_config()

    cfg = get_scheduling_config()
    assert cfg.default_strategy == "custom_strategy"
    assert "custom_strategy" in available_strategies()

    strategy = create_strategy("custom_strategy")
    assert isinstance(strategy, SchedulingStrategy)
    assert strategy.__class__.__name__ == "CustomStrategy"


def test_disable_strategy(tmp_path: Path, monkeypatch):
    config_file = tmp_path / "schedulemesh.yaml"
    config_file.write_text(
        dedent(
            """
            scheduling:
              default_strategy: least_busy
              strategies:
                - name: random
                  enabled: false
            """
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("SCHEDULEMESH_CONFIG", str(config_file))
    reset_scheduling_config()
    get_scheduling_config()

    assert "random" not in available_strategies()
    with pytest.raises(ValueError):
        create_strategy("random")
