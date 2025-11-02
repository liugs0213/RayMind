"""
Metrics reporting helpers for agent implementations.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import fields
from typing import Any, Deque, Dict, Mapping, Optional

import ray

from schedulemesh.core.entities.agent import AgentMetrics

logger = logging.getLogger(__name__)


class MetricsReportingAgent:
    """
    基础 Agent，实现统一的指标上报逻辑。

    该基类假设调度器的 supervisor actor 句柄在初始化时传入，随后在
    :meth:`report_metrics` 中异步调用 ``update_agent_metrics``。指标数据会
    同步到本地的 :class:`AgentMetrics`，便于业务侧自检。
    """

    def __init__(
        self,
        name: str,
        labels: Dict[str, str],
        supervisor: Optional[ray.actor.ActorHandle],
        *,
        report_interval: float = 5.0,
        max_pending_reports: int = 16,
    ) -> None:
        self.name = name
        self.labels = labels
        self.metrics = AgentMetrics()

        self._supervisor = supervisor
        self._report_interval = max(report_interval, 0.0)
        self._last_report_ts = 0.0
        self._latest_payload: Dict[str, Any] = {}
        self._pending_reports: Deque[Any] = deque()
        self._max_pending_reports = max(0, max_pending_reports)

        logger.debug(
            "MetricsReportingAgent[%s] initialized report_interval=%.2fs max_pending=%s supervisor=%s",
            name,
            self._report_interval,
            self._max_pending_reports,
            bool(supervisor),
        )

    # ------------------------------------------------------------------ #
    # Metrics helpers
    # ------------------------------------------------------------------ #

    def report_metrics(
        self,
        metrics: Mapping[str, Any],
        *,
        force: bool = False,
    ) -> bool:
        """
        异步上报指标给调度器，并在本地缓存最新指标。

        Args:
            metrics: 要上报的指标字典。
            force: 是否无视 ``report_interval`` 强制上报。

        Returns:
            bool 表示是否实际触发了一次远程上报。
        """
        if not isinstance(metrics, Mapping):
            raise TypeError("metrics must be a mapping")

        now = time.time()
        if (
            not force
            and self._report_interval > 0
            and (now - self._last_report_ts) < self._report_interval
        ):
            # 仍然更新本地缓存，便于下一次补充。
            self._latest_payload.update(metrics)
            self._update_local_metrics(metrics)
            return False

        payload: Dict[str, Any] = dict(self._latest_payload)
        payload.update(metrics)
        payload.setdefault("timestamp", now)

        # 本地缓存
        self._latest_payload = payload
        self._update_local_metrics(payload)
        self._last_report_ts = now

        if self._supervisor is None:
            logger.debug(
                "MetricsReportingAgent[%s] 无 supervisor 句柄，跳过指标上报 payload=%s",
                self.name,
                payload,
            )
            return False

        try:
            ref = self._supervisor.update_agent_metrics.remote(self.name, payload)
            if self._max_pending_reports > 0:
                self._pending_reports.append(ref)
                while len(self._pending_reports) > self._max_pending_reports:
                    self._pending_reports.popleft()
        except Exception:  # pragma: no cover - best effort logging
            logger.exception(
                "MetricsReportingAgent[%s] 上报指标失败 payload=%s", self.name, payload
            )
            return False

        logger.debug(
            "MetricsReportingAgent[%s] 上报指标 payload=%s force=%s",
            self.name,
            payload,
            force,
        )
        return True

    def flush_metrics(self) -> None:
        """等待挂起的指标上报完成。"""
        if not self._pending_reports or self._supervisor is None:
            return
        pending = list(self._pending_reports)
        self._pending_reports.clear()
        try:
            ray.get(pending)
        except Exception:  # pragma: no cover - best effort logging
            logger.exception("MetricsReportingAgent[%s] 刷新指标失败", self.name)

    def _update_local_metrics(self, metrics: Mapping[str, Any]) -> None:
        if not isinstance(metrics, Mapping):
            return
        for field in fields(AgentMetrics):
            if field.name in metrics:
                try:
                    setattr(self.metrics, field.name, metrics[field.name])
                except Exception:  # pragma: no cover - defensive
                    logger.debug(
                        "MetricsReportingAgent[%s] 忽略无法设置的字段 %s", self.name, field.name
                    )
