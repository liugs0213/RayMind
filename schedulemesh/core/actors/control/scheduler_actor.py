"""
SchedulerActor - 核心调度器实现。

支持：
- 基于优先级的任务队列（高优先级任务优先调度）
- 多标签队列管理
- 防饥饿机制（aging）
- 可插拔调度策略
"""

from __future__ import annotations

import heapq
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import ray
from ray.util import metrics

from schedulemesh.core.config import get_scheduling_config
from schedulemesh.core.scheduling import SchedulingStrategy, create_strategy

logger = logging.getLogger(__name__)


class PriorityTaskQueue:
    """
    优先级任务队列
    
    特性：
    - 按优先级（priority）降序排列，高优先级任务先出队
    - 支持 aging 机制防止低优先级任务饥饿
    - 使用 heapq 实现，时间复杂度 O(log n)
    """
    
    def __init__(self, aging_factor: float = 0.0001):
        """
        Args:
            aging_factor: 等待时间权重，用于防止饥饿（默认 0.0001）
                          有效优先级 = 原始优先级 + aging_factor × 等待时间（秒）
        """
        self._heap: List[Tuple[float, int, dict]] = []
        self._counter = 0  # 用于打破优先级相同时的平局（先进先出）
        self._aging_factor = aging_factor
    
    def push(self, task: dict, priority: float, submit_time: Optional[float] = None) -> None:
        """
        插入任务到优先级队列
        
        Args:
            task: 任务数据
            priority: 任务优先级
            submit_time: 提交时间（用于 aging）
        """
        if submit_time is None:
            submit_time = time.time()
        
        task_with_meta = dict(task)
        task_with_meta["_submit_time"] = submit_time
        task_with_meta["_original_priority"] = priority
        
        # 使用负优先级，因为 heapq 是最小堆，我们需要最大堆
        # counter 确保相同优先级时 FIFO
        heapq.heappush(self._heap, (-priority, self._counter, task_with_meta))
        self._counter += 1
    
    def pop(self) -> Optional[dict]:
        """
        弹出最高优先级的任务（考虑 aging）
        
        Returns:
            任务数据，如果队列为空则返回 None
        """
        if not self._heap:
            return None
        
        # 如果启用了 aging，重新计算所有任务的有效优先级
        if self._aging_factor > 0 and len(self._heap) > 1:
            self._recompute_priorities()
        
        _, _, task = heapq.heappop(self._heap)
        
        # 移除内部元数据
        task.pop("_submit_time", None)
        task.pop("_original_priority", None)
        
        return task
    
    def peek(self) -> Optional[dict]:
        """
        查看最高优先级任务，但不弹出
        
        Returns:
            任务数据的副本，如果队列为空则返回 None
        """
        if not self._heap:
            return None
        
        if self._aging_factor > 0 and len(self._heap) > 1:
            self._recompute_priorities()
        
        _, _, task = self._heap[0]
        return dict(task)
    
    def _recompute_priorities(self) -> None:
        """重新计算所有任务的有效优先级（考虑 aging）"""
        current_time = time.time()
        new_heap = []
        
        for neg_priority, counter, task in self._heap:
            original_priority = task.get("_original_priority", -neg_priority)
            submit_time = task.get("_submit_time", current_time)
            wait_time = current_time - submit_time
            
            # 有效优先级 = 原始优先级 + aging_factor × 等待时间
            effective_priority = original_priority + self._aging_factor * wait_time
            
            new_heap.append((-effective_priority, counter, task))
        
        heapq.heapify(new_heap)
        self._heap = new_heap
    
    def __len__(self) -> int:
        """返回队列长度"""
        return len(self._heap)
    
    def __bool__(self) -> bool:
        """队列是否非空"""
        return len(self._heap) > 0
    
    def is_empty(self) -> bool:
        """队列是否为空"""
        return len(self._heap) == 0

    def snapshot(self) -> dict:
        return {
            "aging_factor": self._aging_factor,
            "counter": self._counter,
            "items": [
                {
                    "priority": -neg_priority,
                    "counter": counter,
                    "task": dict(task),
                }
                for neg_priority, counter, task in self._heap
            ],
        }

    def restore(self, snapshot: dict) -> None:
        aging_factor = snapshot.get("aging_factor", self._aging_factor)
        self._aging_factor = float(aging_factor if aging_factor is not None else self._aging_factor)
        restored_counter = snapshot.get("counter", 0)
        restored_counter = int(restored_counter if restored_counter is not None else 0)
        items = snapshot.get("items", [])
        self._heap = []
        max_counter = restored_counter
        for item in items:
            priority = item.get("priority", 0.0)
            priority = float(priority if priority is not None else 0.0)
            counter = item.get("counter", 0)
            counter = int(counter if counter is not None else 0)
            task = item.get("task")
            task = dict(task if task is not None else {})
            heapq.heappush(self._heap, (-priority, counter, task))
            if counter > max_counter:
                max_counter = counter
        self._counter = max_counter + 1


@ray.remote
class SchedulerActor:
    """
    核心调度器 Actor
    
    功能：
    - 管理按 label 分组的优先级任务队列
    - 注册和匹配 Agent
    - 支持多种调度策略
    - 防饥饿机制（aging）
    """

    def __init__(self, aging_factor: float = 0.0001):
        """
        Args:
            aging_factor: 防饥饿系数，低优先级任务等待时间越长，有效优先级越高
        """
        # 按 label 分组的优先级队列 {label: PriorityTaskQueue}
        self.queues: Dict[str, PriorityTaskQueue] = {}
        self.aging_factor = aging_factor

        # 注册的 Agent {agent_name: agent_info}
        self.agents: Dict[str, Dict[str, Any]] = {}
        self.agent_last_heartbeat: Dict[str, float] = {}

        # 策略缓存
        self._strategy_cache: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], SchedulingStrategy] = {}
        config = get_scheduling_config()
        self.default_strategy = config.default_strategy
        self._state_path: Optional[Path] = None

        # 初始化 Ray metrics（在 actor 初始化时创建，避免重复创建）
        self.task_submit_latency_gauge = metrics.Gauge(
            name="schedulemesh_task_submit_latency_ms",
            description="Task submit latency (ms)",
            tag_keys=("label", "strategy")
        )
        self.task_schedule_latency_gauge = metrics.Gauge(
            name="schedulemesh_task_schedule_latency_ms",
            description="Task schedule latency (ms)",
            tag_keys=("label", "strategy", "agent")
        )
        self.schedule_success_counter = metrics.Counter(
            name="schedulemesh_schedule_success_count",
            description="Schedule success count",
            tag_keys=("label", "strategy")
        )

        logger.info(
            "SchedulerActor 初始化 aging_factor=%.6f (有效优先级 = 原始优先级 + %.6f × 等待秒数)",
            aging_factor,
            aging_factor,
        )
        self._load_state()

    @staticmethod
    def _selection_key(labels: Mapping[str, Any] | None) -> Tuple[Tuple[str, str], ...]:
        if not labels:
            return ()
        return tuple(sorted((str(key), str(value)) for key, value in labels.items()))

    def _strategy_for(self, name: str, labels: Mapping[str, Any] | None):
        key = (name, self._selection_key(labels))
        strategy = self._strategy_cache.get(key)
        if strategy is None:
            strategy = create_strategy(name, labels=labels)
            self._strategy_cache[key] = strategy
        return strategy

    def _matching_agents(self, labels: Mapping[str, Any] | None) -> List[Dict[str, Any]]:
        if not labels:
            return list(self.agents.values())

        required = dict(labels)
        matches: List[Dict[str, Any]] = []
        for agent in self.agents.values():
            agent_labels = dict(agent.get("labels") or {})
            if required.items() <= agent_labels.items():
                matches.append(agent)
        return matches

    def submit_task(
        self,
        label: str,
        payload: Any,
        labels: Optional[Dict[str, str]] = None,
        strategy: Optional[str] = None,
        priority: float = 0.0,
        task_id: Optional[str] = None,
    ) -> dict:
        """
        提交任务到指定 label 的优先级队列
        
        Args:
            label: 队列标签（例如 "gpu-work", "cpu-batch"）
            payload: 任务数据
            labels: 资源匹配标签（用于选择 Agent）
            strategy: 调度策略名称
            priority: 任务优先级（越高越优先）
            task_id: 任务ID（可选）
        
        Returns:
            提交结果，包含 task_id 和队列长度
        """
        # 记录提交开始时间
        start_time = time.time()
        # 获取或创建优先级队列
        if label not in self.queues:
            self.queues[label] = PriorityTaskQueue(aging_factor=self.aging_factor)
        queue = self.queues[label]
        
        strategy_name = (strategy or self.default_strategy).strip().lower()
        # 提前校验策略可用性（若无效会抛出异常）
        create_strategy(strategy_name, labels=labels)
        
        import uuid
        task_id = task_id or f"task-{uuid.uuid4().hex[:8]}"
        submit_time = time.time()
        
        task_data = {
            "task_id": task_id,
            "payload": payload,
            "labels": dict(labels or {}),
            "strategy": strategy_name,
            "priority": priority,
        }
        
        # 按优先级插入队列
        queue.push(task_data, priority, submit_time)
        
        # 记录提交耗时
        submit_latency_ms = (time.time() - start_time) * 1000
        
        # 使用 Ray metrics 记录性能指标
        self.task_submit_latency_gauge.set(
            submit_latency_ms, 
            tags={
                "label": label,
                "strategy": strategy_name
            }
        )
        
        logger.debug(
            "提交任务 task_id=%s label=%s priority=%.2f queue_length=%d latency=%.2fms",
            task_id,
            label,
            priority,
            len(queue),
            submit_latency_ms,
        )
        self._save_state()
        
        return {
            "success": True,
            "task_id": task_id,
            "queue_length": len(queue),
            "strategy": strategy_name,
            "priority": priority,
            "submit_latency_ms": submit_latency_ms,
        }

    def choose_task(self, label: str) -> dict:
        """
        从指定 label 的优先级队列中取出最高优先级任务并匹配 Agent
        
        Args:
            label: 队列标签
        
        Returns:
            调度结果，包含任务数据和分配的 Agent
        """
        # 记录调度开始时间
        start_time = time.time()
        queue = self.queues.get(label)
        if not queue or queue.is_empty():
            return {"success": False, "queue_length": 0}

        # 查看队首任务（最高优先级），但不弹出
        head = queue.peek()
        if head is None:
            return {"success": False, "queue_length": 0}
        
        target_labels: Dict[str, str] = dict(head.get("labels") or {})
        candidates = self._matching_agents(target_labels or None)
        
        if not candidates:
            return {
                "success": False,
                "queue_length": len(queue),
                "error": (
                    f"No registered agents match labels {target_labels}" if target_labels else "No agents registered"
                ),
            }

        strategy_name: str = head.get("strategy") or self.default_strategy
        strategy = self._strategy_for(strategy_name, target_labels or None)
        
        try:
            agent = strategy.select(candidates)
        except ValueError as exc:
            return {"success": False, "queue_length": len(queue), "error": str(exc)}

        # 匹配成功，从队列弹出任务
        entry = queue.pop()
        
        # 记录调度耗时
        schedule_latency_ms = (time.time() - start_time) * 1000
        
        # 使用 Ray metrics 记录性能指标
        self.task_schedule_latency_gauge.set(
            schedule_latency_ms,
            tags={
                "label": label,
                "strategy": strategy_name,
                "agent": agent.get("name", "unknown")
            }
        )
        
        # 记录调度成功次数
        self.schedule_success_counter.inc(
            tags={
                "label": label,
                "strategy": strategy_name
            }
        )
        
        logger.debug(
            "调度任务 task_id=%s priority=%.2f -> agent=%s queue_remaining=%d latency=%.2fms",
            entry.get("task_id"),
            entry.get("priority", 0.0),
            agent.get("name"),
            len(queue),
            schedule_latency_ms,
        )
        self._save_state()
        
        return {
            "success": True,
            "task": entry["payload"],
            "task_id": entry.get("task_id"),
            "priority": entry.get("priority", 0.0),
            "agent": dict(agent),
            "queue_length": len(queue),
            "strategy": strategy_name,
            "schedule_latency_ms": schedule_latency_ms,
        }

    def register_agent(self, agent: Dict[str, Any]) -> dict:
        name = agent.get("name")
        if not name:
            return {"success": False, "error": "Agent record missing 'name'."}

        snapshot = dict(agent)
        snapshot["labels"] = dict(agent.get("labels") or {})
        
        # 在内存中保留 handle，持久化时会自动排除（在_snapshot_state中处理）
        self.agents[name] = snapshot
        self.agent_last_heartbeat[name] = time.time()
        logger.debug("SchedulerActor registered agent=%s labels=%s", name, snapshot["labels"])
        self._save_state()
        
        return {"success": True, "agent": snapshot}

    def unregister_agent(self, name: str) -> dict:
        removed = self.agents.pop(name, None)
        if removed is None:
            return {"success": False, "error": f"Agent '{name}' not registered"}
        self.agent_last_heartbeat.pop(name, None)
        logger.debug("SchedulerActor unregistered agent=%s", name)
        self._save_state()
        return {"success": True}

    def update_agent_metrics(self, name: str, metrics: Mapping[str, Any]) -> dict:
        agent = self.agents.get(name)
        if agent is None:
            return {"success": False, "error": f"Agent '{name}' not registered"}
        agent["metrics"] = dict(metrics)
        heartbeat = metrics.get("timestamp")
        if heartbeat is None:
            heartbeat = time.time()
        self.agent_last_heartbeat[name] = float(heartbeat)
        logger.debug("SchedulerActor updated metrics for agent=%s metrics=%s", name, metrics)
        return {"success": True}

    # ------------------------------------------------------------------
    # Persistence & health helpers

    def _state_file(self) -> Optional[Path]:
        if self._state_path is None:
            return None
        self._state_path.mkdir(parents=True, exist_ok=True)
        return self._state_path / "scheduler-state.json"

    def _snapshot_state(self) -> dict:
        # 序列化时排除handle（无法JSON序列化）
        agents_snapshot = {}
        for name, agent in self.agents.items():
            agent_copy = {k: v for k, v in agent.items() if k != "handle"}
            agent_copy["last_heartbeat"] = self.agent_last_heartbeat.get(name)
            agents_snapshot[name] = agent_copy
        
        return {
            "queues": {label: queue.snapshot() for label, queue in self.queues.items()},
            "agents": agents_snapshot,
            "aging_factor": self.aging_factor,
        }

    def snapshot_state(self) -> dict:
        """Expose current state for testing/inspection."""
        return self._snapshot_state()

    def _save_state(self) -> None:
        state_file = self._state_file()
        if state_file is None:
            return
        try:
            state = self._snapshot_state()
            state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:  # pragma: no cover - defensive
            logger.exception("SchedulerActor 状态持久化失败")

    def _load_state(self) -> None:
        state_file = self._state_file()
        if state_file is None or not state_file.exists():
            return
        try:
            data = json.loads(state_file.read_text(encoding="utf-8"))
            aging_factor = data.get("aging_factor", self.aging_factor)
            self.aging_factor = float(aging_factor if aging_factor is not None else self.aging_factor)
            queues_payload = data.get("queues", {})
            restored: Dict[str, PriorityTaskQueue] = {}
            for label, snapshot in queues_payload.items():
                queue = PriorityTaskQueue(aging_factor=self.aging_factor)
                queue.restore(snapshot)
                restored[label] = queue
            self.queues = restored

            self.agents = {}
            self.agent_last_heartbeat = {}
            for name, agent_snapshot in (data.get("agents") or {}).items():
                snapshot_copy = dict(agent_snapshot)
                last_hb = snapshot_copy.pop("last_heartbeat", 0.0)
                last_hb = float(last_hb if last_hb is not None else 0.0)
                self.agents[name] = snapshot_copy
                if last_hb:
                    self.agent_last_heartbeat[name] = last_hb

            logger.info(
                "SchedulerActor 状态从 %s 恢复: %d queues / %d agents",
                state_file,
                len(self.queues),
                len(self.agents),
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception("SchedulerActor 状态恢复失败")
            self.queues = {}
            self.agents = {}
            self.agent_last_heartbeat = {}

    def configure(self, state_path: Optional[str]) -> dict:
        if state_path:
            self._state_path = Path(state_path).expanduser()
            self._load_state()
        else:
            self._state_path = None
        return {"success": True}

    def detect_stale_agents(self, timeout: float) -> dict:
        now = time.time()
        stale: List[Dict[str, Any]] = []
        active: List[Dict[str, Any]] = []
        for name, info in self.agents.items():
            snapshot = dict(info)
            last = self.agent_last_heartbeat.get(name, 0.0)
            snapshot["name"] = name
            snapshot["last_heartbeat"] = last
            snapshot["age"] = now - last if last else None
            if not last or now - last > timeout:
                stale.append(snapshot)
            else:
                active.append(snapshot)
        return {"timestamp": now, "stale": stale, "active": active}
