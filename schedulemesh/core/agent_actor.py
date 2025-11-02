"""
AgentActor skeleton.

Actual implementations will execute workloads and collect metrics.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

import ray

from schedulemesh.core.agents import MetricsReportingAgent

logger = logging.getLogger(__name__)


@ray.remote
class AgentActor(MetricsReportingAgent):
    """Placeholder agent actor，演示基本任务处理与指标上报。"""

    def __init__(
        self,
        name: str,
        labels: Dict[str, str],
        supervisor: Optional[ray.actor.ActorHandle] = None,
        *,
        report_interval: float = 5.0,
        max_pending_reports: int = 16,
    ):
        super().__init__(
            name,
            labels,
            supervisor,
            report_interval=report_interval,
            max_pending_reports=max_pending_reports,
        )
        
        # 运行中的任务 {task_id: task_info}
        self.running_tasks: Dict[str, Dict[str, Any]] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._task_threads: Dict[str, threading.Thread] = {}
        
        logger.info("AgentActor[%s] initialised", name)

    def invoke(self, method: str, *args, **kwargs):
        logger.info("AgentActor[%s] invoked %s", self.name, method)
        return {"method": method, "args": args, "kwargs": kwargs}
    
    def process(
        self,
        payload: Any,
        task_id: Optional[str] = None,
        priority: float = 0.0,
        simulated_duration: float = 0.0,
    ):
        """
        处理任务（带优先级与可选模拟时长）。

        Args:
            payload: 任务数据
            task_id: 任务ID（可选），传入后任务会进入可取消列表
            priority: 任务优先级
            simulated_duration: 模拟占用时间（秒），>0 时以线程睡眠模拟长耗时
        """
        if task_id:
            self.running_tasks[task_id] = {
                "task_id": task_id,
                "priority": priority,
                "payload": payload,
                "simulated_duration": simulated_duration,
                "start_time": time.time(),
            }
            self._locks[task_id] = threading.Lock()
            logger.debug("AgentActor[%s] 开始任务 task_id=%s priority=%.2f", self.name, task_id, priority)

        def _run_task():
            try:
                if simulated_duration > 0:
                    logger.debug(
                        "AgentActor[%s] 模拟执行任务 task_id=%s duration=%.2fs",
                        self.name,
                        task_id,
                        simulated_duration,
                    )
                    time.sleep(simulated_duration)
            finally:
                if task_id and task_id in self.running_tasks:
                    # 获取已存在的锁，如果不存在则跳过（可能已被取消）
                    lock = self._locks.get(task_id)
                    if lock is not None:
                        with lock:
                            self.running_tasks.pop(task_id, None)
                            self._locks.pop(task_id, None)
                            self._task_threads.pop(task_id, None)
                            logger.debug("AgentActor[%s] 完成任务 task_id=%s", self.name, task_id)

        if task_id and simulated_duration > 0:
            worker = threading.Thread(target=_run_task, name=f"task-{task_id}", daemon=True)
            self._task_threads[task_id] = worker
            worker.start()
        else:
            _run_task()

        return {"method": "process", "kwargs": {"payload": payload}}
    
    def cancel(self, task_id: str) -> dict:
        """
        取消正在运行的任务
        
        Args:
            task_id: 要取消的任务ID
            
        Returns:
            {"success": bool, "task_info": dict}
        """
        lock = self._locks.get(task_id)
        if lock is None or task_id not in self.running_tasks:
            logger.warning("AgentActor[%s] 取消任务失败，任务不存在 task_id=%s", self.name, task_id)
            return {"success": False, "error": f"Task '{task_id}' not found"}

        with lock:
            task_info = self.running_tasks.pop(task_id, None)
            thread = self._task_threads.pop(task_id, None)
            self._locks.pop(task_id, None)

        if thread and thread.is_alive():
            logger.debug("AgentActor[%s] 等待任务线程结束 task_id=%s", self.name, task_id)
            thread.join(timeout=0.1)

        logger.info("AgentActor[%s] 任务已取消 task_id=%s", self.name, task_id)
        return {"success": True, "task_info": task_info}
    
    def get_running_tasks(self) -> dict:
        """获取当前运行中的任务列表"""
        return {
            "agent_name": self.name,
            "running_tasks": list(self.running_tasks.values()),
            "count": len(self.running_tasks),
        }


__all__ = ["AgentActor", "MetricsReportingAgent"]
