"""Rendering helpers for friendly CLI/demo output."""

from __future__ import annotations

from typing import Any, Dict


def pretty_print_stats(stats: Dict[str, Any], title: str) -> None:
    print(title)
    print(f"  - 总抢占次数: {stats.get('total_preemptions', 0)}")
    print(f"  - 同池抢占: {stats.get('same_pool_preemptions', 0)}")
    print(f"  - 跨池抢占: {stats.get('cross_pool_preemptions', 0)}")
    print(f"  - 当前登记运行任务: {stats.get('running_tasks', 0)}")
    recent = stats.get("recent_preemptions") or []
    if recent:
        print("  - 最近抢占记录:")
        for record in recent:
            cancel = "成功" if record.get("cancel_success") else "失败"
            print(
                f"    • 任务 {record.get('task_id')}@{record.get('pool_name')}"
                f" 原因={record.get('reason')} 取消={cancel}"
            )
    else:
        print("  - 最近抢占记录: 无")
    print()


def describe_candidates(title: str, evaluation: Dict[str, Any]) -> None:
    print(title)
    if not evaluation.get("should_preempt"):
        print("  - 无可抢占对象\n")
        return
    candidates = evaluation.get("candidates") or []
    print(f"  - 候选数量: {len(candidates)}")
    for idx, candidate in enumerate(candidates, start=1):
        print(
            f"    • #{idx} 任务={candidate.get('task_id')}"
            f" Agent={candidate.get('agent_name')} Pool={candidate.get('pool_name')}"
            f" 得分={candidate.get('preempt_score'):.2f} 原因={candidate.get('reason')}"
        )
    print()


def describe_submission(result: Dict[str, Any], title: str) -> None:
    print(title)
    if not result.get("success"):
        print(f"  - 提交失败: {result.get('error')} (reason={result.get('reason')})\n")
        return
    agent = result.get("agent", {})
    print("  - 提交成功")
    print(f"    • Agent: {agent.get('name')} @ {agent.get('labels', {}).get('pool')}")
    print(f"    • 资源: {agent.get('resources')}")
    print(f"    • Ray options: {agent.get('options')}\n")
