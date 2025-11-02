# 自动化抢占任务提交功能

## 概述

`submit_task_with_preemption` 方法提供了一个完整的任务提交解决方案，它能够：

1. **自动资源检查**：检查目标资源池是否有足够的可用资源
2. **智能抢占决策**：当资源不足时，自动评估并选择最佳的抢占候选者
3. **无缝任务创建**：在抢占成功后，自动创建新的Agent来执行任务
4. **完整的错误处理**：提供详细的成功/失败信息和原因

## 核心特性

### 1. 参数校验
- 确保所有必要参数（`pool_name`, `resources`, `actor_class`, `task_id`）都已提供
- 自动设置默认优先级（5.0）和标签

### 2. 资源检查
- 使用 `ResourcePoolManagerActor.reserve_agent_slot()` 进行原子性资源预留
- 如果资源充足，直接创建Agent

### 3. 自动抢占
- 当资源不足时，调用 `PreemptionControllerActor.evaluate_preemption()` 评估抢占候选
- 支持基于优先级的抢占和基于标签的抢占
- 自动选择得分最高的候选者

### 4. 抢占执行
- 获取被抢占任务的Agent句柄
- 调用 `execute_preemption()` 执行抢占操作
- 自动删除被抢占的 Agent，释放底层 Ray 资源与配额

### 5. 重试创建
- 抢占成功后，再次尝试预留资源
- 创建新的Agent来执行任务
- 提交资源占用

## 使用方法

### 基本用法

```python
from schedulemesh.simple import SimpleScheduler

# 创建调度器，并确保资源池存在
scheduler = SimpleScheduler("my-scheduler")
scheduler.ensure_pool(
    name="my-pool",
    labels={"tier": "standard"},
    resources={"cpu": 2, "memory": 4.0},
)

# 配置抢占策略（可选）
scheduler.configure_preemption(
    preemption_aggressiveness="medium",
    label_preemption_rules={"tier": {"premium": ["standard"]}},
)

# 提交任务（支持自动抢占）
result = scheduler.submit(
    task_id="my-task-001",
    pool="my-pool",
    resources={"cpu": 1, "memory": 2.0},
    priority=8.0,  # 高优先级
    labels={"tier": "premium", "user": "alice"},
    actor_class=MyTaskActor,
    actor_kwargs={"duration": 60.0}
)

if result["success"]:
    print(f"任务提交成功: {result['agent']}")
else:
    print(f"任务提交失败: {result['error']}")
```

### 参数说明

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `task_id` | str | 是 | 任务的唯一标识符 |
| `pool` | str | 是 | 目标资源池名称 |
| `resources` | dict | 是 | 任务所需的资源规格 |
| `actor_class` | type | 是 | 要实例化的Ray Actor类 |
| `priority` | float | 否 | 任务优先级（默认5.0） |
| `labels` | dict | 否 | 任务标签，用于抢占决策 |
| `actor_args` | list | 否 | 传递给 Actor 构造函数的位置参数 |
| `actor_kwargs` | dict | 否 | 传递给Actor构造函数的关键字参数 |
| `agent_name` | str | 否 | Agent名称（自动生成） |

> 调度器会始终将 `name` 和 `labels` 作为前两个参数传给 `actor_class`，
> 额外的 `actor_args` / `actor_kwargs` 会在其后追加，因此自定义 Actor
> 需要至少接受 `name` 和 `labels` 这两个参数。

### 返回结果

成功时：
```python
{
    "success": True,
    "agent": {
        "name": "my-pool-agent-abc123",
        "handle": <ray.actor.ActorHandle>,
        "resources": {"cpu": 1, "memory": 2.0},
        "labels": {"pool": "my-pool", "tier": "premium"}
    }
}
```

失败时：
```python
{
    "success": False,
    "error": "Insufficient resources and no available preemption candidates",
    "reason": "preemption_failed"
}
```

## 抢占策略配置

### 基于优先级的抢占
```python
# 配置抢占策略
scheduler.update_preemption_policy(
    same_pool_priority_threshold=1.0,    # 同池内抢占阈值
    cross_pool_priority_threshold=5.0,  # 跨池抢占阈值
    enable_cross_pool_preemption=True
)
```

### 基于标签的抢占
```python
# 配置标签抢占规则
scheduler.update_preemption_policy(
    enable_label_preemption=True,
    label_preemption_rules={
        "tier": {
            "premium": ["standard", "batch"],
            "standard": ["batch"]
        },
        "user_type": {
            "admin": ["user", "guest"],
            "user": ["guest"]
        }
    }
)
```

### 抢占积极性级别
```python
# 使用预设的积极性级别
scheduler.update_preemption_policy(
    preemption_aggressiveness="high"  # "high", "medium", "low"
)
```

## 示例场景

### 场景1：资源充足
1. 任务提交 → 资源检查通过 → 直接创建Agent → 返回成功

### 场景2：资源不足，有可抢占任务
1. 任务提交 → 资源检查失败 → 评估抢占候选 → 执行抢占 → 释放资源 → 创建Agent → 返回成功

### 场景3：资源不足，无可抢占任务
1. 任务提交 → 资源检查失败 → 评估抢占候选 → 无候选者 → 返回失败

### 场景4：跨池抢占与手动干预
1. Premium 池资源不足 → 自动评估跨池候选（如 Standard 池的低优任务）
2. 自动抢占无法满足时，可使用 `RayScheduler.preempt_task(...)` 指定目标 Agent
3. 搭配 `RayScheduler.delete_agent(...)` 释放资源后，再提交高优任务到目标池
4. 可参考 `examples/automated_preemption_demo.py` 的第二阶段演示

## 最佳实践

1. **合理设置优先级**：确保重要任务有足够高的优先级
2. **使用标签分类**：通过标签实现细粒度的抢占控制
3. **监控抢占统计**：定期检查 `get_preemption_stats()` 了解系统行为
4. **处理失败情况**：总是检查返回结果的 `success` 字段
5. **资源规划**：合理规划资源池容量，减少抢占频率

## 注意事项

1. **原子性**：整个提交过程是原子性的，要么完全成功，要么完全失败
2. **资源管理**：系统会自动管理资源预留和释放，无需手动干预
3. **抢占影响**：被抢占的任务会被取消，需要重新提交
4. **性能考虑**：抢占操作有一定开销，应合理设置抢占策略
5. **跨池场景**：自动抢占会删除被占用的 Agent，如需保留请提前快照或启用自定义恢复逻辑
6. **错误恢复**：抢占失败时，系统会回滚所有操作，保持一致性

