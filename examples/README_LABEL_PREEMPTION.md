# 基于 Label 的抢占功能

## 概述

ScheduleMesh 支持基于 Label 的细粒度抢占策略，允许你定义不受 pool 边界限制的抢占规则。

## 核心特性

### 1. 多维度 Label 规则

可以基于任意 label 定义抢占层次关系：

```python
scheduler.update_preemption_policy(
    label_preemption_rules={
        # 服务等级抢占
        "tier": {
            "premium": ["standard", "batch"],
            "standard": ["batch"],
        },
        # 用户类型抢占
        "user_tier": {
            "admin": ["vip", "regular"],
            "vip": ["regular"],
        },
        # 优先级分类抢占
        "priority_class": {
            "critical": ["high", "normal", "low"],
            "high": ["normal", "low"],
            "normal": ["low"],
        },
    },
    label_priority_threshold=0.5,
    enable_label_preemption=True,
)
```

### 2. Label 抢占优先于 Pool 抢占

抢占评估的优先级顺序：

1. **Label 抢占**（最高优先级）
   - 只需满足较低的 `label_priority_threshold`
   - 不受 pool 边界限制
   - 可以跨 pool 抢占

2. **Pool 抢占**（回退机制）
   - 同 pool 抢占：`same_pool_priority_threshold`
   - 跨 pool 抢占：`cross_pool_priority_threshold`（通常较高）

### 3. 灵活配置

- `label_preemption_rules`: 定义 label 的抢占层次
- `label_priority_threshold`: Label 抢占的优先级阈值（默认 0.5）
- `enable_label_preemption`: 是否启用 label 抢占（默认 True）

## 使用场景

### 场景 1：分级服务

```python
# 配置 tier 抢占规则
scheduler.update_preemption_policy(
    label_preemption_rules={
        "tier": {
            "premium": ["standard", "batch"],
            "standard": ["batch"],
        }
    },
    label_priority_threshold=0.5,
)

# premium 任务可以抢占 standard 任务，即使优先级相同
scheduler.register_running_task(
    task_id="standard-job",
    agent_name="worker-1",
    pool_name="pool-a",
    priority=5.0,
    labels={"tier": "standard"},
)

eval_result = scheduler.evaluate_preemption(
    incoming_task_priority=5.0,  # 优先级相同
    incoming_task_pool="pool-a",
    incoming_task_labels={"tier": "premium"},
)

assert eval_result["should_preempt"] is True  # 可以抢占
```

### 场景 2：多租户隔离

```python
# 配置租户抢占规则
scheduler.update_preemption_policy(
    label_preemption_rules={
        "user_tier": {
            "admin": ["vip", "regular"],
            "vip": ["regular"],
        }
    },
)

# admin 用户可以抢占 regular 用户，即使优先级更低
scheduler.register_running_task(
    task_id="regular-task",
    agent_name="worker",
    pool_name="shared-pool",
    priority=10.0,
    labels={"user_tier": "regular"},
)

eval_result = scheduler.evaluate_preemption(
    incoming_task_priority=6.0,  # 优先级更低
    incoming_task_pool="shared-pool",
    incoming_task_labels={"user_tier": "admin"},
)

assert eval_result["should_preempt"] is True  # 可以抢占
```

### 场景 3：跨 Pool 的 Label 抢占

```python
# 配置策略
scheduler.update_preemption_policy(
    label_preemption_rules={
        "tier": {"premium": ["standard"]},
    },
    label_priority_threshold=0.5,
    cross_pool_priority_threshold=10.0,  # 跨 pool 需要高优先级差
)

# Pool A 的 standard 任务
scheduler.register_running_task(
    task_id="standard-in-a",
    agent_name="agent-a",
    pool_name="pool-a",
    priority=5.0,
    labels={"tier": "standard"},
)

# Pool B 的 premium 任务（优先级差不足以跨 pool 抢占）
eval_result = scheduler.evaluate_preemption(
    incoming_task_priority=7.0,  # 差值 2.0 < 10.0
    incoming_task_pool="pool-b",
    incoming_task_labels={"tier": "premium"},
)

# 由于 label 规则，仍然可以抢占！
assert eval_result["should_preempt"] is True
assert eval_result["candidates"][0]["reason"] == "label_based_preemption"
```

## 完整示例

运行完整的演示程序：

```bash
python examples/label_preemption_demo.py
```

该示例包含：
1. 基于 tier label 的分级抢占
2. 基于 priority_class 的抢占
3. 基于 user_tier 的多租户抢占
4. Label 抢占优先于 Pool 抢占的演示

## 测试

运行 label 抢占的集成测试：

```bash
pytest tests/integration/test_label_preemption.py -v
```

测试覆盖：
- 基于 tier label 的分级抢占
- 基于 priority_class label 的抢占
- Label 抢占优先于 pool 抢占
- 多维度 label 抢占规则
- 禁用 label 抢占回退到 pool 抢占
- Label 抢占统计

## 配置参数

### label_preemption_rules

定义 label 的抢占层次关系。

格式：
```python
{
    "label_key": {
        "high_value": ["low_value1", "low_value2"],
        "medium_value": ["low_value1"],
    }
}
```

示例：
```python
{
    "tier": {
        "premium": ["standard", "batch"],  # premium 可以抢占 standard 和 batch
        "standard": ["batch"],             # standard 只能抢占 batch
    }
}
```

### label_priority_threshold

Label 抢占所需的最小优先级差值。通常设置为较低的值（如 0.5），允许在满足 label 规则的情况下更容易触发抢占。

### enable_label_preemption

是否启用 label 级别的抢占。设置为 `False` 时，系统会回退到传统的 pool 级别抢占。

## 最佳实践

1. **合理设置阈值**
   - `label_priority_threshold` 通常设置为 0.5-1.0
   - 比 `same_pool_priority_threshold` 更低
   - 确保 label 规则能够有效触发

2. **明确的 Label 层次**
   - 定义清晰的服务等级（如 premium > standard > batch）
   - 避免循环依赖（A 抢占 B，B 抢占 A）
   - 使用有意义的 label 名称

3. **多维度规则**
   - 可以同时配置多个 label 的抢占规则
   - 只要任一 label 规则匹配即可触发抢占
   - 适用于复杂的多租户场景

4. **监控和调试**
   - 使用 `get_preemption_stats()` 监控抢占统计
   - 检查 `reason` 字段确认抢占类型
   - 根据实际情况调整规则和阈值

## 文档

详细文档请参考：
- [抢占功能使用指南](../docs/preemption_guide.md)
- [API 参考文档](../docs/api/schedule_mesh.md)

## 相关示例

- `preemption_demo.py` - 基础抢占功能演示
- `priority_scheduling_demo.py` - 优先级调度演示
- `label_preemption_demo.py` - Label 抢占完整演示（本功能）

