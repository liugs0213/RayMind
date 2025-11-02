# 抢占功能示例

## 快速运行

```bash
cd /Users/admin/PycharmProjects/RayMind
python examples/preemption_demo.py
```

## 功能概览

本示例演示了 ScheduleMesh 的完整抢占功能：

### 1. 同 Pool 内抢占
- 低优先级任务 (priority=1.0) 正在运行
- 高优先级任务 (priority=5.0) 到达
- 系统自动评估并执行抢占
- 被抢占任务状态自动保存

### 2. 跨 Pool 抢占
- `standard-pool` 中的低优任务
- `premium-pool` 中的高优任务尝试抢占
- 演示优先级阈值判断（跨池需要更高的优先级差）

### 3. 资源池保护
- 设置 `production-pool` 为受保护池
- 即使高优任务也无法跨池抢占受保护池中的任务

## 核心 API

### 注册运行任务
```python
scheduler.register_running_task(
    task_id="my-task",
    agent_name="agent-1",
    pool_name="compute-pool",
    priority=5.0,  # 任务优先级
    estimated_duration=60.0,  # 预估执行时间（秒）
)
```

### 评估抢占
```python
result = scheduler.evaluate_preemption(
    incoming_task_priority=10.0,
    incoming_task_pool="target-pool",
)

if result["should_preempt"]:
    candidates = result["candidates"]  # 抢占候选列表
```

### 执行抢占
```python
preempt_result = scheduler.execute_preemption(
    task_id=candidate["task_id"],
    agent_name=candidate["agent_name"],
)

if preempt_result["success"]:
    saved_state = preempt_result["saved_state"]  # 保存的任务状态
```

### 更新抢占策略
```python
scheduler.update_preemption_policy(
    same_pool_priority_threshold=1.0,   # 同池阈值
    cross_pool_priority_threshold=5.0,  # 跨池阈值
    protected_pools=["prod-pool"],      # 受保护的池
)
```

### 查看统计
```python
stats = scheduler.get_preemption_stats()
print(f"总抢占: {stats['total_preemptions']}")
print(f"同池: {stats['same_pool_preemptions']}")
print(f"跨池: {stats['cross_pool_preemptions']}")
```

## 抢占算法

抢占得分计算公式：

```
preempt_score = Δpriority − κ × remaining_time
```

- **Δpriority**: 新任务与运行任务的优先级差值
- **κ**: 剩余时间权重系数（默认 0.1）
- **remaining_time**: 运行任务的剩余执行时间

只有当抢占得分超过阈值时才触发抢占。

## 使用场景

1. **混合工作负载**: 在线服务 + 批处理任务共享资源
2. **SLA 保证**: 关键任务的优先级保障
3. **多租户环境**: 不同租户间的资源隔离与优先级
4. **弹性扩缩**: 资源紧张时的智能调度

## 更多信息

详细文档请查看：
- [抢占功能使用指南](../docs/preemption_guide.md)
- [ScheduleMesh 设计文档](../docs/scheduleMesh_design.md)

