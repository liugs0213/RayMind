# Agent 优先级支持示例

## 🎯 优先级支持概述

当前架构**完全支持优先级**，包括：

### ✅ 多层级优先级支持

1. **Agent 优先级**：Agent 本身有优先级
2. **任务优先级**：每个任务有优先级
3. **资源池优先级**：资源池可以设置优先级
4. **抢占机制**：高优先级任务可以抢占低优先级任务

## 📋 优先级使用示例

### 1. 创建不同优先级的 Agent

```python
import ray
from schedulemesh import RayScheduler

# 初始化调度器
scheduler = RayScheduler("priority_scheduler")

# 创建资源池
cpu_pool = scheduler.create_pool("cpu_pool", "cpu", {"cpu": 8.0})
gpu_pool = scheduler.create_pool("gpu_pool", "gpu", {"gpu": 2.0})

# 创建不同优先级的 Agent
high_priority_agent = scheduler.create_agent(
    name="high_priority_agent",
    pool_name="gpu_pool", 
    actor_class=MyActor,
    priority=10.0  # 高优先级
)

medium_priority_agent = scheduler.create_agent(
    name="medium_priority_agent",
    pool_name="gpu_pool",
    actor_class=MyActor, 
    priority=5.0   # 中等优先级
)

low_priority_agent = scheduler.create_agent(
    name="low_priority_agent",
    pool_name="cpu_pool",
    actor_class=MyActor,
    priority=1.0   # 低优先级
)
```

### 2. 使用优先级调度

```python
# 使用优先级策略选择 Agent
result = scheduler.choose(
    label="gpu", 
    strategy="priority"
).train_model(
    model_data,
    priority=8.0  # 任务优先级
)
```

### 3. 动态调整优先级

```python
# 动态调整 Agent 优先级
high_priority_agent.set_priority(15.0)  # 提高优先级
low_priority_agent.set_priority(0.5)    # 降低优先级

# 检查当前优先级
print(f"High priority agent: {high_priority_agent.get_priority()}")
print(f"Low priority agent: {low_priority_agent.get_priority()}")
```

### 4. 抢占机制

```python
# 检查是否可以抢占
if high_priority_agent.can_preempt(task_priority=12.0):
    print("可以抢占当前任务")
    
# 使用抢占控制器
preemption_result = scheduler.preemption_controller.evaluate_preemption(
    high_priority_task="urgent_task",
    target_label="gpu"
)
```

## 🔄 优先级调度流程

### 1. 任务提交
```python
# 提交高优先级任务
urgent_task = scheduler.choose(label="gpu", strategy="priority").process_urgent_data(
    data,
    priority=15.0  # 高优先级
)
```

### 2. 调度决策
```python
# 调度器会根据以下因素选择 Agent：
# 1. Agent 优先级
# 2. 任务优先级  
# 3. 资源可用性
# 4. 负载均衡
```

### 3. 抢占评估
```python
# 如果资源不足，会评估抢占：
# 1. 计算抢占分数：preempt_score = Δpriority - κ × remaining_time
# 2. 选择可抢占的低优先级任务
# 3. 执行抢占和状态保存
```

## 📊 优先级配置

### 1. 优先级范围
- **Agent 优先级**：0.0 - 100.0（默认 0.0）
- **任务优先级**：任意浮点数
- **抢占阈值**：可配置（默认 0.5）

### 2. 优先级策略
```python
# 支持多种优先级策略
strategies = [
    "priority",      # 纯优先级调度
    "least_busy",    # 最少负载 + 优先级
    "binpacking",    # 装箱 + 优先级
]
```

### 3. 跨标签抢占
```python
# 启用跨标签抢占
scheduler.preemption_controller.cross_label_preemption_enabled = True
```

## 🎯 最佳实践

### 1. 优先级设计
- **系统任务**：优先级 90-100
- **用户高优任务**：优先级 70-89
- **普通任务**：优先级 30-69
- **后台任务**：优先级 1-29

### 2. 资源分配
- **高优先级 Agent**：分配更多资源
- **低优先级 Agent**：可以共享资源
- **动态调整**：根据负载动态调整优先级

### 3. 监控和告警
```python
# 监控优先级分布
metrics = scheduler.collect_metrics()
for agent_name, agent_metrics in metrics.items():
    if agent_metrics.get("priority", 0) > 80:
        print(f"High priority agent: {agent_name}")
```

## 📈 性能优化

### 1. 优先级队列
- 使用堆数据结构维护优先级队列
- O(log n) 的插入和删除复杂度

### 2. 抢占优化
- 批量评估抢占候选
- 异步状态保存和恢复
- 智能重试机制

### 3. 负载均衡
- 结合优先级和负载进行调度
- 避免优先级饥饿
- 支持优先级老化机制

## 🔧 配置示例

```python
# 完整的优先级配置
scheduler = RayScheduler("priority_scheduler")

# 配置抢占参数
scheduler.preemption_controller.preemption_threshold = 0.3
scheduler.preemption_controller.cross_label_preemption_enabled = True

# 创建分层资源池
critical_pool = scheduler.create_pool("critical", "critical", {"cpu": 4.0, "gpu": 1.0})
normal_pool = scheduler.create_pool("normal", "normal", {"cpu": 8.0})

# 创建分层 Agent
critical_agents = [
    scheduler.create_agent(f"critical_{i}", "critical", CriticalActor, priority=90.0)
    for i in range(2)
]

normal_agents = [
    scheduler.create_agent(f"normal_{i}", "normal", NormalActor, priority=50.0)
    for i in range(4)
]
```

## 📋 总结

当前架构**完全支持优先级**，包括：

✅ **Agent 级别优先级**：每个 Agent 可以设置优先级  
✅ **任务级别优先级**：每个任务可以设置优先级  
✅ **调度策略支持**：支持优先级调度策略  
✅ **抢占机制**：高优先级任务可以抢占低优先级任务  
✅ **动态调整**：支持运行时动态调整优先级  
✅ **跨标签抢占**：支持跨资源池的抢占  
✅ **监控和告警**：完整的优先级监控体系  

这套优先级系统设计完善，可以满足各种复杂的调度需求！


