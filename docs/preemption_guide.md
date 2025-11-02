# ScheduleMesh 抢占功能使用指南

## 概述

ScheduleMesh 支持基于优先级的任务抢占机制，包括：

- **同 Pool 内抢占**：优先级高的任务可以抢占同一资源池内优先级低的任务
- **跨 Pool 抢占**：在满足更高阈值的情况下，高优任务可以跨资源池抢占
- **保护策略**：可以设置受保护的资源池，防止被跨池抢占
- **状态保存**：被抢占任务的状态自动保存，支持后续恢复
- **标签语义**：基于 label（如训练阶段、服务等级）定义更细粒度的抢占顺序

## 抢占判定算法

抢占得分计算公式：

```
preempt_score = Δpriority + κ × remaining_time
```

其中：
- `Δpriority`：新任务与运行任务的优先级差值
- `κ`：剩余时间权重系数（默认 0.1）
- `remaining_time`：运行任务的剩余执行时间（秒）

只有当抢占得分超过阈值时才会触发抢占。

## 抢占策略配置

### 默认策略

```python
PreemptionPolicy(
    same_pool_priority_threshold=1.0,      # 同池抢占的最小优先级差值
    cross_pool_priority_threshold=5.0,     # 跨池抢占的最小优先级差值
    remaining_time_weight=0.1,             # 剩余时间权重 κ
    min_preempt_score=0.5,                 # 最小抢占得分
    enable_cross_pool_preemption=True,     # 是否允许跨池抢占
    protected_pools=[],                     # 受保护的资源池列表
    
    # Label 级别抢占配置（新增）
    label_preemption_rules={},             # Label 抢占规则
    label_priority_threshold=0.5,          # Label 抢占优先级阈值
    enable_label_preemption=True,          # 是否启用 Label 抢占
)
```

### 动态调整策略

```python
import ray
from schedulemesh.core import RayScheduler

ray.init()

# 以 detached 模式运行调度器并指定持久化目录，方便其它进程通过 RayScheduler.attach 复用
scheduler = RayScheduler("my-scheduler", detached=True, state_path="/var/lib/schedulemesh")

# 更新抢占策略
scheduler.update_preemption_policy(
    cross_pool_priority_threshold=10.0,  # 提高跨池抢占门槛
    protected_pools=["critical-pool"],    # 保护关键资源池
    enable_cross_pool_preemption=True,
)

### 抢占积极性配置

`PreemptionAggressiveness` 提供语义化阈值，支持枚举、字符串别名和环境变量三种方式配置。

#### 使用枚举

| 枚举常量 | Label 阈值 | 同池阈值 | 适用场景 |
|---------|------------|----------|----------|
| `PreemptionAggressiveness.HIGH` | 0.1 | 0.5 | 抢占响应要快、任务可随时中断 |
| `PreemptionAggressiveness.MEDIUM` | 0.5 | 1.0 | 默认值，平衡吞吐与稳定 |
| `PreemptionAggressiveness.LOW` | 1.0 | 3.0 | 任务更看重连续性 |

```python
from schedulemesh.config.policy import PreemptionAggressiveness

scheduler.update_preemption_policy(
    preemption_aggressiveness=PreemptionAggressiveness.HIGH,
)
```

> 结合持久化目录后，可使用 `scheduler.ensure_agent_health(timeout=120)` 定期巡检 Agent 心跳，自动剔除/重建异常实例，进一步提升调度面高可用能力。

#### 字符串别名

配置时也可以传入大小写不敏感的字符串，或以下语义别名：

| 字符串/别名 | 对应枚举 |
|-------------|----------|
| `"high"`, `"urgent"`, `"critical"`, `"prod"` | `HIGH` |
| `"medium"`, `"default"`, `"normal"` | `MEDIUM` |
| `"low"`, `"background"`, `"batch"` | `LOW` |

```python
scheduler.update_preemption_policy(preemption_aggressiveness="urgent")
```

系统会自动转换为枚举，并在日志中打印解析结果。

#### 环境变量驱动

当需要按部署环境动态调整时，可指定 `env:` 前缀：

```bash
export SCHEDULEMESH_PREEMPTION_LEVEL=high
```

```python
scheduler.update_preemption_policy(
    preemption_aggressiveness="env:SCHEDULEMESH_PREEMPTION_LEVEL",
)
```

如果环境变量不存在或值无效，日志中会给出提示。

> NOTE: 使用 `SimpleScheduler` 时，同样调用 `simple.configure_preemption(preemption_aggressiveness=...)` 即可透传上述配置。

### 一站式提交流程

推荐使用 `RayScheduler.submit_task_with_preemption()`（或 `SimpleScheduler.submit()` 包装）提交任务。该入口会串联资源检查、抢占决策、Agent 创建与错误回滚，避免手动维护多个步骤。

```python
from examples.training_preemption_demo import TrainingAgentActor

result = scheduler.submit_task_with_preemption(
    task_id="critical-train-job",
    pool_name="pretrain-pool",
    resources={"cpu": 2.0, "memory": 4096.0},
    priority=8.0,
    labels={"training_stage": "rlhf"},
    actor_class=TrainingAgentActor,
    actor_kwargs={
        "supervisor": scheduler.supervisor_handle(),
        "report_interval": 1.0,
    },
)

if not result["success"]:
    raise RuntimeError(result["error"])
```

## 使用示例

### 1. 基础抢占流程

```python
import ray
from schedulemesh.core import RayScheduler
from schedulemesh.core.agent_actor import AgentActor

# 初始化
ray.init()
scheduler = RayScheduler("demo-scheduler")

# 创建资源池
scheduler.create_pool(
    name="compute-pool",
    labels={"tier": "standard"},
    resources={"cpu": 4.0, "memory": 8192.0, "gpu": 0.0},
    target_agents=2,
)

# 创建 Agent
scheduler.create_agent(
    name="agent-1",
    pool="compute-pool",
    actor_class=AgentActor,
)

# 注册一个低优先级运行任务
scheduler.register_running_task(
    task_id="low-priority-job",
    agent_name="agent-1",
    pool_name="compute-pool",
    priority=1.0,
    labels={"pool": "compute-pool"},
    estimated_duration=60.0,  # 预计运行 60 秒
    payload={"data": "some work"},
)

# 当高优先级任务到达时，评估是否需要抢占
eval_result = scheduler.evaluate_preemption(
    incoming_task_priority=5.0,
    incoming_task_pool="compute-pool",
)

if eval_result["should_preempt"]:
    # 选择第一个候选（得分最高）
    candidate = eval_result["candidates"][0]
    print(f"抢占候选: {candidate['task_id']}, 得分: {candidate['preempt_score']}")
    
    # 执行抢占
    preempt_result = scheduler.execute_preemption(
        task_id=candidate["task_id"],
        agent_name=candidate["agent_name"],
    )
    
    if preempt_result["success"]:
        print(f"抢占成功，状态已保存: {preempt_result['saved_state']}")
        # 现在可以调度高优先级任务到该 Agent
```

### 2. 同 Pool 内抢占

```python
# 同一资源池内的任务抢占，门槛较低（默认优先级差 >= 1.0）

# 注册低优先级任务
scheduler.register_running_task(
    task_id="batch-job",
    agent_name="agent-1",
    pool_name="compute-pool",
    priority=1.0,  # 低优先级
)

# 高优先级任务到达
eval = scheduler.evaluate_preemption(
    incoming_task_priority=3.0,  # 差值 = 2.0 > 1.0，满足条件
    incoming_task_pool="compute-pool",
)

assert eval["should_preempt"] is True
assert eval["candidates"][0]["reason"] == "same_pool_preemption"
```

### 3. 跨 Pool 抢占

```python
# 创建第二个资源池
scheduler.create_pool(
    name="premium-pool",
    labels={"tier": "premium"},
    resources={"cpu": 2.0, "memory": 4096.0, "gpu": 1.0},
    target_agents=1,
)

# 在标准池有一个低优任务
scheduler.register_running_task(
    task_id="standard-job",
    agent_name="agent-1",
    pool_name="compute-pool",
    priority=2.0,
)

# 从高级池发起抢占请求
eval = scheduler.evaluate_preemption(
    incoming_task_priority=10.0,  # 差值 = 8.0 > 5.0（跨池阈值），满足条件
    incoming_task_pool="premium-pool",
)

if eval["should_preempt"]:
    candidate = eval["candidates"][0]
    assert candidate["reason"] == "cross_pool_preemption"
    # 执行抢占...
```

### 4. 保护关键资源池

```python
# 设置生产环境池为受保护池
scheduler.update_preemption_policy(
    protected_pools=["production-pool", "critical-services"],
)

# 在受保护池运行任务
scheduler.register_running_task(
    task_id="prod-service",
    agent_name="prod-agent",
    pool_name="production-pool",
    priority=5.0,
)

# 即使有更高优先级的跨池任务，也无法抢占受保护池
eval = scheduler.evaluate_preemption(
    incoming_task_priority=100.0,  # 超高优先级
    incoming_task_pool="other-pool",
)

assert eval["should_preempt"] is False  # 受保护，不能抢占
```

### 5. 基于 Label 的抢占（高级特性）

基于 Label 的抢占允许你定义更细粒度的抢占策略，不受 pool 边界限制。

#### 5.1 配置 Label 抢占规则

```python
# 配置基于 tier label 的分级抢占
scheduler.update_preemption_policy(
    label_preemption_rules={
        "tier": {
            "premium": ["standard", "batch"],  # premium 可以抢占 standard 和 batch
            "standard": ["batch"],             # standard 可以抢占 batch
        }
    },
    label_priority_threshold=0.5,  # Label 抢占只需较低的优先级差值
    enable_label_preemption=True,
)
```

#### 5.2 多维度 Label 规则

可以同时配置多个 label 的抢占规则：

```python
scheduler.update_preemption_policy(
    label_preemption_rules={
        # 基于服务等级的抢占
        "tier": {
            "premium": ["standard", "batch"],
            "standard": ["batch"],
        },
        # 基于用户类型的抢占
        "user_tier": {
            "admin": ["vip", "regular"],
            "vip": ["regular"],
        },
        # 基于优先级分类的抢占
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

#### 5.3 使用场景示例

**场景 1：分级服务抢占**

```python
# 注册运行任务
scheduler.register_running_task(
    task_id="batch-job",
    agent_name="worker-1",
    pool_name="compute-pool",
    priority=5.0,
    labels={"tier": "batch"},  # batch 级别任务
)

# premium 任务到达（即使优先级相同，也可以抢占）
eval_result = scheduler.evaluate_preemption(
    incoming_task_priority=5.0,  # 优先级相同
    incoming_task_pool="compute-pool",
    incoming_task_labels={"tier": "premium"},  # premium 级别
)

assert eval_result["should_preempt"] is True
assert eval_result["candidates"][0]["reason"] == "label_based_preemption"
```

**场景 2：跨 Pool 的 Label 抢占**

Label 抢占优先于 pool 抢占检查，即使不满足跨 pool 抢占的高阈值，也可以基于 label 规则抢占：

```python
# 配置策略
scheduler.update_preemption_policy(
    label_preemption_rules={
        "tier": {"premium": ["standard"]},
    },
    label_priority_threshold=0.5,      # Label 抢占阈值低
    cross_pool_priority_threshold=10.0, # 跨 pool 抢占阈值高
    enable_label_preemption=True,
)

# Pool A 中有 standard 任务
scheduler.register_running_task(
    task_id="standard-in-a",
    agent_name="agent-a",
    pool_name="pool-a",
    priority=5.0,
    labels={"tier": "standard"},
)

# Pool B 的 premium 任务（优先级差不足以跨 pool 抢占）
eval_result = scheduler.evaluate_preemption(
    incoming_task_priority=7.0,  # 差值 2.0 < 10.0（跨 pool 阈值）
    incoming_task_pool="pool-b",
    incoming_task_labels={"tier": "premium"},
)

# 但由于 label 规则，仍然可以抢占！
assert eval_result["should_preempt"] is True
assert eval_result["candidates"][0]["reason"] == "label_based_preemption"
```

**场景 3：多租户隔离抢占**

```python
# 配置租户抢占规则
scheduler.update_preemption_policy(
    label_preemption_rules={
        "user_tier": {
            "admin": ["vip", "regular"],  # 管理员可以抢占所有用户
            "vip": ["regular"],           # VIP 可以抢占普通用户
        }
    },
    label_priority_threshold=1.0,
    enable_label_preemption=True,
)

# 注册 regular 用户任务
scheduler.register_running_task(
    task_id="user-task",
    agent_name="shared-worker",
    pool_name="shared-pool",
    priority=10.0,  # 高优先级
    labels={"user_tier": "regular"},
)

# admin 用户任务（中等优先级）
eval_result = scheduler.evaluate_preemption(
    incoming_task_priority=6.0,  # 优先级更低
    incoming_task_pool="shared-pool",
    incoming_task_labels={"user_tier": "admin"},
)

# admin 可以抢占 regular，即使优先级更低
assert eval_result["should_preempt"] is True
```

#### 5.4 Label 抢占优先级

抢占评估的优先级顺序：

1. **Label 抢占**（最高优先级）
   - 如果 `enable_label_preemption=True` 且配置了规则
   - 只需满足 `label_priority_threshold`（通常较低）
   - 不受 pool 边界限制

2. **Pool 抢占**（回退机制）
   - 如果 label 规则不匹配或未启用
   - 同 pool 内抢占：需满足 `same_pool_priority_threshold`
   - 跨 pool 抢占：需满足 `cross_pool_priority_threshold`（通常较高）

#### 5.5 禁用 Label 抢占

如果需要临时禁用 label 抢占，系统会自动回退到 pool 级别抢占：

```python
scheduler.update_preemption_policy(
    enable_label_preemption=False,  # 禁用 label 抢占
)

# 此时只能使用 pool 级别的抢占规则
```

#### 5.6 完整示例

完整的示例代码请参考：
- `examples/label_preemption_demo.py` - 基于 label 的抢占演示
- `tests/integration/test_label_preemption.py` - Label 抢占测试用例

### 6. 训练阶段的抢占流程

在真实的大模型训练中，预训练、SFT、RLHF 等阶段对资源的紧迫度不同。可以结合 label 抢占与 `submit_task_with_preemption()` 打造语义化策略：

```python
from schedulemesh.core.controllers.ray_scheduler import RayScheduler
from examples.training_preemption_demo import TrainingAgentActor

scheduler = RayScheduler("training-preemption")

# 语义化配置：后训练阶段优先于预训练
scheduler.update_preemption_policy(
    enable_label_preemption=True,
    label_preemption_rules={
        "training_stage": {
            "rlhf": ["pretraining", "sft"],
            "dpo": ["pretraining"],
            "sft": ["pretraining"],
        }
    },
    preemption_aggressiveness="medium",  # 自动设置阈值
)

# 预训练任务先占用资源池
scheduler.submit_task_with_preemption(
    task_id="llama-70b-pretrain",
    pool_name="pretrain-pool",
    priority=5.0,
    resources={"cpu": 2.0, "memory": 4096.0},
    labels={"training_stage": "pretraining"},
    actor_class=TrainingAgentActor,
    actor_kwargs={"supervisor": scheduler.supervisor_handle()},
)

# RLHF 任务到达时，即使优先级数值相同，也会基于 label 触发抢占
rlhf = scheduler.submit_task_with_preemption(
    task_id="llama-7b-rlhf",
    pool_name="pretrain-pool",
    priority=5.0,
    resources={"cpu": 1.0, "memory": 2048.0},
    labels={"training_stage": "rlhf"},
    actor_class=TrainingAgentActor,
    actor_kwargs={"supervisor": scheduler.supervisor_handle()},
)

assert rlhf["success"], rlhf
```

运行 `python examples/training_preemption_demo.py` 可观察完整流程：预训练任务保存检查点后释放资源，而 RLHF 任务接管资源继续训练。

> 提示：示例中的 `TrainingAgentActor` 继承自 `schedulemesh.core.agents.MetricsReportingAgent`，会在训练循环中周期性调用 `report_metrics()`，无需阻塞训练线程即可将进度同步给调度器。

### 7. 自定义状态处理器

```python
from schedulemesh.core.actors.control.preemption_controller import TaskState

# 定义自定义状态保存器
def custom_state_preserver(task_state: TaskState) -> dict:
    """自定义状态序列化逻辑"""
    return {
        "task_id": task_state.task_id,
        "checkpoint_path": f"/checkpoints/{task_state.task_id}.ckpt",
        "custom_data": "my_serialized_data",
    }

# 定义自定义状态恢复器
def custom_state_restorer(saved_state: dict) -> None:
    """自定义状态恢复逻辑"""
    checkpoint_path = saved_state["checkpoint_path"]
    print(f"从 {checkpoint_path} 恢复任务...")
    # 实际恢复逻辑...

# 注册自定义处理器（通常在系统初始化时配置）
scheduler.register_state_handlers(
    pool_name="custom-pool",
    preserver=custom_state_preserver,
    restorer=custom_state_restorer,
)
```

### 8. 监控抢占统计

```python
# 获取抢占统计信息
stats = scheduler.get_preemption_stats()

print(f"总抢占次数: {stats['total_preemptions']}")
print(f"同池抢占: {stats['same_pool_preemptions']}")
print(f"跨池抢占: {stats['cross_pool_preemptions']}")
print(f"当前运行任务数: {stats['running_tasks']}")

# 查看最近的抢占历史
for record in stats['recent_preemptions']:
    print(f"时间: {record['timestamp']}, 任务: {record['task_id']}, "
          f"池: {record['pool_name']}, 优先级: {record['priority']}")
```

## 抢占策略最佳实践

### 1. 优先级设计

建议使用分层优先级体系：

```python
PRIORITY_CRITICAL = 10.0   # 关键任务（SLA 保证）
PRIORITY_HIGH = 5.0        # 高优先级
PRIORITY_NORMAL = 1.0      # 普通任务
PRIORITY_LOW = 0.5         # 低优先级（批处理）
PRIORITY_BACKGROUND = 0.1  # 后台任务
```

### 2. 跨池抢占门槛

对于生产环境，建议提高跨池抢占门槛：

```python
scheduler.update_preemption_policy(
    same_pool_priority_threshold=1.0,   # 同池：优先级差 >= 1.0
    cross_pool_priority_threshold=8.0,  # 跨池：优先级差 >= 8.0（更严格）
)
```

### 3. 保护关键服务

将生产环境、在线服务等关键资源池设为受保护：

```python
scheduler.update_preemption_policy(
    protected_pools=[
        "production-api",
        "real-time-inference",
        "critical-services",
    ],
)
```

### 4. 预估执行时间

为任务提供准确的执行时间预估，有助于抢占算法做出更好的决策：

```python
scheduler.register_running_task(
    task_id="training-job",
    agent_name="gpu-agent",
    pool_name="ml-pool",
    priority=2.0,
    estimated_duration=3600.0,  # 预计 1 小时
)
```

## 性能考虑

1. **抢占开销**：抢占操作涉及状态保存、任务取消和重调度，有一定开销
2. **频繁抢占**：避免设置过低的阈值导致频繁抢占，影响系统稳定性
3. **状态大小**：自定义状态处理器应尽量精简保存的数据
4. **评估性能**：抢占评估会遍历所有运行任务，任务数量大时需注意性能

## 故障处理

### 抢占失败

如果抢占执行失败，任务状态仍会被保存，可以手动恢复：

```python
preempt_result = scheduler.execute_preemption(task_id, agent_name)

if not preempt_result["cancel_success"]:
    print("任务取消失败，但状态已保存")
    saved_state = preempt_result["saved_state"]
    # 根据业务逻辑决定是否重试或手动干预
```

### 状态恢复失败

状态恢复失败时会返回错误信息：

```python
result = scheduler.restore_task(saved_state)

if not result["success"]:
    print(f"恢复失败: {result['error']}")
    # 回退到默认恢复策略或人工介入
```

## 总结

抢占功能为 ScheduleMesh 提供了强大的优先级保证能力，适用于：

- 混合工作负载场景（在线服务 + 批处理）
- SLA 要求严格的业务
- 资源紧张时的优先级调度
- 多租户环境下的资源隔离

合理配置抢占策略可以显著提升资源利用率和任务响应时间。

### 6. 本地文件持久化示例

抢占状态默认保存在 PreemptionControllerActor 内存中。若需要在调度面持久化，可通过自定义 `state_handlers` 写入本地文件：

```python
from pathlib import Path
import json

from schedulemesh.core.actors.control.preemption_controller import TaskState

CHECKPOINT_DIR = Path("/var/lib/schedulemesh/checkpoints")
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

def file_state_preserver(task_state: TaskState) -> dict:
    data = {
        "task_id": task_state.task_id,
        "pool": task_state.pool_name,
        "payload": task_state.payload,
    }
    path = CHECKPOINT_DIR / f"{task_state.task_id}.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return {"task_id": task_state.task_id, "checkpoint_path": str(path)}

def file_state_restorer(saved_state: dict) -> None:
    path = Path(saved_state["checkpoint_path"])
    payload = json.loads(path.read_text(encoding="utf-8"))
    # 将 payload 回放到业务系统

scheduler.register_state_handlers(
    pool_name="file-pool",
    preserver=file_state_preserver,
    restorer=file_state_restorer,
)
```

仓库 `examples/preemption_file_state.py` 给出了完整示例，并可根据部署实际情况替换为 SQLite、Redis 等存储。

### 7. Agent 取消任务支持

`AgentActor` 已提供 `cancel(task_id)` 方法，抢占控制器会在执行抢占时自动调用。如果你实现自定义 Agent，需要确保：

```python
from schedulemesh.core.agent_actor import AgentActor

class MyAgent(AgentActor):
    @ray.method(num_returns=1)
    def cancel(self, task_id: str) -> dict:
        # 返回结构需与基类一致
        return super().cancel(task_id)
```

* 建议在处理任务时传入 `task_id`，并在任务启动前调用 `scheduler.register_running_task(...)`，这样抢占时才能准确定位并取消任务。
* 对于模拟长耗时任务，可使用 `simulated_duration` 参数演示取消效果：

```python
ray.get(agent.process.remote(payload={}, task_id="job-1", simulated_duration=30.0))
```

### 8. 常见排查

- **统计为 0**：开启抢占后仍看到 `same_pool_preemptions=0`，可能是 Agent 未正确注册或取消时失败，可查看日志中的警告。
- **任务未被取消**：检查 Agent 是否实现了 `cancel()`，或者 `register_running_task` 是否传入了正确的 `task_id`。
