# ScheduleMesh

[![PyPI version](https://badge.fury.io/py/schedulemesh-core.svg)](https://badge.fury.io/py/schedulemesh-core)
[![Python Support](https://img.shields.io/pypi/pyversions/schedulemesh-core.svg)](https://pypi.org/project/schedulemesh-core/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://git.kanzhun-inc.com/arsenal/ray-mind/badges/main/pipeline.svg)](https://git.kanzhun-inc.com/arsenal/ray-mind/-/pipelines)

**ScheduleMesh** 是一个基于 Ray 的统一调度与资源管控平面，提供强大的分布式计算调度能力。

## ✨ 特性

- 🎯 **统一调度平面**：构建全局调度中心，替代 Ray 分散式调度
- 🏷️ **标签化资源管理**：基于 label 的资源域隔离和精准匹配
- 🔄 **动态扩缩容**：支持 PlacementGroup 的弹性管理
- 🚀 **PG 池化管理**：PlacementGroup 池化管理，支持高优 PG 预留与快速抢占
- ⚡ **优先级抢占**：高优任务的抢占机制和故障恢复
- 🎨 **Label 级别抢占**：支持基于任意 label 的细粒度抢占策略
- 📊 **实时监控**：异步 metrics 汇聚与全局优化
- 🔌 **可插拔架构**：支持自定义调度策略和分片策略
- 🌐 **多维度分片**：支持 pipeline parallel、tensor parallel、sequence parallel

## 🚀 快速开始

### 安装

```bash
# 基础安装
pip install schedulemesh-core

# 完整安装（包含所有功能）
pip install schedulemesh[full]

# 开发安装
pip install -e .[dev]
```

### 基础使用

#### 极简入口：`SimpleScheduler`

```python
import ray
from schedulemesh.simple import SimpleScheduler

ray.init()

simple = SimpleScheduler()
simple.ensure_pool(
    name="demo-pool",
    labels={"stage": "demo"},
    resources={"cpu": 2.0, "memory": 2048.0},
)
simple.configure_preemption(
    enable_label_preemption=True,
    label_preemption_rules={"stage": {"demo": ["batch"]}},
)
simple.submit(
    task_id="job-demo-1",
    pool="demo-pool",
    actor_class=MyDemoActor,
    resources={"cpu": 1.0},
    labels={"stage": "demo"},
    priority=10.0,
)
```

#### 细粒度控制：`RayScheduler`

```python
import ray
from schedulemesh.core.controllers.ray_scheduler import RayScheduler
from schedulemesh.core.actors.head import ScheduleMeshHead
from schedulemesh.core.agent_actor import AgentActor

# 启动 Ray（示例：声明自定义 accelerator 资源）
ray.init()

# 如果在 standalone head 节点上运行，可使用 ScheduleMeshHead 管理 supervisor
head = ScheduleMeshHead()
head.start()

# 创建调度门面
scheduler = RayScheduler(name="demo-mesh")

# 预留一个资源池：总共 4 vCPU、4096 MB 内存、2 个自定义 accelerator 资源
pool = scheduler.create_pool(
    name="demo-pool",
    labels={"stage": "demo"},
    resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0, "custom": {"accelerator": 1.0}},
    target_agents=2,
)

# 创建默认规格的 Agent（使用池默认配置）
agent_a = scheduler.create_agent(
    name="agent-a",
    pool="demo-pool",
    actor_class=AgentActor,
)

# 创建定制规格的 Agent：0.5 CPU / 512 MB，并传入 Ray actor 参数
agent_b = scheduler.create_agent(
    name="agent-b",
    pool="demo-pool",
    actor_class=AgentActor,
    resources={"cpu": 0.5, "memory": 512.0, "gpu": 0.0, "custom": {"accelerator": 0.25}},
    ray_options={"max_restarts": 1, "runtime_env": {"env_vars": {"MODE": "test"}}},
)

# 列出资源池中的 agent
agents = scheduler.list_agents("demo-pool")  # => {"agents": [...]} 包含资源、Ray options 等信息

# 使用 Agent 句柄执行逻辑
handle = agent_a["agent"]["handle"]
print(ray.get(handle.invoke.remote("process", payload="hello")))

# 删除 Agent 并自动归还资源
scheduler.delete_agent("agent-b")

# 清理
scheduler.shutdown()
head.stop()
ray.shutdown()

# 如果希望长时间运行，可使用 detached + state_path 模式并在新进程中 attach
persistent = RayScheduler(
    name="prod-scheduler",
    detached=True,
    namespace="prod",
    state_path="/var/lib/schedulemesh/state",
)
...
persistent.shutdown()  # 仅在真正需要销毁时调用

# 其他进程可使用 attach 获取句柄
client = RayScheduler.attach(name="prod-scheduler", namespace="prod", state_path="/var/lib/schedulemesh/state")
client.list_agents()
```

#### 常用 API 速查

- `create_pool(name, labels, resources, target_agents=0, placement_strategy="STRICT_PACK")`  
  以总量的方式声明资源池容量；`resources` 支持 `cpu` / `memory (MB)` / `gpu` / `custom`。
- `create_agent(name, pool, actor_class, resources=None, ray_options=None)`  
  在池内创建 Ray actor；`resources` 可覆盖池默认规格，`ray_options` 会与资源推导出的 `num_cpus` 等参数合并。
- `list_agents(pool_name=None)`  
  返回当前已注册的 Agent（含资源与 Ray options）；传入 `pool_name` 可按池过滤。
- `delete_agent(name, force=False)`  
  终止 Ray actor 并归还资源配额；`force=True` 时忽略 Ray Kill 的异常。
- `delete_agent` / `create_agent` 均会在失败时自动回滚资源预留，确保资源账本与 Ray 状态一致。


## 🏗️ 架构设计

ScheduleMesh 采用分层架构设计：

```
ScheduleMesh Manager (全局调度中心)
        ↓
   Supervisor Actor (协调器)
        ↓
   ┌─────────────────┬─────────────────┐
   │  Resource Pool   │  PG Pool        │
   │  Manager         │  Manager        │
   │  (虚拟资源配额)   │  (物理资源分配)   │
   └─────────────────┴─────────────────┘
        ↓                    ↓
   Resource Pools (按 Label 分组)
        ↓                    ↓
   PG Pools (高优 PG + 动态 PG)
        ↓
   Agent Actor (任务执行 + Metrics 收集)
```

详细架构图请参考 [设计文档](docs/scheduleMesh_design.md#60-架构总览图)。

### 核心组件

- **ScheduleMesh Manager**：全局调度中心，负责任务分发与资源协调。对外暴露 `SimpleScheduler` / `RayScheduler` 等门面。
- **Supervisor Actor**：协调器，统一管理所有控制组件（Resource Pool Manager、PG Pool Manager、Agent Manager、Scheduler、Preemption Controller 等）。
- **Resource Pool Manager**：资源池管理器，负责按 pool 维度记录容量、已用配额与目标 Agent 数量，管理虚拟资源配额，替代早期的「Broker」概念。
- **PlacementGroup Pool Manager**：PG 池化管理器，管理物理资源分配（PG 创建、分配、复用），支持高优 PG 预留与动态 PG 管理，实现高优作业快速启动。
- **PreemptionController**：优先级抢占控制器，结合 label / 数值阈值做细粒度抢占决策，与 PG Pool 配合实现快速抢占。
- **Agent Manager & Agent Actor**：Agent 管理器维护 Ray actor 生命周期，协调 Resource Pool 和 PG Pool 确保虚拟配额与物理资源一致；Agent Actor 负责执行任务并通过 `MetricsReportingAgent` 异步上报指标。
- **Resource Registry**：统一资源注册表，封装集群资源快照与池内资源账本。

### 资源管理双层架构

ScheduleMesh 采用**虚拟配额 + 物理资源**的双层管理架构：

- **Resource Pool Manager**：管理虚拟资源配额（容量、已用、预留），确保资源账本一致性，防止超分配。
- **PlacementGroup Pool Manager**：管理物理资源分配（PG 创建、分配、复用），保证资源隔离与快速启动。
- **协作机制**：每个 Resource Pool 对应一个 PG Pool，Agent 创建时先预留虚拟配额，再从 PG Pool 分配物理 PG，确保虚拟配额与物理资源的一致性。

## 🔌 插件系统

ScheduleMesh 支持可插拔的插件架构：

### 调度策略插件

```python
from schedulemesh.plugins import SchedulingStrategyPlugin

class CustomSchedulingPlugin(SchedulingStrategyPlugin):
    def score(self, task, resources):
        # 自定义资源打分逻辑
        return custom_score
    
    def priority(self, task):
        # 自定义优先级计算
        return custom_priority
    
    def preemption_policy(self, high_priority_task, low_priority_task):
        # 自定义抢占策略
        return should_preempt
```

### 分片策略插件

```python
from schedulemesh.plugins import DispatchStrategyPlugin

class CustomDispatchPlugin(DispatchStrategyPlugin):
    def python_dispatch_fn(self, data, workers_a, workers_b):
        # 自定义 Python 数据分片
        return custom_shards
    
    def torch_dispatch_fn(self, tensor, workers_a, workers_b):
        # 自定义 PyTorch 张量分片
        return custom_tensor_shards
```

## 📊 监控和 Metrics

ScheduleMesh 提供完整的监控能力：

- **实时 Metrics**：CPU、内存、GPU 使用率
- **调度指标**：调度延迟、成功率、排队时间
- **性能指标**：吞吐量、延迟、资源利用率
- **告警系统**：基于阈值的自动告警

```bash
# 克隆仓库
git clone https://git.kanzhun-inc.com/arsenal/ray-mind.git
cd ray-mind

# 安装开发依赖
pip install -e .[dev]

# 运行测试
pytest

# 代码格式化
black .
isort .

# 类型检查
mypy schedulemesh/
```
