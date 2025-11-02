# Agent 配置指南

本指南介绍如何配置 ScheduleMesh 中的 Agent，包括资源配置、Ray actor 选项和自定义参数。

## 目录

1. [基础资源配置](#基础资源配置)
2. [Ray Actor 高级选项](#ray-actor-高级选项)
3. [自定义初始化参数](#自定义初始化参数)
4. [组合配置](#组合配置)
5. [配置参数完整列表](#配置参数完整列表)

## 基础资源配置

### 指定 CPU、Memory、GPU

```python
scheduler.create_agent(
    name="gpu-worker",
    pool="ml-pool",
    actor_class=AgentActor,
    resources={
        "cpu": 4.0,       # 4个CPU核心
        "memory": 8192.0, # 8GB内存（单位：MB）
        "gpu": 1.0,       # 1个GPU
    },
)
```

### 自定义资源类型

Ray 支持自定义资源类型，你可以定义任何资源（如 FPGA、特殊硬件等）：

```python
# 1. 启动Ray时声明自定义资源
ray.init(resources={
    "fpga": 4,
    "special_hardware": 2,
})

# 2. 创建使用自定义资源的Pool
scheduler.create_pool(
    name="fpga-pool",
    labels={"hardware": "fpga"},
    resources={
        "cpu": 2.0,
        "memory": 4096.0,
        "gpu": 0.0,
        "fpga": 1.0,  # 自定义资源
    },
    target_agents=2,
)

# 3. 创建使用自定义资源的Agent
scheduler.create_agent(
    name="fpga-agent",
    pool="fpga-pool",
    actor_class=AgentActor,
    resources={
        "cpu": 2.0,
        "memory": 4096.0,
        "gpu": 0.0,
        "fpga": 1.0,
    },
)
```

## Ray Actor 高级选项

通过 `ray_options` 参数可以配置 Ray actor 的高级特性。

### 常用选项

```python
scheduler.create_agent(
    name="reliable-agent",
    pool="production-pool",
    actor_class=AgentActor,
    ray_options={
        # ========== 容错和恢复 ==========
        "max_restarts": 5,           # Actor失败后最多重启5次
        "max_task_retries": 3,       # 单个任务最多重试3次
        
        # ========== 并发控制 ==========
        "max_concurrency": 20,       # 最多同时处理20个请求
        
        # ========== 生命周期管理 ==========
        "lifetime": "detached",      # 不随创建者退出（持久化actor）
        "name": "persistent-agent",  # 命名actor（用于重新连接）
        
        # ========== 资源隔离 ==========
        "runtime_env": {             # 运行时环境
            "env_vars": {"MODEL_PATH": "/models"},
            "pip": ["torch==2.0.0"],
        },
        
        # ========== 调度策略 ==========
        "scheduling_strategy": "SPREAD",  # 分散调度策略
    },
)
```

### Ray Options 完整列表

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `max_restarts` | int | -1 | Actor崩溃后的最大重启次数（-1表示无限） |
| `max_task_retries` | int | 3 | 单个任务失败后的最大重试次数 |
| `max_concurrency` | int | 1000 | Actor可同时处理的最大请求数 |
| `lifetime` | str | None | `"detached"` 表示持久化actor |
| `name` | str | None | Actor名称（用于 `ray.get_actor(name)`） |
| `namespace` | str | None | Actor命名空间 |
| `num_cpus` | float | - | CPU核心数（会被resources覆盖） |
| `num_gpus` | float | - | GPU数量（会被resources覆盖） |
| `memory` | int | - | 内存字节数（会被resources覆盖） |
| `resources` | dict | {} | 自定义资源字典 |
| `runtime_env` | dict | {} | 运行时环境配置 |
| `scheduling_strategy` | str | "DEFAULT" | 调度策略：DEFAULT/SPREAD/NODE_AFFINITY |
| `get_if_exists` | bool | False | 如果已存在则获取而不是创建 |

## 自定义初始化参数

通过 `actor_args` 和 `actor_kwargs` 可以传递自定义参数给 Agent 的 `__init__` 方法。

### 定义自定义 Agent

```python
import ray
from schedulemesh.core.agent_actor import AgentActor

@ray.remote
class ModelServerAgent(AgentActor):
    """自定义模型服务Agent"""
    
    def __init__(
        self,
        name: str,
        labels: dict,
        supervisor=None,
        *,
        model_path: str,
        batch_size: int = 32,
        timeout: float = 30.0,
        enable_cache: bool = True,
    ):
        super().__init__(name, labels, supervisor)
        
        self.model_path = model_path
        self.batch_size = batch_size
        self.timeout = timeout
        self.enable_cache = enable_cache
        
        # 加载模型等初始化逻辑
        print(f"加载模型: {model_path}, batch_size={batch_size}")
```

### 创建自定义 Agent

```python
scheduler.create_agent(
    name="model-server-1",
    pool="inference-pool",
    actor_class=ModelServerAgent,
    # 传递自定义参数
    actor_kwargs={
        "model_path": "/models/gpt-4",
        "batch_size": 64,
        "timeout": 60.0,
        "enable_cache": True,
    },
)
```

### 位置参数和关键字参数

```python
@ray.remote
class CustomAgent(AgentActor):
    def __init__(
        self,
        name: str,      # 必需（由ScheduleMesh自动传递）
        labels: dict,   # 必需（由ScheduleMesh自动传递）
        supervisor=None,# 可选（由ScheduleMesh自动传递）
        arg1,           # 额外位置参数
        arg2,           # 额外位置参数
        kwarg1=None,    # 额外关键字参数
        kwarg2=None,    # 额外关键字参数
    ):
        super().__init__(name, labels, supervisor)
        # ... 自定义初始化

scheduler.create_agent(
    name="custom-agent",
    pool="custom-pool",
    actor_class=CustomAgent,
    actor_args=["value1", "value2"],  # 对应 arg1, arg2
    actor_kwargs={                    # 对应 kwarg1, kwarg2
        "kwarg1": "foo",
        "kwarg2": "bar",
    },
)
```

## 组合配置

所有配置选项可以组合使用：

```python
scheduler.create_agent(
    name="premium-agent",
    pool="premium-pool",
    actor_class=CustomModelAgent,
    
    # 1. 资源配置
    resources={
        "cpu": 8.0,
        "memory": 16384.0,
        "gpu": 2.0,
        "tpu": 1.0,  # 自定义资源
    },
    
    # 2. Ray actor选项
    ray_options={
        "max_restarts": 5,
        "max_concurrency": 50,
        "lifetime": "detached",
        "name": "premium-agent-persistent",
        "runtime_env": {
            "env_vars": {
                "CUDA_VISIBLE_DEVICES": "0,1",
                "OMP_NUM_THREADS": "8",
            },
            "pip": [
                "torch==2.0.0",
                "transformers==4.30.0",
            ],
        },
    },
    
    # 3. 自定义初始化参数
    actor_kwargs={
        "model_path": "/models/llama-2-70b",
        "batch_size": 128,
        "max_seq_length": 4096,
        "enable_flash_attention": True,
        "quantization": "int8",
    },
)
```

## 配置参数完整列表

### `create_agent()` 方法签名

```python
def create_agent(
    name: str,                              # Agent名称（必需）
    pool: str,                              # 所属资源池（必需）
    actor_class: type,                      # Actor类（必需）
    *,
    resources: dict[str, float] | None = None,      # 资源配置
    ray_options: dict[str, Any] | None = None,      # Ray actor选项
    actor_args: list[Any] | None = None,            # 位置参数
    actor_kwargs: dict[str, Any] | None = None,     # 关键字参数
) -> dict:
    """创建Agent"""
```

### 参数说明

#### `name`
- **类型**: `str`
- **必需**: ✅
- **说明**: Agent的唯一标识符

#### `pool`
- **类型**: `str`
- **必需**: ✅
- **说明**: Agent所属的资源池名称

#### `actor_class`
- **类型**: `type`
- **必需**: ✅
- **说明**: Ray actor类（必须是 `@ray.remote` 装饰的类）

#### `resources`
- **类型**: `dict[str, float] | None`
- **必需**: ❌
- **默认**: 使用Pool的默认资源
- **说明**: Agent的资源需求
- **示例**:
  ```python
  {
      "cpu": 4.0,
      "memory": 8192.0,  # MB
      "gpu": 1.0,
      "custom_resource": 1.0,
  }
  ```

#### `ray_options`
- **类型**: `dict[str, Any] | None`
- **必需**: ❌
- **默认**: `{}`
- **说明**: Ray actor的配置选项
- **常用选项**: 见 [Ray Options 完整列表](#ray-options-完整列表)

#### `actor_args`
- **类型**: `list[Any] | None`
- **必需**: ❌
- **默认**: `[]`
- **说明**: 传递给Actor `__init__` 的额外位置参数
- **注意**: `name`, `labels`, `supervisor` 会自动传递，无需在此指定

#### `actor_kwargs`
- **类型**: `dict[str, Any] | None`
- **必需**: ❌
- **默认**: `{}`
- **说明**: 传递给Actor `__init__` 的额外关键字参数

## 常见场景示例

### 场景1: GPU训练任务

```python
scheduler.create_agent(
    name="training-worker",
    pool="gpu-pool",
    actor_class=TrainingAgent,
    resources={
        "cpu": 4.0,
        "memory": 16384.0,
        "gpu": 1.0,
    },
    ray_options={
        "max_restarts": 3,  # 训练可能会OOM，允许重启
    },
    actor_kwargs={
        "model_name": "resnet50",
        "batch_size": 128,
        "learning_rate": 0.001,
    },
)
```

### 场景2: 高可靠性在线服务

```python
scheduler.create_agent(
    name="api-server",
    pool="production-pool",
    actor_class=APIServerAgent,
    resources={
        "cpu": 2.0,
        "memory": 4096.0,
    },
    ray_options={
        "max_restarts": -1,        # 无限重启
        "max_concurrency": 100,    # 高并发
        "lifetime": "detached",    # 持久化
        "name": "api-server-main", # 可重连
    },
    actor_kwargs={
        "port": 8000,
        "workers": 4,
        "timeout": 30.0,
    },
)
```

### 场景3: 批处理任务

```python
scheduler.create_agent(
    name="batch-processor",
    pool="batch-pool",
    actor_class=BatchProcessorAgent,
    resources={
        "cpu": 8.0,
        "memory": 32768.0,
    },
    ray_options={
        "max_concurrency": 1,  # 串行处理
    },
    actor_kwargs={
        "batch_size": 1000,
        "checkpoint_interval": 100,
    },
)
```

## 最佳实践

### 1. 资源配置

- ✅ **明确指定资源**: 避免资源冲突和OOM
- ✅ **预留资源**: 为系统和监控留出10-20%的资源
- ✅ **使用自定义资源**: 标记特殊硬件，确保任务调度到正确的节点

### 2. 容错配置

- ✅ **设置合理的 max_restarts**: 
  - 生产服务: `-1` (无限重启)
  - 训练任务: `3-5` (有限重启)
  - 批处理: `0` (不重启，由上层重试)
  
- ✅ **使用 detached lifetime**: 对于长期运行的服务
- ✅ **命名关键 actors**: 便于监控和重新连接

### 3. 性能优化

- ✅ **调整 max_concurrency**: 根据任务类型设置合适的并发度
- ✅ **使用 runtime_env**: 隔离依赖，避免版本冲突
- ✅ **合理分配资源**: 避免过度订阅或资源浪费

### 4. 参数传递

- ✅ **使用 actor_kwargs**: 更清晰和可维护
- ✅ **提供默认值**: 让Agent更容易使用
- ✅ **验证参数**: 在Agent的 `__init__` 中验证参数有效性

## 故障排查

### 问题1: Actor创建失败

**错误**: `ValueError: Insufficient pool capacity`

**解决**:
```python
# 检查Pool剩余资源
pool_info = scheduler.get_pool("my-pool")
print(pool_info["pool"]["available_resources"])

# 调整资源需求或增加Pool容量
```

### 问题2: Actor频繁重启

**错误**: Actor一直重启，任务无法完成

**解决**:
```python
# 1. 检查日志找到崩溃原因
# 2. 增加资源配置
resources={"memory": 8192.0}  # 增加内存

# 3. 限制重启次数，避免无限循环
ray_options={"max_restarts": 3}
```

### 问题3: 参数传递错误

**错误**: `TypeError: __init__() got an unexpected keyword argument`

**解决**:
```python
# 确保Agent的__init__接受传递的参数
@ray.remote
class MyAgent(AgentActor):
    def __init__(self, name, labels, supervisor=None, *, my_param=None):
        super().__init__(name, labels, supervisor)
        self.my_param = my_param  # 添加参数接收
```

## 相关资源

- [Ray Actor 文档](https://docs.ray.io/en/latest/ray-core/actors.html)
- [Ray 资源管理](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html)
- [ScheduleMesh 资源池指南](../README.md#资源池管理)
- [示例代码](../examples/agent_advanced_options_demo.py)

