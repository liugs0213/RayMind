# PG 池 RLHF 调度使用指南

本文介绍如何使用 `SimpleScheduler` 配合 Placement Group (PG) 池，调度 RLHF 训练中的不同角色。示例基于仓库自带的 `examples/pg_pool_rlhf_full_demo.py`。

示例中包含以下角色：

- **Rollout**：生成样本，优先级较低，使用动态 PG；
- **Reward**：评估样本，优先级中等，使用预留 PG；
- **Train**：模型训练，优先级最高，使用预留 PG。

脚本演示了：

1. 如何为 Train/Reward 预留 PG，Rollout 动态建 PG；
2. 当 PG 路径因配额不足失败时，退回传统抢占 API 释放低优任务并重建高优任务；
3. Rollout 扩容触发自动抢占（`total_preemptions > 0`），观察 PG 的复用及销毁；
4. 集群资源不足时自动缩放任务与 PG 规格；
5. 如何调用 `worker.run()` 模拟业务工作、清理任务；
6. 使用 `cancel_timeout` 防止抢占在 `cancel()` 阶段阻塞。

---

## 1. 运行示例

```bash
python examples/pg_pool_rlhf_full_demo.py
```

输出内容较长，可在 Ray Dashboard (`http://127.0.0.1:8265`) 查看 Actor 变化。脚本会优先连接 `RAY_ADDRESS` 指定的集群，其次尝试 `address="auto"`，最后退回本地 Ray，并根据集群实际资源自动缩放 PG 规格。

---

## 2. 关键配置

### 2.1 配置抢占策略

```python
from schedulemesh.config.policy import PreemptionAggressiveness

scheduler.configure_preemption(
    enable_label_preemption=True,
    label_preemption_rules={
        "role": {
            "train": ["reward", "rollout"],
            "reward": ["rollout"],
        }
    },
    preemption_aggressiveness=PreemptionAggressiveness.MEDIUM,
    cancel_timeout=0.5,  # 取消超时时间（秒）
    enable_cross_pool_preemption=False,
)
```

- `label_preemption_rules` 决定谁能抢谁（示例中 Train > Reward > Rollout）。
- `preemption_aggressiveness` 设定抢占需要的优先级差，`MEDIUM` 对应 label 阈值 0.5 / 同池阈值 1.0。
- `cancel_timeout` 可防止 `cancel()` 阶段卡住抢占流程。
- `enable_cross_pool_preemption` 为单池示例设置为 `False`。

### 2.2 创建启用 PG 的资源池

```python
scheduler.ensure_pool(
    name="rlhf-pg-pool",
    resources={"cpu": 6.0, "memory": 8192.0},  # 资源池总配额
    pg_pool_config={
        "enable": True,
        "high_priority_pg_specs": [
            {"cpu": 2.0, "memory": 2048.0},  # Train 预留 PG
            {"cpu": 2.0, "memory": 2048.0},  # Reward 预留 PG
        ],
        "enable_dynamic_pgs": True,
        "max_dynamic_pgs": 2,              # Rollout 最多动态 PG
        "enable_pg_reuse": True,
    },
)
```

> 小贴士：Ray 在 `placement_group()` 完成就绪后会立即为 PG 预留底层资源；因此即便还没有创建任何 Actor，这部分 CPU/内存已经从集群可用资源里扣除。ScheduleMesh 自己的资源池账本仍然在 Agent 创建 (`reserve_agent_slot → commit_agent_slot`) 时扣减，如果池配额小于 PG 规格，提交阶段仍可能因为账本不足而失败并回退到传统抢占。若希望保证池账本也提前锁定，可调大池配额或预热 Agent。

### 2.3 提交任务

```python
result = scheduler.submit_with_pg_preemption(
    task_id="train-00",
    pool="rlhf-pg-pool",
    actor_class=RLHFWorker,
    resources={"cpu": 2.0, "memory": 2048.0},
    priority=9.0,
    labels={"role": "train"},
    estimated_duration=25.0,
    ray_options={"name": "train-worker-00"},
)
```

若 PG 路径失败，脚本示例会退回到 `scheduler.submit_spec(SimpleTaskSpec(...))`，该路径允许传统抢占释放低优任务后继续创建。

## 3. 观察输出

运行脚本后，你会看到按场景划分的日志。默认脚本会尝试：

- 根据 `RAY_ADDRESS` 环境变量连接现有集群，若未设置则 `ray.init(address="auto")`；若两次尝试都失败，会打印失败原因并回退到 `local_mode=True` 的本地 Ray；
- 若仍无法连接，则本地启动一个 8 CPU 的 Ray；
- 检查集群总资源，不足以支撑默认 7 CPU / 6GiB 配置时自动按比例缩小 PG 与任务规格。

关键场景日志如下：

### 场景 1：Rollout（动态 PG）

```
=== 场景 1: Rollout 角色启动（动态 PG） ===
  提交 rollout-00: success=True
  提交 rollout-01: success=True
[Rollout 启动后] 自带 PG 池统计 / 抢占统计
```

两个 Rollout 使用动态 PG，预留 PG 尚未被占用。

### 场景 2：Reward（预留 PG）

```
=== 场景 2: Reward 角色到达（高优运行） ===
  提交 reward-00: success=True
```

Reward 成功提交，未触发抢占。

### 场景 3：Train 回退抢占

```
  提交 train-00 (PG 优先): success=False
  ⚠️ PG 路径失败，改用传统抢占 API 触发自动抢占。
[INFO] 找到抢占目标 ... victim_task=reward-00 ...
  传统抢占提交 train-00: success=True
  ▶️ 触发 train-00 worker.run()（传统抢占成功）
```

由于资源池配额不足，PG 路径失败；传统抢占释放 `reward-00` 后创建 Train，抢占次数 (`total_preemptions`) 增为 1。

### 场景 4：Rollout 扩容触发自动抢占

```
[WARNING] 资源不足 task_id=rollout-extra-00...
[INFO] 找到抢占目标 ... victim_task=rollout-01 ...
[Worker:rollout-01] 任务 rollout-01 被抢占取消
  提交 rollout-extra-00 (传统抢占): success=True
  ▶️ 触发 rollout-extra-00 worker.run()
```

自动抢占释放了 `rollout-01`，重新创建扩容任务，`total_preemptions` 累计到 2。

### 清理阶段

脚本最后会调用 `scheduler.complete()` 和 `scheduler.shutdown()` 清理任务与 Ray 集群。

---

## 4. Metrics 暴露

调度器与抢占控制器会通过 Ray metrics exporter 输出 Prometheus 指标。默认本地运行时可直接访问 `http://127.0.0.1:8080/metrics`：

```bash
curl -s 127.0.0.1:8080/metrics | rg schedulemesh_
```

> 注意：务必在 Ray Head 节点启动时显式开启 Prometheus exporter（例如 `ray start --head --metrics-export-port=8080 --dashboard-host=0.0.0.0`），或在 `ray.init()` 中传入 `_metrics_export_port=8080` 且不要启用 `local_mode=True`。否则 `curl` 输出里不会出现 `schedulemesh_` 指标。

关键指标：

- `schedulemesh_preemption_count`：按照 `pool`/`reason` 标签统计的抢占次数；
- `schedulemesh_preemption_execution_latency_ms`：抢占执行耗时（含 PG 复用延迟）；
- `schedulemesh_task_schedule_latency_ms`：调度器将任务分配给 Agent 的延迟。

结合脚本最后打印的 `scheduler.stats()`、`scheduler.pg_pool_stats()` 可以快速定位 PG 路径是否正常。若在多节点部署，可通过 Prometheus/Grafana 拉取上述指标并设置报警。

---

## 5. 使用建议

1. **资源配额**：`resources` 是池的总可用资源，PG 预留不会自动扣配额；如果你需要保障高优任务一定可运行，可以调高池配额或预热 Agent。
2. **抢占敏感度**：`PreemptionAggressiveness.HIGH` 减小优先级差阈值，响应更快；`LOW` 更保守。
3. **PG 回收**：调用 `scheduler.delete_agent(..., destroy_pg=True)` 或 `scheduler.shutdown()` 时，会销毁关联 PG（即便是高优 PG），确保 Ray 集群资源被立刻释放；若想保留以便复用，将 `destroy_pg` 设为 `False`。
4. **取消策略**：业务侧应在 `Worker.cancel()` 中尽快返回，可设置取消标志或异步清理；示例通过 `cancel_timeout` 避免抢占卡住。
5. **监控**：结合 `scheduler.pg_pool_stats()`、`scheduler.stats()` 或 Ray Dashboard 观察 PG 与抢占行为。
6. **扩展场景**：可以复制脚本测试多 pool、跨 pool 抢占，或换成自定义 Worker 验证实际训练流程。

---

通过 `SimpleScheduler` + PG 池，配合标签和优先级策略，即可快速搭建“高优任务零等待、低优任务按需释放”的 RLHF 调度方案。
