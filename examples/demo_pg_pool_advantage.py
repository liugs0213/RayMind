#!/usr/bin/env python3
"""
PG Pool 优势演示
================

对比传统抢占 vs PG 池化快速抢占的性能差异

运行方式：
    python demo_pg_pool_advantage.py
"""

import time
import ray
import sys
from pathlib import Path

# 添加项目根目录到路径
# __file__ -> demo_pg_pool_advantage.py
# .parent -> examples/
# .parent.parent -> RayMind/ (项目根目录)
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from schedulemesh.simple import SimpleScheduler


@ray.remote
class DemoWorker:
    def __init__(self, name: str, labels: dict):
        self.name = name
        self.labels = labels
        print(f"🤖 Worker '{name}' 启动完成")
    
    def work(self, duration: float = 1.0) -> str:
        time.sleep(duration)
        return f"Worker {self.name} 完成任务"
    
    def cancel(self, task_id: str) -> dict:
        print(f"❌ Worker {self.name} 被抢占")
        return {"success": True}


def demo_traditional_vs_pg_pool():
    """演示传统抢占 vs PG池化抢占的性能对比"""
    
    print("🚀 PG Pool 优势演示")
    print("=" * 60)
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True, local_mode=True, num_cpus=4)
        print("📋 创建新的本地 Ray 集群")
    
    # 场景 1: 传统抢占（不使用 PG 池）
    print("\n📊 场景 1: 传统抢占模式")
    print("-" * 40)
    
    try:
        scheduler_traditional = SimpleScheduler("demo-traditional")
        
        # 创建传统资源池
        scheduler_traditional.ensure_pool(
            name="traditional-pool",
            resources={"cpu": 2.0, "memory": 4096.0},
        )
        
        # 提交低优任务
        low_task = scheduler_traditional.submit(
            task_id="low-traditional",
            pool="traditional-pool",
            actor_class=DemoWorker,
            resources={"cpu": 2.0, "memory": 4096.0},
            priority=3.0,
            labels={"priority": "low"},
        )
        print(f"✅ 低优任务提交: {low_task.get('success')}")
        
        time.sleep(0.2)  # 让低优任务运行一会儿
        
        # 提交高优任务，测量抢占时间
        print("⏱️  测量传统抢占时间...")
        start_time = time.time()
        
        high_task = scheduler_traditional.submit(
            task_id="high-traditional",
            pool="traditional-pool",
            actor_class=DemoWorker,
            resources={"cpu": 2.0, "memory": 4096.0},
            priority=9.0,
            labels={"priority": "high"},
        )
        
        traditional_duration = time.time() - start_time
        print(f"📤 高优任务提交: {high_task.get('success')}")
        print(f"⏱️  传统抢占耗时: {traditional_duration:.3f} 秒")
        
        scheduler_traditional.shutdown()
        
    except Exception as e:
        print(f"⚠️  传统模式测试异常: {e}")
        traditional_duration = "N/A"
    
    # 场景 2: PG 池化抢占
    print("\n📊 场景 2: PG 池化快速抢占")
    print("-" * 40)
    
    try:
        scheduler_pg = SimpleScheduler("demo-pg-pool")
        
        # 创建带 PG 池的资源池
        scheduler_pg.ensure_pool(
            name="pg-pool",
            resources={"cpu": 4.0, "memory": 8192.0},
            pg_pool_config={
                "enable": True,
                "high_priority_pg_specs": [
                    {"cpu": 2.0, "memory": 4096.0},  # 预留一个高优 PG
                ],
                "max_dynamic_pgs": 5,
                "enable_pg_reuse": True,
            }
        )
        
        print("✅ PG 池配置完成")
        
        # 提交低优任务（使用动态 PG）
        low_task_pg = scheduler_pg.submit_with_pg_preemption(
            task_id="low-pg",
            pool="pg-pool",
            actor_class=DemoWorker,
            resources={"cpu": 2.0, "memory": 4096.0},
            priority=3.0,
            labels={"priority": "low"},
        )
        print(f"✅ 低优任务提交: {low_task_pg.get('success')}")
        
        time.sleep(0.2)  # 让低优任务运行一会儿
        
        # 提交高优任务，测量 PG 快速启动时间
        print("⏱️  测量 PG 快速启动时间...")
        start_time = time.time()
        
        high_task_pg = scheduler_pg.submit_with_pg_preemption(
            task_id="high-pg",
            pool="pg-pool",
            actor_class=DemoWorker,
            resources={"cpu": 2.0, "memory": 4096.0},
            priority=9.0,
            labels={"priority": "high"},
        )
        
        pg_duration = time.time() - start_time
        print(f"📤 高优任务提交: {high_task_pg.get('success')}")
        print(f"⚡ PG 快速启动耗时: {pg_duration:.3f} 秒")
        
        # 显示 PG 池统计
        pg_stats = scheduler_pg.pg_pool_stats("pg-pool")
        print(f"📊 PG 池统计: {pg_stats.get('total_pgs', 0)} 个 PG，"
              f"{pg_stats.get('available_pgs', 0)} 个可用")
        
        scheduler_pg.shutdown()
        
    except Exception as e:
        print(f"⚠️  PG 模式测试异常: {e}")
        pg_duration = "N/A"
    
    # 性能对比总结
    print("\n" + "=" * 60)
    print("🏆 性能对比总结")
    print("=" * 60)
    
    print(f"📊 传统抢占耗时:     {traditional_duration if isinstance(traditional_duration, str) else f'{traditional_duration:.3f} 秒'}")
    print(f"⚡ PG 快速启动耗时:  {pg_duration if isinstance(pg_duration, str) else f'{pg_duration:.3f} 秒'}")
    
    if isinstance(traditional_duration, float) and isinstance(pg_duration, float):
        if pg_duration > 0:
            speedup = traditional_duration / pg_duration
            print(f"🚀 性能提升倍数:     {speedup:.1f}x")
        print(f"⏱️  延迟降低:        {(traditional_duration - pg_duration) * 1000:.0f}ms")
    
    print("\n💡 PG 池化优势:")
    print("   ✅ 预留资源保障高优任务零等待")
    print("   ✅ PlacementGroup 复用避免创建开销")
    print("   ✅ 物理资源隔离防止碎片化")
    print("   ✅ 完全向后兼容现有代码")

    ray.shutdown()


def demo_pg_pool_features():
    """演示 PG 池的核心功能"""
    
    print("\n🎯 PG 池核心功能演示")
    print("=" * 60)
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True, local_mode=True, num_cpus=4)
        print("📋 创建新的本地 Ray 集群")
    
    scheduler = SimpleScheduler("demo-features")
    
    # 创建 PG 池
    print("🏗️  创建 PG 池配置...")
    scheduler.ensure_pool(
        name="feature-demo-pool",
        resources={"cpu": 4.0, "memory": 8192.0},
        pg_pool_config={
            "enable": True,
            "high_priority_pg_specs": [
                {"cpu": 2.0, "memory": 4096.0},
                {"cpu": 1.0, "memory": 2048.0},
            ],
            "max_dynamic_pgs": 3,
            "enable_pg_reuse": True,
        }
    )
    
    # 显示初始统计
    stats = scheduler.pg_pool_stats("feature-demo-pool")
    print(f"📊 初始 PG 池: {stats.get('high_priority_pgs', 0)} 个高优PG，"
          f"{stats.get('total_pgs', 0)} 个总PG")
    
    # 提交多个任务展示不同PG分配策略
    tasks = []
    
    print("\n🚀 提交不同优先级任务...")
    
    # 高优任务 (使用预留PG)
    task1 = scheduler.submit_with_pg_preemption(
        task_id="high-1",
        pool="feature-demo-pool",
        actor_class=DemoWorker,
        resources={"cpu": 2.0, "memory": 4096.0},
        priority=9.0,
        labels={"type": "high"},
    )
    tasks.append(("高优任务1", task1))
    
    # 普通任务 (创建动态PG)
    task2 = scheduler.submit_with_pg_preemption(
        task_id="normal-1",
        pool="feature-demo-pool",
        actor_class=DemoWorker,
        resources={"cpu": 1.0, "memory": 2048.0},
        priority=5.0,
        labels={"type": "normal"},
    )
    tasks.append(("普通任务1", task2))
    
    # 另一个高优任务 (使用预留PG)
    task3 = scheduler.submit_with_pg_preemption(
        task_id="high-2",
        pool="feature-demo-pool",
        actor_class=DemoWorker,
        resources={"cpu": 1.0, "memory": 2048.0},
        priority=8.0,
        labels={"type": "high"},
    )
    tasks.append(("高优任务2", task3))
    
    # 显示任务提交结果
    for task_name, result in tasks:
        status = "✅ 成功" if result.get("success") else "❌ 失败"
        print(f"   {task_name}: {status}")
    
    # 显示最终PG池统计
    final_stats = scheduler.pg_pool_stats("feature-demo-pool")
    print(f"\n📊 最终 PG 池统计:")
    print(f"   总 PG 数量: {final_stats.get('total_pgs', 0)}")
    print(f"   高优 PG: {final_stats.get('high_priority_pgs', 0)}")
    print(f"   动态 PG: {final_stats.get('dynamic_pgs', 0)}")
    print(f"   已分配 PG: {final_stats.get('allocated_pgs', 0)}")
    print(f"   可用 PG: {final_stats.get('available_pgs', 0)}")
    print(f"   总复用次数: {final_stats.get('total_reuse_count', 0)}")
    
    scheduler.shutdown()
    ray.shutdown()


def main():
    """主演示函数"""
    print("🎪 ScheduleMesh PG Pool 功能演示")
    print("展示 PlacementGroup 池化预分配机制的强大优势\n")
    
    # 演示 1: 性能对比
    demo_traditional_vs_pg_pool()
    
    # 演示 2: 功能展示
    demo_pg_pool_features()
    
    print("\n" + "=" * 60)
    print("🎉 演示完成！")
    print("💡 建议在生产环境中启用 PG 池化，享受快速抢占的强大优势！")


if __name__ == "__main__":
    main()
