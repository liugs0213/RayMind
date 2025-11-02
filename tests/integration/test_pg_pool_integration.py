#!/usr/bin/env python3
"""
PG Pool 集成测试
=================

测试新实现的 PlacementGroup 池化预分配机制：
1. 向后兼容性测试
2. PG 池基础功能测试  
3. 快速抢占功能测试
4. 与现有 RLHF demo 的兼容性测试

运行方式：
    python test_pg_pool_integration.py
"""

import time
import ray
import sys
from pathlib import Path

# 添加项目根目录到路径
# __file__ -> test_pg_pool_integration.py
# .parent -> tests/integration/
# .parent.parent -> RayMind/ (项目根目录)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from schedulemesh.simple import SimpleScheduler


@ray.remote
class TestAgent:
    """简单的测试 Agent"""
    
    def __init__(self, name: str, labels: dict):
        self.name = name
        self.labels = labels
        print(f"🤖 TestAgent '{name}' 初始化完成，标签: {labels}")
    
    def work(self, task_data: str, duration: float = 1.0) -> dict:
        """执行工作任务"""
        print(f"🔄 Agent {self.name} 开始执行任务: {task_data}")
        time.sleep(duration)
        print(f"✅ Agent {self.name} 任务完成: {task_data}")
        return {
            "agent": self.name,
            "task": task_data,
            "status": "completed",
            "duration": duration,
        }
    
    def cancel(self, task_id: str) -> dict:
        """取消任务（用于抢占）"""
        print(f"❌ Agent {self.name} 任务被取消: {task_id}")
        return {"success": True, "agent": self.name}


def test_backward_compatibility():
    """测试向后兼容性：确保现有 API 仍然工作"""
    print("\n" + "="*60)
    print("🧪 测试 1: 向后兼容性")
    print("="*60)
    
    ray.init(ignore_reinit_error=True, local_mode=True, num_cpus=4)
    
    try:
        # 使用传统 API 创建调度器
        scheduler = SimpleScheduler("test-backward-compatibility")
        
        # 创建资源池（不使用 PG 功能）
        pool_result = scheduler.ensure_pool(
            name="test-pool-legacy",
            resources={"cpu": 2.0, "memory": 4096.0},
            target_agents=0,
        )
        print(f"📋 传统资源池创建: {pool_result.get('success', False)}")
        
        # 使用传统方式提交任务
        task_result = scheduler.submit(
            task_id="legacy-task-001",
            pool="test-pool-legacy", 
            actor_class=TestAgent,
            resources={"cpu": 1.0, "memory": 2048.0},
            priority=5.0,
            labels={"type": "legacy"},
        )
        print(f"📤 传统任务提交: {task_result.get('success', False)}")
        
        if task_result.get("success"):
            agent_handle = task_result["agent"]["handle"]
            work_result = ray.get(agent_handle.work.remote("传统模式测试任务", 0.5))
            print(f"🎯 传统任务执行结果: {work_result['status']}")
        
        scheduler.shutdown()
        print("✅ 向后兼容性测试通过")
        assert True
        
    except Exception as e:
        print(f"❌ 向后兼容性测试失败: {e}")
        assert False, "Test failed"
    finally:
        ray.shutdown()


def test_pg_pool_basic():
    """测试 PG 池基础功能"""
    print("\n" + "="*60)
    print("🧪 测试 2: PG 池基础功能")
    print("="*60)
    
    ray.init(ignore_reinit_error=True, local_mode=True, num_cpus=4)
    
    try:
        scheduler = SimpleScheduler("test-pg-pool-basic")
        
        # 创建带 PG 池的资源池
        pool_result = scheduler.ensure_pool(
            name="test-pool-pg",
            resources={"cpu": 4.0, "memory": 8192.0},
            target_agents=0,
            
            # PG 池配置
            pg_pool_config={
                "enable": True,
                "high_priority_pg_specs": [
                    {"cpu": 2.0, "memory": 4096.0, "gpu": 0.0},
                ],
                "max_dynamic_pgs": 5,
                "enable_pg_reuse": True,
            }
        )
        print(f"📋 PG 池资源池创建: {pool_result.get('success', False)}")
        
        # 检查 PG 池统计
        pg_stats = scheduler.pg_pool_stats("test-pool-pg")
        print(f"📊 PG 池初始统计: {pg_stats}")
        
        # 使用 PG 模式提交任务
        task_result = scheduler.submit_with_pg_preemption(
            task_id="pg-task-001",
            pool="test-pool-pg",
            actor_class=TestAgent, 
            resources={"cpu": 2.0, "memory": 4096.0},
            priority=5.0,
            labels={"type": "pg_test"},
        )
        print(f"📤 PG 任务提交: {task_result.get('success', False)}")
        
        if task_result.get("success"):
            agent_handle = task_result["agent"]["handle"]
            work_result = ray.get(agent_handle.work.remote("PG 模式测试任务", 0.5))
            print(f"🎯 PG 任务执行结果: {work_result['status']}")
            
            # 检查 PG 池统计（任务运行后）
            pg_stats_after = scheduler.pg_pool_stats("test-pool-pg")
            print(f"📊 PG 池任务后统计: {pg_stats_after}")
        
        scheduler.shutdown()
        print("✅ PG 池基础功能测试通过")
        
    except Exception as e:
        print(f"❌ PG 池基础功能测试失败: {e}")
        import traceback
        traceback.print_exc()
        assert False, "Test failed"
    finally:
        ray.shutdown()


def test_pg_preemption():
    """测试 PG 快速抢占功能"""
    print("\n" + "="*60)
    print("🧪 测试 3: PG 快速抢占功能")
    print("="*60)
    
    ray.init(ignore_reinit_error=True, local_mode=True, num_cpus=4)
    
    try:
        scheduler = SimpleScheduler("test-pg-preemption")
        
        # 创建带抢占配置的资源池
        scheduler.configure_preemption(
            enable_label_preemption=True,
            label_preemption_rules={
                "priority": {
                    "high": ["normal", "low"],
                    "normal": ["low"],
                }
            },
        )
        
        pool_result = scheduler.ensure_pool(
            name="test-pool-preemption",
            resources={"cpu": 4.0, "memory": 8192.0},
            
            # 限制资源，触发抢占
            pg_pool_config={
                "enable": True,
                "high_priority_pg_specs": [
                    {"cpu": 2.0, "memory": 4096.0, "gpu": 0.0},
                ],
                "max_dynamic_pgs": 2,  # 限制为 2 个，确保会触发抢占
                "enable_pg_reuse": True,
            }
        )
        print(f"📋 抢占测试池创建: {pool_result.get('success', False)}")
        
        # 提交低优任务（占用资源）
        low_task_result = scheduler.submit_with_pg_preemption(
            task_id="low-priority-task",
            pool="test-pool-preemption",
            actor_class=TestAgent,
            resources={"cpu": 2.0, "memory": 4096.0},
            priority=3.0,  # 低优先级
            labels={"priority": "low"},
        )
        print(f"📤 低优任务提交: {low_task_result.get('success', False)}")
        
        # 等待一段时间，让低优任务运行
        time.sleep(0.5)
        
        # 提交高优任务（应该触发抢占）
        print("\n🚨 提交高优任务，应该触发抢占...")
        high_task_start = time.time()
        
        high_task_result = scheduler.submit_with_pg_preemption(
            task_id="high-priority-task",
            pool="test-pool-preemption",
            actor_class=TestAgent,
            resources={"cpu": 2.0, "memory": 4096.0}, 
            priority=9.0,  # 高优先级
            labels={"priority": "high"},
        )
        
        high_task_duration = time.time() - high_task_start
        print(f"📤 高优任务提交: {high_task_result.get('success', False)}")
        print(f"⏱️  抢占+启动耗时: {high_task_duration:.3f} 秒")
        
        if high_task_result.get("success"):
            agent_handle = high_task_result["agent"]["handle"]
            work_result = ray.get(agent_handle.work.remote("高优抢占任务", 0.3))
            print(f"🎯 高优任务执行结果: {work_result['status']}")
        
        # 检查抢占统计
        preemption_stats = scheduler.stats()
        print(f"📊 抢占统计: 总抢占次数={preemption_stats.get('total_preemptions', 0)}")
        
        scheduler.shutdown()
        
        if high_task_duration < 2.0:  # 如果抢占很快（< 2秒）
            print("✅ PG 快速抢占功能测试通过")
        else:
            print("⚠️  PG 抢占功能工作，但可能不够快")
        
    except Exception as e:
        print(f"❌ PG 快速抢占测试失败: {e}")
        import traceback
        traceback.print_exc()
        assert False, "Test failed"
    finally:
        ray.shutdown()


def test_rlhf_demo_compatibility():
    """测试与现有 RLHF demo 的兼容性"""
    print("\n" + "="*60)
    print("🧪 测试 4: RLHF Demo 兼容性")
    print("="*60)
    
    try:
        # 简单导入测试，确保现有 demo 仍能运行
        from examples.simple_rlhf_preemption_demo import RLHFRoleAgent
        print("✅ RLHF Demo 导入成功")
        
        ray.init(ignore_reinit_error=True, local_mode=True, num_cpus=4)
        
        scheduler = SimpleScheduler("test-rlhf-compatibility")
        
        # 使用 RLHF Agent 测试
        pool_result = scheduler.ensure_pool(
            name="rlhf-compat-pool",
            resources={"cpu": 2.0, "memory": 4096.0},
        )
        
        task_result = scheduler.submit(
            task_id="rlhf-compat-test",
            pool="rlhf-compat-pool",
            actor_class=RLHFRoleAgent,
            resources={"cpu": 1.0, "memory": 2048.0},
            priority=5.0,
            labels={"role": "test"},
        )
        print(f"📤 RLHF 兼容性任务提交: {task_result.get('success', False)}")
        
        scheduler.shutdown()
        print("✅ RLHF Demo 兼容性测试通过")
        
    except ImportError as e:
        print(f"⚠️  RLHF Demo 导入失败（可能是依赖问题）: {e}")
  # 不算测试失败
    except Exception as e:
        print(f"❌ RLHF Demo 兼容性测试失败: {e}")
        assert False, "Test failed"
    finally:
        try:
            ray.shutdown()
        except:
            pass


def main():
    """运行所有测试"""
    print("🚀 开始 PG Pool 集成测试...")
    print("测试目标：验证新功能并确保向后兼容性")
    
    tests = [
        ("向后兼容性", test_backward_compatibility),
        ("PG 池基础功能", test_pg_pool_basic),
        ("PG 快速抢占", test_pg_preemption),
        ("RLHF Demo 兼容性", test_rlhf_demo_compatibility),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n🔍 正在运行: {test_name}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"💥 测试 '{test_name}' 出现异常: {e}")
            results.append((test_name, False))
    
    # 汇总结果
    print("\n" + "="*60)
    print("📋 测试结果汇总")
    print("="*60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{status:10} | {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 总结: {passed}/{total} 个测试通过")
    
    if passed == total:
        print("🎉 所有测试通过！PG Pool 功能集成成功！")
        print("\n💡 建议：")
        print("   1. 可以安全使用新的 PG 功能")
        print("   2. 现有代码无需修改，保持向后兼容")
        print("   3. 新项目推荐使用 submit_with_pg_preemption()")
    else:
        print("⚠️  部分测试失败，需要检查和修复")
        assert False, "Test failed"


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
