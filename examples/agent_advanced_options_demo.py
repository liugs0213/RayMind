"""
Agent高级配置示例

展示如何：
1. 设置Ray actor的资源需求（CPU、GPU、Memory等）
2. 配置Ray actor的高级选项（max_retries、lifetime等）
3. 传递自定义初始化参数给Agent
"""

import ray
from schedulemesh.core import RayScheduler
from schedulemesh.core.agent_actor import AgentActor


def demo_basic_resource_configuration():
    """示例1: 基础资源配置"""
    print("\n=== 示例1: 基础资源配置 ===")
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True)
        print("📋 创建新的本地 Ray 集群")
    scheduler = RayScheduler("resource-demo")
    
    try:
        # 创建资源池
        scheduler.create_pool(
            name="gpu-pool",
            labels={"type": "gpu"},
            resources={"cpu": 2.0, "memory": 4096.0, "gpu": 1.0},
            target_agents=1,
        )
        
        # 创建Agent，指定资源需求
        result = scheduler.create_agent(
            name="gpu-agent-1",
            pool="gpu-pool",
            actor_class=AgentActor,
            resources={
                "cpu": 2.0,      # 2个CPU核心
                "memory": 4096.0, # 4GB内存
                "gpu": 1.0,       # 1个GPU
            },
        )
        
        print(f"Agent创建成功: {result['success']}")
        if result['success']:
            agent_info = result['agent']
            print(f"  - Name: {agent_info['name']}")
            print(f"  - Resources: {agent_info['resources']}")
        
    finally:
        scheduler.shutdown()
        ray.shutdown()


def demo_ray_actor_options():
    """示例2: Ray Actor高级选项"""
    print("\n=== 示例2: Ray Actor高级选项 ===")
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True)
        print("📋 创建新的本地 Ray 集群")
    scheduler = RayScheduler("options-demo")
    
    try:
        # 创建资源池
        scheduler.create_pool(
            name="reliable-pool",
            labels={"reliability": "high"},
            resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
            target_agents=1,
        )
        
        # 创建Agent，配置Ray actor选项
        result = scheduler.create_agent(
            name="reliable-agent",
            pool="reliable-pool",
            actor_class=AgentActor,
            ray_options={
                # Actor失败后的最大重试次数
                "max_restarts": 3,
                
                # Actor生命周期策略
                # "lifetime": "detached",  # detached模式，不随创建者退出
                
                # 自定义actor名称（用于重新连接）
                "name": "my-persistent-agent",
                
                # 指定运行的节点（可选）
                # "resources": {"special_hardware": 1.0},
                
                # 并发调用限制
                "max_concurrency": 10,
            },
        )
        
        print(f"Agent创建成功: {result['success']}")
        if result['success']:
            print("  配置的Ray选项已应用")
        
    finally:
        scheduler.shutdown()
        ray.shutdown()


def demo_custom_agent_initialization():
    """示例3: 自定义Agent初始化参数"""
    print("\n=== 示例3: 自定义Agent初始化参数 ===")
    
    # 注意: Ray不支持继承@ray.remote装饰的类
    # 我们使用标准的AgentActor，通过actor_kwargs传递参数
    # actor_kwargs会传递给AgentActor的__init__
    # AgentActor继承自MetricsReportingAgent，支持以下参数：
    #   - report_interval: float = 5.0
    #   - max_pending_reports: int = 16
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True)
        print("📋 创建新的本地 Ray 集群")
    scheduler = RayScheduler("custom-demo")
    
    try:
        # 创建资源池
        scheduler.create_pool(
            name="custom-pool",
            labels={"type": "custom"},
            resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
            target_agents=1,
        )
        
        # 创建Agent，使用支持的参数
        result = scheduler.create_agent(
            name="custom-agent-1",
            pool="custom-pool",
            actor_class=AgentActor,
            # 传递MetricsReportingAgent支持的参数
            actor_kwargs={
                "report_interval": 2.0,        # 每2秒上报一次指标
                "max_pending_reports": 32,     # 最多缓存32个待上报的指标
            },
        )
        
        print(f"Agent创建成功: {result['success']}")
        if result['success']:
            print("  - report_interval: 2.0s")
            print("  - max_pending_reports: 32")
        
    finally:
        scheduler.shutdown()
        ray.shutdown()


def demo_combined_configuration():
    """示例4: 组合配置 - 资源+选项+参数"""
    print("\n=== 示例4: 组合配置 ===")
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True)
        print("📋 创建新的本地 Ray 集群")
    scheduler = RayScheduler("combined-demo")
    
    try:
        # 创建高性能资源池
        scheduler.create_pool(
            name="high-perf-pool",
            labels={"performance": "high", "tier": "premium"},
            resources={"cpu": 4.0, "memory": 8192.0, "gpu": 1.0},
            target_agents=2,
        )
        
        # 创建高性能Agent，组合所有配置选项
        result = scheduler.create_agent(
            name="premium-agent",
            pool="high-perf-pool",
            actor_class=AgentActor,
            # 1. 资源配置
            resources={
                "cpu": 4.0,
                "memory": 8192.0,
                "gpu": 1.0,
            },
            # 2. Ray actor选项
            ray_options={
                "max_restarts": 5,
                "max_concurrency": 20,
                "name": "premium-agent-persistent",
            },
            # 3. 初始化参数（如果Agent支持）
            actor_kwargs={
                "report_interval": 1.0,  # MetricsReportingAgent支持的参数
                "max_pending_reports": 32,
            },
        )
        
        print(f"高性能Agent创建成功: {result['success']}")
        if result['success']:
            agent_info = result['agent']
            print(f"  - Name: {agent_info['name']}")
            print(f"  - Resources: {agent_info['resources']}")
            print(f"  - Status: {agent_info['status']}")
        
    finally:
        scheduler.shutdown()
        ray.shutdown()


def demo_custom_resources():
    """示例5: 自定义资源类型"""
    print("\n=== 示例5: 自定义资源类型 ===")
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True)
        print("📋 创建新的本地 Ray 集群")
    scheduler = RayScheduler("custom-resource-demo")
    
    try:
        # 创建使用自定义资源的资源池
        scheduler.create_pool(
            name="special-pool",
            labels={"hardware": "special"},
            resources={
                "cpu": 1.0,
                "memory": 512.0,
                "gpu": 0.0,
                "special_hardware": 1.0,  # 自定义资源
            },
            target_agents=2,
        )
        
        # 创建使用自定义资源的Agent
        result = scheduler.create_agent(
            name="special-agent",
            pool="special-pool",
            actor_class=AgentActor,
            resources={
                "cpu": 1.0,
                "memory": 512.0,
                "gpu": 0.0,
                "special_hardware": 1.0,  # 请求自定义资源
            },
        )
        
        print(f"特殊资源Agent创建成功: {result['success']}")
        
    finally:
        scheduler.shutdown()
        ray.shutdown()


def main():
    """运行所有示例"""
    print("=" * 60)
    print("Agent高级配置示例")
    print("=" * 60)
    
    demo_basic_resource_configuration()
    demo_ray_actor_options()
    demo_custom_agent_initialization()
    demo_combined_configuration()
    demo_custom_resources()
    
    print("\n" + "=" * 60)
    print("所有示例运行完成！")
    print("=" * 60)
    
    print("\n📚 总结:")
    print("1. ✅ 支持设置CPU、GPU、Memory等资源")
    print("2. ✅ 支持Ray actor选项（max_restarts、max_concurrency等）")
    print("3. ✅ 支持传递自定义初始化参数（actor_kwargs）")
    print("4. ✅ 支持自定义资源类型")
    print("5. ✅ 所有配置可以组合使用")


if __name__ == "__main__":
    main()

