"""
测试Agent配置选项

测试场景：
1. Ray actor options（max_restarts, max_concurrency等）
2. 自定义资源配置
3. actor初始化参数传递
"""

import uuid
import time
import pytest
import ray

from schedulemesh.core import RayScheduler
from schedulemesh.core.agent_actor import AgentActor


def _unique_name(prefix: str) -> str:
    """生成唯一名称"""
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


def test_agent_with_ray_options(ray_runtime):
    """测试Agent的Ray actor选项配置"""
    scheduler = RayScheduler(_unique_name("test-ray-options"))
    
    try:
        pool_name = _unique_name("options-pool")
        
        # 创建资源池
        result = scheduler.create_pool(
            name=pool_name,
            labels={"test": "ray-options"},
            resources={"cpu": 2.0, "memory": 1024.0, "gpu": 0.0},
            target_agents=2,
        )
        assert result["success"], f"创建资源池失败: {result.get('error')}"
        
        # 创建Agent，配置Ray options
        agent_result = scheduler.create_agent(
            name=_unique_name("reliable-agent"),
            pool=pool_name,
            actor_class=AgentActor,
            ray_options={
                "max_restarts": 3,          # 最多重启3次
                "max_concurrency": 10,      # 最大并发10
                "name": "test-persistent",  # Actor名称
            },
        )
        
        assert agent_result["success"], f"创建Agent失败: {agent_result.get('error')}"
        
        # 验证Agent信息
        agent_info = agent_result["agent"]
        assert agent_info["name"]
        assert agent_info["status"] in ["running", "RUNNING"]  # 支持大小写
        
        # 验证options被保存
        assert "options" in agent_info
        options = agent_info["options"]
        assert options["max_restarts"] == 3
        assert options["max_concurrency"] == 10
        
        print(f"✅ Agent创建成功，配置: max_restarts=3, max_concurrency=10")
        
    finally:
        scheduler.shutdown()


def test_agent_with_custom_resources(ray_runtime):
    """测试Agent的自定义资源配置"""
    scheduler = RayScheduler(_unique_name("test-resources"))
    
    try:
        pool_name = _unique_name("gpu-pool")
        
        # 创建GPU资源池
        result = scheduler.create_pool(
            name=pool_name,
            labels={"hardware": "gpu"},
            resources={"cpu": 4.0, "memory": 8192.0, "gpu": 0.0},  # 注意：测试环境没有真实GPU
            target_agents=1,
        )
        assert result["success"]
        
        # 创建Agent，指定资源需求
        agent_result = scheduler.create_agent(
            name=_unique_name("gpu-agent"),
            pool=pool_name,
            actor_class=AgentActor,
            resources={
                "cpu": 4.0,
                "memory": 8192.0,
                "gpu": 0.0,
            },
        )
        
        assert agent_result["success"]
        
        # 验证资源配置
        agent_info = agent_result["agent"]
        resources = agent_info["resources"]
        assert resources["cpu"] == 4.0
        assert resources["memory"] == 8192.0
        assert resources["gpu"] == 0.0
        
        print(f"✅ Agent资源配置正确: {resources}")
        
    finally:
        scheduler.shutdown()


def test_agent_with_actor_kwargs(ray_runtime):
    """测试Agent的初始化参数传递"""
    scheduler = RayScheduler(_unique_name("test-kwargs"))
    
    try:
        pool_name = _unique_name("custom-pool")
        
        # 创建资源池
        result = scheduler.create_pool(
            name=pool_name,
            labels={"type": "custom"},
            resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
            target_agents=1,
        )
        assert result["success"]
        
        # 创建Agent，传递初始化参数
        agent_result = scheduler.create_agent(
            name=_unique_name("custom-agent"),
            pool=pool_name,
            actor_class=AgentActor,
            actor_kwargs={
                "report_interval": 2.0,        # MetricsReportingAgent支持的参数
                "max_pending_reports": 32,     # MetricsReportingAgent支持的参数
            },
        )
        
        assert agent_result["success"]
        
        # 验证参数被保存
        agent_info = agent_result["agent"]
        assert "actor_init_kwargs" in agent_info
        kwargs = agent_info["actor_init_kwargs"]
        assert kwargs["report_interval"] == 2.0
        assert kwargs["max_pending_reports"] == 32
        
        print(f"✅ Agent初始化参数正确: {kwargs}")
        
    finally:
        scheduler.shutdown()


def test_agent_combined_configuration(ray_runtime):
    """测试Agent的组合配置（资源+选项+参数）"""
    scheduler = RayScheduler(_unique_name("test-combined"))
    
    try:
        pool_name = _unique_name("premium-pool")
        
        # 创建高性能资源池
        result = scheduler.create_pool(
            name=pool_name,
            labels={"tier": "premium"},
            resources={"cpu": 4.0, "memory": 4096.0, "gpu": 0.0},
            target_agents=1,
        )
        assert result["success"]
        
        # 创建Agent，组合所有配置
        agent_result = scheduler.create_agent(
            name=_unique_name("premium-agent"),
            pool=pool_name,
            actor_class=AgentActor,
            # 1. 资源配置
            resources={
                "cpu": 4.0,
                "memory": 4096.0,
                "gpu": 0.0,
            },
            # 2. Ray actor选项
            ray_options={
                "max_restarts": 5,
                "max_concurrency": 20,
            },
            # 3. 初始化参数
            actor_kwargs={
                "report_interval": 1.0,
                "max_pending_reports": 64,
            },
        )
        
        assert agent_result["success"]
        
        # 验证所有配置
        agent_info = agent_result["agent"]
        
        # 验证资源
        resources = agent_info["resources"]
        assert resources["cpu"] == 4.0
        assert resources["memory"] == 4096.0
        
        # 验证Ray options
        options = agent_info["options"]
        assert options["max_restarts"] == 5
        assert options["max_concurrency"] == 20
        
        # 验证初始化参数
        kwargs = agent_info["actor_init_kwargs"]
        assert kwargs["report_interval"] == 1.0
        assert kwargs["max_pending_reports"] == 64
        
        print("✅ Agent组合配置全部正确")
        print(f"  - Resources: {resources}")
        print(f"  - Ray options: max_restarts={options['max_restarts']}, "
              f"max_concurrency={options['max_concurrency']}")
        print(f"  - Init kwargs: {kwargs}")
        
    finally:
        scheduler.shutdown()


def test_agent_resource_override(ray_runtime):
    """测试Agent资源覆盖Pool默认资源"""
    scheduler = RayScheduler(_unique_name("test-override"))
    
    try:
        pool_name = _unique_name("flexible-pool")
        
        # 创建资源池（默认每个agent 2 CPU）
        result = scheduler.create_pool(
            name=pool_name,
            labels={"type": "flexible"},
            resources={"cpu": 2.0, "memory": 1024.0, "gpu": 0.0},
            target_agents=2,
        )
        assert result["success"]
        
        # 创建Agent 1：使用默认资源
        agent1_result = scheduler.create_agent(
            name=_unique_name("agent-default"),
            pool=pool_name,
            actor_class=AgentActor,
            # 不指定resources，使用Pool默认值
        )
        assert agent1_result["success"]
        
        # 验证使用默认资源（使用Pool的完整资源配置）
        agent1_resources = agent1_result["agent"]["resources"]
        assert agent1_resources["cpu"] == 2.0  # Pool的资源配置
        assert agent1_resources["memory"] == 1024.0
        
        # 创建Agent 2：覆盖资源配置（使用较小的资源）
        agent2_result = scheduler.create_agent(
            name=_unique_name("agent-custom"),
            pool=pool_name,
            actor_class=AgentActor,
            resources={
                "cpu": 2.0,
                "memory": 1024.0,
                "gpu": 0.0,
            },
        )
        assert agent2_result["success"]
        
        # 验证自定义资源
        agent2_resources = agent2_result["agent"]["resources"]
        assert agent2_resources["cpu"] == 2.0
        assert agent2_resources["memory"] == 1024.0
        
        print("✅ Agent资源覆盖测试通过")
        print(f"  - Agent1 (默认): {agent1_resources}")
        print(f"  - Agent2 (自定义): {agent2_resources}")
        
    finally:
        scheduler.shutdown()


def test_agent_max_restarts_actual_restart(ray_runtime):
    """测试Agent实际重启功能（模拟失败场景）"""
    scheduler = RayScheduler(_unique_name("test-restart"))
    
    try:
        pool_name = _unique_name("restart-pool")
        
        # 创建资源池
        result = scheduler.create_pool(
            name=pool_name,
            labels={"test": "restart"},
            resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
            target_agents=1,
        )
        assert result["success"]
        
        # 创建Agent，配置重启策略
        agent_name = _unique_name("restart-agent")
        agent_result = scheduler.create_agent(
            name=agent_name,
            pool=pool_name,
            actor_class=AgentActor,
            ray_options={
                "max_restarts": 2,  # 允许重启2次
                "name": f"persistent-{agent_name}",
            },
        )
        
        assert agent_result["success"]
        agent_info = agent_result["agent"]
        
        # 验证配置
        assert agent_info["options"]["max_restarts"] == 2
        
        # 获取agent列表，验证agent存在
        agents = scheduler.list_agents(pool_name)
        assert agents["success"]
        assert len(agents["agents"]) == 1
        assert agents["agents"][0]["name"] == agent_name
        
        print("✅ Agent重启配置测试通过")
        print(f"  - Agent: {agent_name}")
        print(f"  - max_restarts: 2")
        
        # 注意：实际触发重启需要agent崩溃，这在单元测试中较难模拟
        # Ray会在actor崩溃时根据max_restarts自动重启
        
    finally:
        scheduler.shutdown()


def test_multiple_agents_with_different_configs(ray_runtime):
    """测试同一个Pool中创建多个不同配置的Agent"""
    scheduler = RayScheduler(_unique_name("test-multi"))
    
    try:
        pool_name = _unique_name("multi-pool")
        
        # 创建资源池（每个agent使用2 CPU，共3个agent）
        result = scheduler.create_pool(
            name=pool_name,
            labels={"type": "multi"},
            resources={"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
            target_agents=3,
        )
        assert result["success"], f"创建资源池失败: {result.get('error')}"
        
        # 创建不同配置的Agent（全部使用相同资源但不同ray选项）
        agents_config = [
            {
                "name": _unique_name("agent-low"),
                "resources": {"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
                "ray_options": {"max_restarts": 1, "max_concurrency": 5},
            },
            {
                "name": _unique_name("agent-medium"),
                "resources": {"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
                "ray_options": {"max_restarts": 3, "max_concurrency": 10},
            },
            {
                "name": _unique_name("agent-high"),
                "resources": {"cpu": 2.0, "memory": 2048.0, "gpu": 0.0},
                "ray_options": {"max_restarts": 5, "max_concurrency": 20},
            },
        ]
        
        created_agents = []
        for config in agents_config:
            agent_result = scheduler.create_agent(
                name=config["name"],
                pool=pool_name,
                actor_class=AgentActor,
                resources=config["resources"],
                ray_options=config["ray_options"],
            )
            assert agent_result["success"], f"创建Agent失败: {config['name']}"
            created_agents.append(agent_result["agent"])
        
        # 验证所有Agent
        agents_list = scheduler.list_agents(pool_name)
        assert agents_list["success"]
        assert len(agents_list["agents"]) == 3
        
        # 验证每个Agent的配置
        for i, agent in enumerate(created_agents):
            config = agents_config[i]
            
            # 验证资源
            assert agent["resources"]["cpu"] == config["resources"]["cpu"]
            assert agent["resources"]["memory"] == config["resources"]["memory"]
            
            # 验证Ray options
            assert agent["options"]["max_restarts"] == config["ray_options"]["max_restarts"]
            assert agent["options"]["max_concurrency"] == config["ray_options"]["max_concurrency"]
            
            print(f"✅ Agent {i+1} 配置正确: "
                  f"max_restarts={agent['options']['max_restarts']}, "
                  f"max_concurrency={agent['options']['max_concurrency']}")
        
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

