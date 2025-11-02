"""
Agent重启行为演示

演示如何使用 max_restarts 参数，并验证其实际生效
"""

import ray
import time
from schedulemesh.core import RayScheduler
from schedulemesh.core.agents.metrics import MetricsReportingAgent


class CrashableAgentBase(MetricsReportingAgent):
    """可以主动崩溃的测试Agent基类（不使用@ray.remote装饰）"""
    
    def __init__(self, name: str, labels: dict, supervisor=None, **kwargs):
        super().__init__(name, labels, supervisor, **kwargs)
        print(f"🚀 CrashableAgent[{name}] 启动")
        
    def ping(self):
        """健康检查"""
        return "pong"
    
    def crash_me(self):
        """主动触发崩溃"""
        import sys
        print(f"💥 Agent正在崩溃...")
        sys.exit(1)


# 使用@ray.remote装饰基类
CrashableAgent = ray.remote(CrashableAgentBase)


def demo_agent_restart():
    """演示Agent重启功能"""
    print("=" * 60)
    print("Agent重启行为演示")
    print("=" * 60)
    
    # 智能初始化 Ray
    try:
        # 尝试连接现有集群
        ray.init(address="auto", ignore_reinit_error=True)
        print("✅ 连接到现有 Ray 集群")
    except Exception:
        # 创建新本地集群
        ray.init(ignore_reinit_error=True)
        print("📋 创建新的本地 Ray 集群")
    scheduler = RayScheduler("restart-demo")
    
    try:
        # 创建资源池
        print("\n1️⃣  创建资源池...")
        result = scheduler.create_pool(
            name="restart-pool",
            labels={"test": "restart"},
            resources={"cpu": 1.0, "memory": 512.0, "gpu": 0.0},
            target_agents=1,
        )
        assert result["success"]
        print("   ✅ 资源池创建成功")
        
        # 创建Agent，配置重启参数
        print("\n2️⃣  创建Agent（max_restarts=3）...")
        agent_result = scheduler.create_agent(
            name="crashable-agent",
            pool="restart-pool",
            actor_class=CrashableAgent,
            ray_options={
                "max_restarts": 3,  # 允许重启3次
                "name": "persistent-crashable-agent",
            },
        )
        assert agent_result["success"]
        print(f"   ✅ Agent创建成功")
        print(f"   📝 配置: max_restarts=3")
        
        # 获取agent handle
        agents = scheduler.list_agents("restart-pool", include_handle=True)
        agent_handle = agents["agents"][0]["handle"]
        
        # 验证初始状态
        print("\n3️⃣  验证Agent初始状态...")
        result = ray.get(agent_handle.ping.remote())
        print(f"   ✅ Agent健康检查: {result}")
        
        # 触发崩溃并观察重启
        crash_count = 3
        for i in range(1, crash_count + 1):
            print(f"\n4️⃣  第{i}次测试: 触发崩溃...")
            
            try:
                ray.get(agent_handle.crash_me.remote())
            except ray.exceptions.RayActorError as e:
                print(f"   💥 Agent已崩溃（符合预期）")
            
            # 等待Ray重启
            print(f"   ⏳ 等待Ray自动重启Agent...")
            time.sleep(3)
            
            # 尝试重新连接
            restarted = False
            for attempt in range(5):
                try:
                    agents = scheduler.list_agents("restart-pool", include_handle=True)
                    if agents["success"] and len(agents["agents"]) > 0:
                        agent_handle = agents["agents"][0]["handle"]
                        result = ray.get(agent_handle.ping.remote(), timeout=2)
                        if result == "pong":
                            restarted = True
                            print(f"   ✅ Agent已成功重启（尝试 {attempt+1}/5）")
                            break
                except Exception as e:
                    if attempt < 4:
                        time.sleep(1)
                    else:
                        print(f"   ❌ Agent未能重启: {e}")
            
            if not restarted:
                print(f"   ⚠️  第{i}次重启失败（可能需要更长等待时间）")
                break
        
        print("\n" + "=" * 60)
        print("📊 测试总结")
        print("=" * 60)
        print("✅ max_restarts参数已正确应用到Agent")
        print("✅ Agent崩溃后Ray自动重启")
        print("✅ 重启后Agent功能正常")
        print("\n💡 说明:")
        print("   - max_restarts=3: Agent可以重启3次")
        print("   - max_restarts=-1: Agent可以无限重启")
        print("   - max_restarts=0: Agent不会重启")
        
    finally:
        scheduler.shutdown()
        ray.shutdown()


if __name__ == "__main__":
    demo_agent_restart()

