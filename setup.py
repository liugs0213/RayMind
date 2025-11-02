"""
ScheduleMesh 项目构建配置

支持多种安装方式和分发策略
"""

from setuptools import setup, find_packages
import os

# 读取 README 文件
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# 读取版本信息
def read_version():
    with open("schedulemesh/__init__.py", "r", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"').strip("'")
    return "0.1.0"

# 读取依赖文件
def read_requirements():
    requirements = []
    if os.path.exists("requirements.txt"):
        with open("requirements.txt", "r", encoding="utf-8") as fh:
            requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]
    return requirements

# 读取开发依赖
def read_dev_requirements():
    dev_requirements = []
    if os.path.exists("requirements-dev.txt"):
        with open("requirements-dev.txt", "r", encoding="utf-8") as fh:
            dev_requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]
    return dev_requirements

setup(
    name="schedulemesh-core",
    version=read_version(),
    author="Arsenal Team",
    author_email="liugaosheng@kanzhun.com",
    description="统一调度与资源管控平面 - ScheduleMesh",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": read_dev_requirements(),
        "full": [
            "schedulemesh-core",
            "schedulemesh-plugins",
            "schedulemesh-examples",
        ],
        "gpu": [
            "torch>=1.9.0",
            "cupy-cuda11x",  # 根据 CUDA 版本调整
        ],
        "monitoring": [
            "prometheus-client>=0.12.0",
            "opentelemetry-api>=1.0.0",
            "opentelemetry-sdk>=1.0.0",
            "opentelemetry-exporter-prometheus>=1.0.0",
        ],
        "database": [
            "sqlalchemy>=1.4.0",
            "alembic>=1.7.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "schedulemesh=schedulemesh.cli:main",
            "schedulemesh-server=schedulemesh.server:main",
        ],
        "schedulemesh.plugins.scheduling": [
            "least_busy=schedulemesh.plugins.scheduling.least_busy:LeastBusyPlugin",
            "priority=schedulemesh.plugins.scheduling.priority:PriorityPlugin",
            "binpacking=schedulemesh.plugins.scheduling.binpacking:BinpackingPlugin",
        ],
        "schedulemesh.plugins.dispatch": [
            "tensor_parallel=schedulemesh.plugins.dispatch.tensor_parallel:TensorParallelPlugin",
            "pipeline_parallel=schedulemesh.plugins.dispatch.pipeline_parallel:PipelineParallelPlugin",
            "sequence_parallel=schedulemesh.plugins.dispatch.sequence_parallel:SequenceParallelPlugin",
        ],
        "schedulemesh.plugins.state": [
            "checkpoint=schedulemesh.plugins.state.checkpoint:CheckpointPlugin",
            "memory=schedulemesh.plugins.state.memory:MemoryPlugin",
            "database=schedulemesh.plugins.state.database:DatabasePlugin",
        ],
    },
    include_package_data=True,
    package_data={
        "schedulemesh": [
            "config/*.yaml",
            "config/*.json",
            "templates/*.html",
            "static/*.css",
            "static/*.js",
        ],
    },
    zip_safe=False,
    keywords=[
        "distributed-computing",
        "scheduling",
        "ray",
        "machine-learning",
        "deep-learning",
        "gpu",
        "cluster",
        "resource-management",
    ],
)


