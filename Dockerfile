# ScheduleMesh Docker 配置

# 基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV RAY_DISABLE_IMPORT_WARNING=1

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt requirements-dev.txt ./

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 安装 ScheduleMesh
RUN pip install -e .

# 创建非 root 用户
RUN useradd -m -u 1000 schedulemesh && \
    chown -R schedulemesh:schedulemesh /app
USER schedulemesh

# 暴露端口
EXPOSE 8000 8001 8002

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# 启动命令
CMD ["python", "-m", "schedulemesh.server"]


