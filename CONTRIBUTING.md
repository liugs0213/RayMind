# ScheduleMesh 贡献指南

感谢您对 ScheduleMesh 项目的关注！我们欢迎社区贡献，无论是代码、文档、测试还是其他形式的贡献。

## 🤝 如何贡献

### 报告问题

如果您发现了 bug 或有功能请求，请：

1. 检查 [Issues](https://git.kanzhun-inc.com/arsenal/ray-mind/-/issues) 是否已存在
2. 创建新的 Issue，包含：
   - 清晰的问题描述
   - 复现步骤
   - 预期行为
   - 实际行为
   - 环境信息（Python 版本、Ray 版本等）

### 提交代码

1. **Fork 仓库**
   ```bash
   git clone https://git.kanzhun-inc.com/your-username/ray-mind.git
   cd ray-mind
   ```

2. **创建分支**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **设置开发环境**
   ```bash
   pip install -e .[dev]
   pre-commit install
   ```

4. **编写代码**
   - 遵循项目代码规范
   - 添加必要的测试
   - 更新相关文档

5. **运行测试**
   ```bash
   pytest
   black --check .
   isort --check-only .
   flake8 .
   mypy .
   ```

6. **提交更改**
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   git push origin feature/your-feature-name
   ```

7. **创建 Pull Request**
   - 填写 PR 模板
   - 关联相关 Issue
   - 等待代码审查

## 📝 代码规范

### Python 代码风格

- 使用 [Black](https://black.readthedocs.io/) 进行代码格式化
- 使用 [isort](https://pycqa.github.io/isort/) 进行导入排序
- 使用 [flake8](https://flake8.pycqa.org/) 进行代码检查
- 使用 [mypy](https://mypy.readthedocs.io/) 进行类型检查

### 提交信息规范

使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

类型包括：
- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档更新
- `style`: 代码格式调整
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

示例：
```
feat(scheduler): add priority-based scheduling strategy

- Implement priority queue for task scheduling
- Add preemption mechanism for high-priority tasks
- Update scheduler metrics collection

Closes #123
```

### 文档规范

- 使用 [Sphinx](https://www.sphinx-doc.org/) 生成文档
- 遵循 [Google 风格](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) 的文档字符串
- 提供完整的 API 文档
- 包含使用示例

## 🧪 测试指南

### 测试类型

1. **单元测试**：测试单个函数或类
2. **集成测试**：测试模块间的交互
3. **性能测试**：测试系统性能
4. **端到端测试**：测试完整工作流

### 测试规范

- 测试覆盖率应达到 80% 以上
- 使用 `pytest` 作为测试框架
- 测试文件命名：`test_*.py` 或 `*_test.py`
- 测试函数命名：`test_*`
- 使用描述性的测试名称

### 运行测试

```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/test_scheduler.py

# 运行带覆盖率的测试
pytest --cov=schedulemesh

# 运行性能测试
pytest -m performance

# 运行集成测试
pytest -m integration
```

## 🔌 插件开发

### 创建自定义插件

1. **调度策略插件**
   ```python
   from schedulemesh.plugins import SchedulingStrategyPlugin
   
   class MySchedulingPlugin(SchedulingStrategyPlugin):
       def score(self, task, resources):
           # 实现自定义打分逻辑
           pass
       
       def priority(self, task):
           # 实现自定义优先级计算
           pass
       
       def preemption_policy(self, high_priority_task, low_priority_task):
           # 实现自定义抢占策略
           pass
   ```

2. **分片策略插件**
   ```python
   from schedulemesh.plugins import DispatchStrategyPlugin
   
   class MyDispatchPlugin(DispatchStrategyPlugin):
       def python_dispatch_fn(self, data, workers_a, workers_b):
           # 实现自定义 Python 分片
           pass
       
       def torch_dispatch_fn(self, tensor, workers_a, workers_b):
           # 实现自定义 PyTorch 分片
           pass
   ```

### 插件测试

```python
def test_my_plugin():
    plugin = MySchedulingPlugin("my_plugin")
    assert plugin.initialize({}) == True
    assert plugin.score(mock_task, mock_resources) > 0
    plugin.cleanup()
```

## 📚 文档贡献

### 文档类型

1. **API 文档**：函数、类、模块的详细说明
2. **教程**：step-by-step 的使用指南
3. **示例**：实际使用场景的代码示例
4. **故障排查**：常见问题和解决方案

### 文档更新

- 修改代码时同步更新相关文档
- 添加新功能时提供使用示例
- 保持文档的准确性和时效性

## 🐛 Bug 修复

### 修复流程

1. 复现 bug
2. 编写测试用例
3. 修复代码
4. 确保测试通过
5. 更新相关文档

### Bug 报告模板

```markdown
## Bug 描述
简要描述 bug 的内容

## 复现步骤
1. 执行命令 '...'
2. 点击 '...'
3. 查看错误

## 预期行为
描述您期望的正确行为

## 实际行为
描述实际发生的错误行为

## 环境信息
- Python 版本：
- Ray 版本：
- ScheduleMesh 版本：
- 操作系统：
- 其他相关信息：

## 附加信息
添加任何其他有助于解决问题的信息
```

## 🚀 性能优化

### 性能测试

- 使用 `pytest-benchmark` 进行性能测试
- 监控内存使用情况
- 测试不同负载下的表现

### 优化建议

- 避免不必要的对象创建
- 使用适当的数据结构
- 优化算法复杂度
- 减少网络通信开销

## 📞 获取帮助

- 📧 邮箱：liugaosheng@kanzhun.com
- 🐛 问题反馈：[GitLab Issues](https://git.kanzhun-inc.com/arsenal/ray-mind/-/issues)
- 💬 讨论：[项目 Wiki](https://git.kanzhun-inc.com/arsenal/ray-mind)

## 📄 许可证

通过贡献代码，您同意您的贡献将在 [Apache 2.0 许可证](LICENSE) 下发布。

感谢您的贡献！🎉

