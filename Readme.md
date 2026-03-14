Data Merge Utility (DMU) 🚀
Data Merge Utility 是一款基于 Python 和 PySide6 开发的高效、美观的工业级数据处理工具。它旨在解决多源数据合并过程中的表头对齐、格式清洗及标准化导出问题，特别适合需要频繁处理重复性 Excel/CSV 任务的办公场景。
✨ 核心特性
🧠 模板学习 (Template Learning)：通过导入标准样表，系统自动探查数据特征（类型、空值比、唯一值），支持可视化配置每一列的清洗策略（去重、去空）。
🔗 智能多文件合并：
解耦设计：独立加载 JSON 配置文件，支持动态切换不同业务模板。
子集兼容：自动识别待合并文件是否包含目标字段，支持冗余列自动剔除。
异步执行：基于多线程 (QThread) 处理大批量数据，界面流畅不卡顿。
📊 工业级导出：
支持导出为 .xlsx 格式。
自动样式应用：内置自动列宽调整、指定“等线”字体、去除冗余边框，产出即报表级。
🎨 莫兰迪风格 UI：深色工业风设计，针对 1600x800 分辨率深度优化，视觉感官舒适。
🛠️ 技术栈
GUI 框架: PySide6 (Qt for Python)
数据处理: Pandas
Excel 引擎: Openpyxl
架构: 异步多线程架构，配置与逻辑完全解耦
🚀 快速开始

1. 克隆仓库

Bash

git clone https://github.com/YourUsername/Data-Merge-Utility.git
cd Data-Merge-Utility

2. 安装依赖
确保你的环境为 Python 3.10+，然后运行：

Bash

pip install pyside6 pandas openpyxl

3. 运行程序

Bash

python MainApp.py

📖 使用指南
第一步：进入 “模板学习” 页面，导入你的标准 Excel 文件。在底部的配置面板选择清洗策略，点击“保存配置”。
第二步：切换到 “多文件合并” 页面。
第三步：在下拉框中选择刚才生成的 模板.json。
第四步：点击“选择文件”导入需要合并的一堆文件，点击“开始执行”。
第五步：任务完成后，点击“导出”获取标准化的结果。
📂 项目结构

Plaintext

├── MainApp.py                # 主程序入口及导航逻辑
├── TemplateLearningPage.py   # 模块 A: 模板学习与规则定义
├── MultiFileMergePage.py     # 模块 C: 多文件批量合并与清洗
├── DataProfilingEngine.py    # 核心引擎: 数据特征探查
└── 模板.json                  # (自动生成) 存储清洗规则

📄 开源协议
MIT License
