import sys
import json
import pandas as pd
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QTableWidget, QTableWidgetItem, QHeaderView, 
                             QComboBox, QLabel, QFileDialog, QScrollArea, QMessageBox)
from PySide6.QtCore import Qt, QThread, Signal
from DataProfilingEngine import DataProfilingEngine

# --- 样式表 (QSS)：冷色系工业风 ---
STYLE_SHEET = """
QWidget {
    background-color: #1E1E24;
    color: #D1D1D1;
    font-family: 'Segoe UI', sans-serif;
}
QPushButton {
    background-color: #0984E3;
    border: none;
    padding: 8px 15px;
    border-radius: 4px;
    font-weight: bold;
}
QPushButton:hover { background-color: #74B9FF; }
QTableWidget {
    background-color: #2D3436;
    gridline-color: #636E72;
    border: 1px solid #636E72;
}
QHeaderView::section {
    background-color: #0984E3;
    padding: 4px;
    border: 1px solid #636E72;
    font-weight: bold;
}
QComboBox {
    background-color: #2D3436;
    border: 1px solid #0984E3;
    padding: 2px;
}
"""

# --- 异步处理：探查任务线程 ---
class ProfilingWorker(QThread):
    result_ready = Signal(dict)
    
    def __init__(self, df):
        super().__init__()
        self.df = df
        
    def run(self):
        engine = DataProfilingEngine(self.df)
        report = engine.profile_all()
        self.result_ready.emit(report)

# --- 主界面：模块 B ---
class TemplateLearningPage(QWidget):
    def __init__(self):
        super().__init__()
        self.current_df = None
        self.init_ui()
        self.setStyleSheet(STYLE_SHEET)

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # 1. 顶部操作栏
        top_bar = QHBoxLayout()
        self.btn_import = QPushButton("📁 导入模板文件 (XLSX/CSV)")
        self.btn_import.clicked.connect(self.import_template)
        
        self.status_label = QLabel("等待导入...")
        self.btn_save = QPushButton("💾 保存模板.json")
        self.btn_save.setEnabled(False)
        self.btn_save.clicked.connect(self.save_template_json)
        
        top_bar.addWidget(self.btn_import)
        top_bar.addStretch()
        top_bar.addWidget(self.status_label)
        top_bar.addWidget(self.btn_save)
        layout.addLayout(top_bar)

        # 2. 核心表格区 (使用 ScrollArea 包装)
        self.table = QTableWidget()
        self.table.setRowCount(3)  # 固定三行：标题、统计信息、配置项
        self.table.setVerticalHeaderLabels(["原始标题", "健康度报告", "清洗策略配置"])
        layout.addWidget(self.table)

    def import_template(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "选择模板文件", "", "Data Files (*.xlsx *.csv)")
        if not file_path:
            return

        try:
            if file_path.endswith('.csv'):
                self.current_df = pd.read_csv(file_path, nrows=1000) # 仅读前1000行用于展示
            else:
                self.current_df = pd.read_excel(file_path, nrows=1000)
            
            self.status_label.setText("正在深度探查数据健康度...")
            self.start_profiling()
        except Exception as e:
            QMessageBox.critical(self, "导入失败", f"无法读取文件: {str(e)}")

    def start_profiling(self):
        """启动异步线程执行模块 A 逻辑"""
        self.worker = ProfilingWorker(self.current_df)
        self.worker.result_ready.connect(self.update_table_with_report)
        self.worker.start()

    def update_table_with_report(self, report):
        """将模块 A 的分析字典转化为表格组件"""
        columns = list(report.keys())
        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels(columns)

        for i, col_name in enumerate(columns):
            data = report[col_name]
            
            # 第一行：原始标题
            self.table.setItem(0, i, QTableWidgetItem(col_name))
            
            # 第二行：健康度摘要 (多行展示)
            stats_text = (f"类型: {data['inferred_type']}\n"
                          f"空值率: {data['stats']['null_ratio']}\n"
                          f"唯一值: {data['stats']['unique_count']}")
            item_stats = QTableWidgetItem(stats_text)
            item_stats.setFlags(Qt.ItemIsEnabled) # 不可编辑
            self.table.setItem(1, i, item_stats)
            
            # 第三行：配置下拉菜单 (Null Strategy)
            config_widget = QWidget()
            config_layout = QVBoxLayout(config_widget)
            
            combo_null = QComboBox()
            combo_null.addItems(["保留空值", "删除空值行", "填充默认值"])
            
            combo_dup = QComboBox()
            combo_dup.addItems(["允许重复", "删除重复(仅留首行)"])
            
            config_layout.addWidget(QLabel("空值策略:"))
            config_layout.addWidget(combo_null)
            config_layout.addWidget(QLabel("重复策略:"))
            config_layout.addWidget(combo_dup)
            
            self.table.setCellWidget(2, i, config_widget)

        # 自动调整行高和列宽
        self.table.resizeRowsToContents()
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.status_label.setText("探查完成，请配置模板策略")
        self.btn_save.setEnabled(True)

    def save_template_json(self):
        """序列化配置到 template.json"""
        template_data = {
            "headers": [self.table.horizontalHeaderItem(i).text() for i in range(self.table.columnCount())],
            "configs": []
        }
        
        # 遍历每一列获取配置
        for i in range(self.table.columnCount()):
            widget = self.table.cellWidget(2, i)
            combos = widget.findChildren(QComboBox)
            template_data["configs"].append({
                "column": template_data["headers"][i],
                "null_strategy": combos[0].currentText(),
                "dup_strategy": combos[1].currentText()
            })
            
        with open("模板.json", "w", encoding="utf-8") as f:
            json.dump(template_data, f, indent=4, ensure_ascii=False)
        
        QMessageBox.information(self, "成功", "模板配置已保存为 模板.json")

# --- 启动测试 ---
if __name__ == "__main__":
    from PySide6.QtWidgets import QApplication
    app = QApplication(sys.argv)
    window = TemplateLearningPage()
    window.resize(1200, 600)
    window.show()
    sys.exit(app.exec())