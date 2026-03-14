import sys
import json
import pandas as pd
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QTableWidget, QTableWidgetItem, QHeaderView, 
                             QComboBox, QLabel, QFileDialog, QMessageBox, QApplication)
from PySide6.QtCore import Qt, QThread, Signal

# 尝试导入自定义的引擎类
try:
    from DataProfilingEngine import DataProfilingEngine
except ImportError:
    QMessageBox.critical(None, "错误", "找不到 DataProfilingEngine.py 文件，请确保它在同一目录下。")

# --- 1. 柔和工业风 QSS ---
STYLE_SHEET = """
QWidget {
    background-color: #2F3542; 
    color: #F1F2F6;
    font-family: 'Microsoft YaHei', 'Segoe UI';
    font-size: 13px;
}
QPushButton {
    background-color: #57606F; 
    border: 1px solid #747D8C;
    padding: 8px 20px;
    border-radius: 6px;
    color: white;
    font-weight: bold;
}
QPushButton:hover { background-color: #747D8C; }
QPushButton:disabled { background-color: #3d4450; color: #7f8c8d; }

QTableWidget {
    background-color: #353B48;
    gridline-color: #475062;
    border-radius: 8px;
    border: 1px solid #475062;
}
QHeaderView::section {
    background-color: #2F3542;
    color: #70A1FF;
    padding: 10px;
    border: 1px solid #475062;
    font-weight: bold;
}
QScrollBar:horizontal { height: 12px; background: #2F3542; }
QScrollBar::handle:horizontal { background: #57606F; border-radius: 6px; }
"""

# --- 2. 异步处理线程 ---
class ProfilingWorker(QThread):
    # 使用自定义信号避开系统关键字 'finished'
    result_ready = Signal(dict)
    
    def __init__(self, df):
        super().__init__()
        self.df = df
        
    def run(self):
        try:
            engine = DataProfilingEngine(self.df)
            report = engine.profile_all()
            self.result_ready.emit(report)
        except Exception as e:
            print(f"线程执行错误: {e}")

# --- 3. 主界面类 ---
class TemplateLearningPage(QWidget):
    def __init__(self):
        super().__init__()
        self.current_df = None
        self.worker = None
        self.init_ui()
        self.setStyleSheet(STYLE_SHEET)
        self.setWindowTitle("数据合并工具 - 模板学习")

    def init_ui(self):
        # 设置窗口固定大小 (符合规格说明书)
        self.setFixedSize(1600, 800)
        
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        # --- 顶部操作栏 ---
        top_bar = QHBoxLayout()
        self.btn_import = QPushButton("📁 导入模板文件 (XLSX/CSV)")
        self.status_label = QLabel("等待数据录入...")
        self.status_label.setStyleSheet("color: #A4B0BE; margin-left: 10px;")
        
        self.btn_save = QPushButton("💾 保存配置为 模板.json")
        self.btn_save.setEnabled(False)
        
        # 显式关联点击事件
        self.btn_import.clicked.connect(self.handle_import_clicked)
        self.btn_save.clicked.connect(self.save_template_json)
        
        top_bar.addWidget(self.btn_import)
        top_bar.addWidget(self.status_label)
        top_bar.addStretch()
        top_bar.addWidget(self.btn_save)
        main_layout.addLayout(top_bar)

        # --- 核心表格区 ---
        self.table = QTableWidget()
        self.table.setRowCount(3)
        self.table.setVerticalHeaderLabels(["字段标题", "健康度报告", "清洗策略"])
        
        # 关键美化设置
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents) # 核心：不挤压文字
        self.table.verticalHeader().setFixedWidth(120)
        self.table.verticalHeader().setDefaultSectionSize(150) # 给策略面板留足空间
        
        main_layout.addWidget(self.table)

    def handle_import_clicked(self):
        """点击导入按钮逻辑"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择模板文件", "", "Data Files (*.xlsx *.csv)"
        )
        if not file_path:
            return

        try:
            self.status_label.setText("正在读取文件并探查特征...")
            self.btn_import.setEnabled(False) # 防止重复点击
            
            # 读取数据样本
            if file_path.endswith('.csv'):
                self.current_df = pd.read_csv(file_path, nrows=500)
            else:
                self.current_df = pd.read_excel(file_path, nrows=500)
            
            # 开启异步探查
            self.worker = ProfilingWorker(self.current_df)
            self.worker.result_ready.connect(self.update_table_ui)
            self.worker.start()
            
        except Exception as e:
            QMessageBox.critical(self, "导入失败", f"错误详情: {str(e)}")
            self.btn_import.setEnabled(True)
            self.status_label.setText("导入失败")

    def update_table_ui(self, report):
        """接收分析报告并渲染界面"""
        columns = list(report.keys())
        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels(columns)

        for i, col_name in enumerate(columns):
            data = report[col_name]
            
            # 第一行：标题项 (居中展示)
            item_title = QTableWidgetItem(col_name)
            item_title.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(0, i, item_title)
            
            # 第二行：健康度报告 (富文本感展示)
            stats = data['stats']
            health_info = (
                f"数据类型: {data['inferred_type']}\n"
                f"空值占比: {stats['null_ratio']}\n"
                f"唯一值数: {stats['unique_count']}"
            )
            item_health = QTableWidgetItem(health_info)
            item_health.setFlags(Qt.ItemIsEnabled) # 设为只读
            self.table.setItem(1, i, item_health)
            
            # 第三行：清洗策略配置面板
            config_panel = QWidget()
            panel_layout = QVBoxLayout(config_panel)
            panel_layout.setContentsMargins(10, 10, 10, 10)
            
            l1 = QLabel("空值策略:")
            l1.setStyleSheet("font-size: 11px; color: #70A1FF;")
            cb_null = QComboBox()
            cb_null.addItems(["保留空值", "删除空值行", "填充默认值"])
            
            l2 = QLabel("重复策略:")
            l2.setStyleSheet("font-size: 11px; color: #70A1FF;")
            cb_dup = QComboBox()
            cb_dup.addItems(["保留重复", "仅留首行"])
            
            panel_layout.addWidget(l1)
            panel_layout.addWidget(cb_null)
            panel_layout.addSpacing(8)
            panel_layout.addWidget(l2)
            panel_layout.addWidget(cb_dup)
            
            self.table.setCellWidget(2, i, config_panel)

        self.btn_import.setEnabled(True)
        self.btn_save.setEnabled(True)
        self.status_label.setText(f"分析完成！共计 {len(columns)} 个字段")

    def save_template_json(self):
        """将界面配置导出为标准 JSON"""
        template_config = {
            "headers": [self.table.horizontalHeaderItem(i).text() for i in range(self.table.columnCount())],
            "column_settings": {}
        }
        
        for i in range(self.table.columnCount()):
            col_name = template_config["headers"][i]
            panel = self.table.cellWidget(2, i)
            combos = panel.findChildren(QComboBox)
            
            template_config["column_settings"][col_name] = {
                "null_policy": combos[0].currentText(),
                "duplicate_policy": combos[1].currentText()
            }
            
        try:
            with open("模板.json", "w", encoding="utf-8") as f:
                json.dump(template_config, f, indent=4, ensure_ascii=False)
            QMessageBox.information(self, "导出成功", "配置已持久化为 模板.json")
        except Exception as e:
            QMessageBox.warning(self, "保存失败", f"无法写入文件: {e}")

# --- 程序入口 ---
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = TemplateLearningPage()
    window.show()
    sys.exit(app.exec())