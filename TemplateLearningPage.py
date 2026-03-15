import sys
import json
import pandas as pd
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QTableWidget, QTableWidgetItem, QHeaderView, 
                             QComboBox, QLabel, QFileDialog, QMessageBox, QApplication,
                             QProgressBar)
from PySide6.QtCore import Qt, QThread, Signal

# 尝试导入自定义的引擎类
try:
    from DataProfilingEngine import DataProfilingEngine
except ImportError:
    DataProfilingEngine = None

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

# 性能优化：大量列时的阈值
LARGE_COLUMN_THRESHOLD = 500

# --- 2. 异步处理线程 ---
class ProfilingWorker(QThread):
    result_ready = Signal(dict)
    progress_update = Signal(int, str)
    
    def __init__(self, df):
        super().__init__()
        self.df = df
        
    def run(self):
        try:
            self.progress_update.emit(10, "正在初始化分析引擎...")
            engine = DataProfilingEngine(self.df)
            self.progress_update.emit(30, "正在分析数据列...")
            report = engine.profile_all()
            self.progress_update.emit(100, "分析完成")
            self.result_ready.emit(report)
        except Exception as e:
            print(f"线程执行错误: {e}")

# --- 3. 主界面类 ---
class TemplateLearningPage(QWidget):
    def __init__(self):
        super().__init__()
        self.current_df = None
        self.worker = None
        self.report_data = None
        self.is_simple_mode = False
        self.init_ui()
        self.setStyleSheet(STYLE_SHEET)
        self.setWindowTitle("数据合并工具 - 模板学习")

    def init_ui(self):
        self.setFixedSize(1600, 800)
        
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        # 顶部操作栏
        top_bar = QHBoxLayout()
        self.btn_import = QPushButton("📁 导入模板文件 (XLSX/CSV)")
        self.status_label = QLabel("等待数据录入...")
        self.status_label.setStyleSheet("color: #A4B0BE; margin-left: 10px;")
        
        self.btn_save = QPushButton("💾 保存配置为 模板.json")
        self.btn_save.setEnabled(False)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedWidth(150)
        self.progress_bar.setVisible(False)
        
        self.btn_import.clicked.connect(self.handle_import_clicked)
        self.btn_save.clicked.connect(self.save_template_json)
        
        top_bar.addWidget(self.btn_import)
        top_bar.addWidget(self.status_label)
        top_bar.addWidget(self.progress_bar)
        top_bar.addStretch()
        top_bar.addWidget(self.btn_save)
        main_layout.addLayout(top_bar)

        # 核心表格区
        self.table = QTableWidget()
        self.table.setRowCount(3)
        self.table.setVerticalHeaderLabels(["字段标题", "健康度报告", "清洗策略"])
        
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.verticalHeader().setFixedWidth(120)
        self.table.verticalHeader().setDefaultSectionSize(150)
        
        # 启用虚拟滚动
        self.table.setVerticalScrollMode(QTableWidget.ScrollPerPixel)
        self.table.setHorizontalScrollMode(QTableWidget.ScrollPerPixel)
        
        main_layout.addWidget(self.table)

    def handle_import_clicked(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择模板文件", "", "Data Files (*.xlsx *.csv)"
        )
        if not file_path:
            return

        try:
            self.status_label.setText("正在读取文件...")
            self.btn_import.setEnabled(False)
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            if file_path.endswith('.csv'):
                self.current_df = pd.read_csv(file_path, nrows=500, dtype=str)
            else:
                self.current_df = pd.read_excel(file_path, nrows=500, dtype=str)
            
            column_count = len(self.current_df.columns)
            
            if column_count > LARGE_COLUMN_THRESHOLD:
                self.is_simple_mode = True
                self.status_label.setText(f"检测到大量列({column_count})，已启用高性能模式")
            else:
                self.is_simple_mode = False
            
            self.worker = ProfilingWorker(self.current_df)
            self.worker.progress_update.connect(self.on_progress_update)
            self.worker.result_ready.connect(self.update_table_ui)
            self.worker.start()
            
        except Exception as e:
            QMessageBox.critical(self, "导入失败", f"错误详情: {str(e)}")
            self.btn_import.setEnabled(True)
            self.progress_bar.setVisible(False)
            self.status_label.setText("导入失败")

    def on_progress_update(self, value, text):
        self.progress_bar.setValue(value)
        self.status_label.setText(text)

    def update_table_ui(self, report):
        self.report_data = report
        
        if self.current_df is not None:
            columns = list(self.current_df.columns)
        else:
            columns = list(report.keys())

        column_count = len(columns)
        self.table.setColumnCount(column_count)
        self.table.setHorizontalHeaderLabels(columns)

        if self.is_simple_mode:
            self._render_simple_mode(columns, report)
        else:
            self._render_batch(columns, report)

        self.btn_import.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.btn_save.setEnabled(True)
        self.status_label.setText(f"分析完成！共计 {column_count} 个字段")

    def _render_simple_mode(self, columns, report):
        self.table.setRowCount(3)
        render_limit = min(100, len(columns))
        
        for i in range(render_limit):
            col_name = columns[i]
            data = report.get(col_name)
            if not data:
                continue
            
            item_title = QTableWidgetItem(col_name)
            item_title.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(0, i, item_title)
            
            stats = data['stats']
            health_info = f"{data['inferred_type']} | 空值:{stats['null_ratio']} | 唯一:{stats['unique_count']}"
            item_health = QTableWidgetItem(health_info)
            item_health.setFlags(Qt.ItemIsEnabled)
            self.table.setItem(1, i, item_health)
            
            item_strategy = QTableWidgetItem("保留空值 | 保留重复")
            item_strategy.setFlags(Qt.ItemIsEnabled)
            self.table.setItem(2, i, item_strategy)

    def _render_batch(self, columns, report):
        for i, col_name in enumerate(columns):
            data = report.get(col_name)
            if not data:
                continue
            
            item_title = QTableWidgetItem(col_name)
            item_title.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(0, i, item_title)
            
            stats = data['stats']
            health_info = (
                f"数据类型: {data['inferred_type']}\n"
                f"空值占比: {stats['null_ratio']}\n"
                f"唯一值数: {stats['unique_count']}"
            )
            item_health = QTableWidgetItem(health_info)
            item_health.setFlags(Qt.ItemIsEnabled) 
            self.table.setItem(1, i, item_health)
            
            self._create_strategy_widget(i, col_name)

    def _create_strategy_widget(self, col_index, col_name):
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
        
        self.table.setCellWidget(2, col_index, config_panel)

    def save_template_json(self):
        template_config = {
            "headers": [self.table.horizontalHeaderItem(i).text() for i in range(self.table.columnCount())],
            "column_settings": {}
        }
        
        if self.report_data:
            all_columns = list(self.report_data.keys())
        else:
            all_columns = [self.table.horizontalHeaderItem(i).text() for i in range(self.table.columnCount())]
        
        for col_name in all_columns:
            col_index = template_config["headers"].index(col_name) if col_name in template_config["headers"] else -1
            
            if col_index >= 0:
                panel = self.table.cellWidget(2, col_index)
                if panel:
                    combos = panel.findChildren(QComboBox)
                    if len(combos) >= 2:
                        template_config["column_settings"][col_name] = {
                            "null_policy": combos[0].currentText(),
                            "duplicate_policy": combos[1].currentText()
                        }
                        continue
            
            template_config["column_settings"][col_name] = {
                "null_policy": "保留空值",
                "duplicate_policy": "保留重复"
            }
            
        try:
            with open("模板.json", "w", encoding="utf-8") as f:
                json.dump(template_config, f, indent=4, ensure_ascii=False, sort_keys=False)
            QMessageBox.information(self, "导出成功", "配置已持久化为 模板.json")
        except Exception as e:
            QMessageBox.warning(self, "保存失败", f"无法写入文件: {e}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = TemplateLearningPage()
    window.show()
    sys.exit(app.exec())
