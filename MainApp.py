import sys
import json
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QStackedWidget, QLabel, QFrame)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QIcon, QFont

# 导入自定义子页面
from TemplateLearningPage import TemplateLearningPage
from MultiFileMergePage import MultiFileMergePage

# --- 全局 UI (莫兰迪工业风) ---
MAIN_STYLE = """
#SideBar {
    background-color: #262B35;
    border-right: 1px solid #475062;
    min-width: 240px;
    max-width: 240px;
}

#NavButton {
    background-color: transparent;
    border: none;
    color: #A4B0BE;
    text-align: left;
    padding: 15px 25px;
    font-size: 14px;
    font-weight: bold;
    border-radius: 0px;
}

#NavButton:hover {
    background-color: #353B48;
    color: #70A1FF;
}

#NavButton[active="true"] {
    background-color: #353B48;
    color: #70A1FF;
    border-left: 4px solid #70A1FF;
}

#LogoLabel {
    color: #70A1FF;
    font-size: 20px;
    font-weight: bold;
    padding: 30px 20px;
    border-bottom: 1px solid #475062;
    margin-bottom: 20px;
}

#FooterLabel {
    color: #57606F;
    font-size: 11px;
    padding: 15px;
}
"""

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Data Merge Utility v1.0")
        
        # 【修改点2】：为了容纳子页面的 1600 宽度，主窗口宽度扩展为 1600 + 侧边栏 240 = 1840
        # 这样子页面就不会被强行挤压，保证原汁原味的视觉比例
        self.setFixedSize(1840, 800) 
        
        # 设置整个主窗口的底色，与子页面相同，实现无缝衔接
        self.setStyleSheet("QMainWindow { background-color: #2F3542; } " + MAIN_STYLE)

        # 核心逻辑：主布局
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.main_layout = QHBoxLayout(central_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # 1. 创建左侧导航栏
        self.create_sidebar()

        # 2. 创建右侧内容区 (Stacked Widget)
        self.content_stack = QStackedWidget()
        self.main_layout.addWidget(self.content_stack)

        # 3. 初始化子页面
        self.page_template = TemplateLearningPage()
        self.page_merge = MultiFileMergePage()
        self.page_manual = self.create_manual_page()

        self.content_stack.addWidget(self.page_template)
        self.content_stack.addWidget(self.page_merge)
        self.content_stack.addWidget(self.page_manual)

        # 默认显示第一页
        self.switch_page(0)

    def create_sidebar(self):
        self.sidebar = QFrame()
        self.sidebar.setObjectName("SideBar")
        sidebar_layout = QVBoxLayout(self.sidebar)
        sidebar_layout.setContentsMargins(0, 0, 0, 0)
        sidebar_layout.setSpacing(0)

        # Logo/标题
        logo = QLabel("DATA MERGE Utility")
        logo.setObjectName("LogoLabel")
        logo.setAlignment(Qt.AlignCenter)
        sidebar_layout.addWidget(logo)

        # 导航按钮
        self.nav_btns = []
        menu_items = [
            ("🧠 模板学习", 0),
            ("🔗 多文件合并", 1),
            ("📖 使用手册", 2)
        ]

        for text, index in menu_items:
            btn = QPushButton(text)
            btn.setObjectName("NavButton")
            btn.setCheckable(True)
            btn.clicked.connect(lambda checked=False, idx=index: self.switch_page(idx))
            sidebar_layout.addWidget(btn)
            self.nav_btns.append(btn)

        sidebar_layout.addStretch()

        # 作者信息
        footer = QLabel("Engine: Python 3.10+\nGUI: PySide6\nAuthor: Juary Wang")
        footer.setObjectName("FooterLabel")
        sidebar_layout.addWidget(footer)

        self.main_layout.addWidget(self.sidebar)

    def switch_page(self, index):
        # 切换堆栈窗口
        self.content_stack.setCurrentIndex(index)
        
        # 更新导航按钮的视觉状态
        for i, btn in enumerate(self.nav_btns):
            is_active = (i == index)
            btn.setChecked(is_active)  # 设置选中状态
            btn.setProperty("active", is_active) # 对应 QSS 中的 [active="true"]
            
            # 强制刷新样式表（这在 PySide 中很重要）
            btn.style().unpolish(btn)
            btn.style().polish(btn)

    def create_manual_page(self):
        """简单生成模块 D 的使用手册界面"""
        page = QWidget()
        # 手动给独立生成的手册页加上和子页面一样的底色和默认字体
        page.setStyleSheet("background-color: #2F3542; font-family: 'Microsoft YaHei', 'Segoe UI';")
        layout = QVBoxLayout(page)
        layout.setContentsMargins(50, 50, 50, 50)
        
        title = QLabel("用户使用手册")
        title.setStyleSheet("font-size: 28px; color: #70A1FF; font-weight: bold; background: transparent;")
        
        content = QLabel(
            "1. 【模板学习】：导入标准文件，配置每一列的清洗规则，保存为 模板.json。\n\n"
            "2. 【多文件合并】：加载已有的模板，选择多个待处理文件。系统会自动校验表头对齐情况。\n\n"
            "3. 【导出】：合并后的数据经过异常检测后，支持导出为 Excel/CSV。\n\n"
            "注意：所有操作均为异步执行，不会阻塞界面。"
        )
        content.setWordWrap(True)
        content.setStyleSheet("font-size: 16px; line-height: 150%; color: #A4B0BE; background: transparent;")
        
        layout.addWidget(title)
        layout.addSpacing(30)
        layout.addWidget(content)
        layout.addStretch()
        return page

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # 【修改点3】：删除了 app.setFont(...)
    # 完全尊重子页面自己的 StyleSheet 设置，不再进行全局霸道干预。
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
