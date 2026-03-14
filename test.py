from DataProfilingEngine import DataProfilingEngine
import pandas as pd
import json

if __name__ == "__main__":
    # 模拟构造数据
    data = {
        "用户ID": [101, 102, 103, 104, 105, 106, 107, 9999], # 9999 是潜在数值异常
        "激活状态": ["是", "否", "是", "是", "N/A", "否", "1", "0"], # 布尔型混合
        "消费金额": ["1,200.50", "300", "50%", "-", "2,100", "150", "800", "10,000"], # 数值型带千分位
        "注册日期": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-05-01", # 存在日期断层
                     "2023-05-02", "2023-05-03", "2024-02-10", "2024-02-11"],
        "来源渠道": ["搜索", "搜索 ", "广告", "广告", "推荐", "推荐", "推荐", "搜索"] # 模糊重复项
    }
    
    df_test = pd.DataFrame(data)
    
    # 初始化引擎
    engine = DataProfilingEngine(df_test)
    report = engine.profile_all()
    
    # 打印格式化后的结果
    print(json.dumps(report, indent=4, ensure_ascii=False))