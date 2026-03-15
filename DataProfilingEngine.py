import os
import pandas as pd
import numpy as np
from dateutil.parser import parse
import holidays
from rapidfuzz import process, fuzz
from scipy import stats
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

class DataProfilingEngine:
    def __init__(self, df: pd.DataFrame, max_workers: int = None):
        self.df = df.copy()
        self.report = {}
        # 预定义空值占位符
        self.null_placeholders = ['N/A', 'NULL', '-', 'nan', 'null', '']
        # 节假日查找器 (默认中国)
        self.cn_holidays = holidays.CN()
        # 并行处理的工作线程数，默认使用 CPU 核心数
        self.max_workers = max_workers or min(8, (os.cpu_count() or 1) + 4)

    def profile_all(self):
        """执行全量探查主入口 - 使用并行处理加速"""
        columns = list(self.df.columns)
        
        # 使用 ThreadPoolExecutor 并行处理各列
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有列的分析任务
            future_to_col = {
                executor.submit(self._profile_column, self.df[col]): col 
                for col in columns
            }
            
            # 收集结果
            for future in as_completed(future_to_col):
                col = future_to_col[future]
                try:
                    self.report[col] = future.result()
                except Exception as e:
                    print(f"列 {col} 分析失败: {e}")
                    self.report[col] = {"error": str(e)}
        
        # 按原始列顺序返回
        ordered_report = {col: self.report[col] for col in columns}
        return ordered_report

    def _profile_column(self, series: pd.Series) -> dict:
        """单列探测逻辑 - 优化版本"""
        total_count = len(series)
        # 将占位符统一替换为 NaN
        cleaned_series = series.replace(self.null_placeholders, np.nan)
        null_count = cleaned_series.isnull().sum()
        unique_values = cleaned_series.dropna().unique()
        
        col_report = {
            "stats": {
                "total": total_count,
                "null_count": int(null_count),
                "null_ratio": f"{(null_count / total_count):.2%}",
                "unique_count": len(unique_values)
            },
            "inferred_type": "Unknown",
            "anomalies": []
        }

        # --- 瀑布流类型识别引擎 (兼容性改写) ---
        
        # Level 1: Boolean
        if self._is_boolean(unique_values):
            col_report["inferred_type"] = "Boolean"
        
        else:
            # Level 2: Numeric
            num_data = self._try_parse_numeric(cleaned_series)
            if num_data is not None:
                col_report["inferred_type"] = "Numeric"
                col_report["details"] = self._profile_numeric(num_data)
            
            else:
                # Level 3: DateTime
                dt_data = self._try_parse_datetime(cleaned_series)
                if dt_data is not None:
                    col_report["inferred_type"] = "DateTime"
                    col_report["details"] = self._profile_datetime(dt_data)
                
                # Level 4: Categorical (根据唯一值占比判定)
                elif len(unique_values) / total_count < 0.05:
                    col_report["inferred_type"] = "Categorical"
                    col_report["details"] = self._profile_categorical(cleaned_series, unique_values)
                
                # Level 5: String/Object - 使用优化后的方法
                else:
                    col_report["inferred_type"] = "String"
                    col_report.update(self._profile_string_optimized(cleaned_series, unique_values))

        return col_report

    # --- 类型识别辅助方法 ---

    def _is_boolean(self, unique_vals) -> bool:
        bool_set = {True, False, 1, 0, '1', '0', '是', '否', 'yes', 'no', 'true', 'false'}
        return set(str(v).lower() for v in unique_vals).issubset(bool_set)

    def _try_parse_numeric(self, series):
        try:
            # 清洗千分位和百分号
            s = series.astype(str).str.replace(r'[,%]', '', regex=True)
            return pd.to_numeric(s, errors='raise')
        except Exception:
            return None

    def _try_parse_datetime(self, series):
        try:
            # 抽样快速测试
            sample = series.dropna().head(5).astype(str)
            if sample.empty: return None
            for s in sample:
                parse(s)
            return pd.to_datetime(series, errors='coerce', dtype=str)
        except Exception:
            return None

    # --- 健康度报告生成器 (核心逻辑保持不变) ---

    def _profile_numeric(self, data: pd.Series) -> dict:
        mu = data.mean()
        sigma = data.std()
        lower, upper = mu - 3 * sigma, mu + 3 * sigma
        outliers = data[(data < lower) | (data > upper)]
        
        return {
            "min": float(data.min()),
            "max": float(data.max()),
            "mean": float(mu),
            "std": float(sigma),
            "outliers_count": len(outliers)
        }

    def _profile_datetime(self, data: pd.Series) -> dict:
        valid_dates = data.dropna().sort_values()
        if valid_dates.empty: return {}
        holiday_count = sum(1 for d in valid_dates if d in self.cn_holidays)
        return {
            "range": [valid_dates.min().strftime('%Y-%m-%d'), valid_dates.max().strftime('%Y-%m-%d')],
            "holiday_ratio": f"{(holiday_count / len(valid_dates)):.2%}"
        }

    def _profile_string_optimized(self, series, unique_vals) -> dict:
        """
        优化版的字符串分析 - 使用更快的模糊匹配算法
        优化点：
        1. 限制采样数量（只取前50个）
        2. 使用更高效的匹配算法
        3. 减少比较次数
        """
        suggestions = []
        unique_count = len(unique_vals)
        
        # 只有在唯一值数量合理时才进行分析
        if 1 < unique_count <= 500:
            # 采样限制为50个，避免 O(n²) 爆炸
            sample_size = min(50, unique_count)
            sample_vals = [str(x).strip() for x in unique_vals[:sample_size]]
            
            # 使用更快的 cdist 进行批量匹配
            if len(sample_vals) > 1:
                try:
                    # 使用 rapidfuzz 的 cdist 进行批量计算，比逐个 extract 快很多
                    from rapidfuzz.distance import cdist
                    
                    # 计算所有对的相似度矩阵
                    scores = cdist(sample_vals, sample_vals, scorer=fuzz.ratio, workers=-1)
                    
                    # 找出高相似度对（但排除自己跟自己的比较）
                    for i in range(len(sample_vals)):
                        for j in range(i + 1, len(sample_vals)):
                            if scores[i][j] >= 90:  # 相似度阈值
                                suggestions.append(f"建议合并: '{sample_vals[i]}' 与 '{sample_vals[j]}'")
                                if len(suggestions) >= 3:
                                    break
                        if len(suggestions) >= 3:
                            break
                except Exception as e:
                    # 如果 cdist 失败，回退到原来的方法（但限制规模）
                    suggestions = self._profile_string_fallback(sample_vals)
        elif unique_count > 500:
            # 唯一值太多，跳过模糊匹配（这通常是自由文本字段）
            pass
            
        return {"fuzzy_suggestions": suggestions[:3]}

    def _profile_string_fallback(self, sample_vals) -> list:
        """回退方案：使用原方法但限制规模"""
        suggestions = []
        for i, val in enumerate(sample_vals):
            if len(suggestions) >= 3:
                break
            matches = process.extract(val, sample_vals[i+1:], score_cutoff=90, limit=1)
            for m in matches:
                suggestions.append(f"建议合并: '{val}' 与 '{m[0]}'")
        return suggestions

    def _profile_string(self, series, unique_vals) -> dict:
        """原版字符串分析 - 保留兼容性"""
        suggestions = []
        if 1 < len(unique_vals) < 1000:
            sample_vals = [str(x).strip() for x in unique_vals[:100]]
            for i, val in enumerate(sample_vals):
                matches = process.extract(val, sample_vals[i+1:], score_cutoff=90, limit=1)
                for m in matches:
                    suggestions.append(f"建议合并: '{val}' 与 '{m[0]}'")
        return {"fuzzy_suggestions": suggestions[:3]}

    def _profile_categorical(self, series, unique_vals) -> dict:
        return {"top_categories": series.value_counts().head(3).to_dict()}
