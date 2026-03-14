import pandas as pd
import numpy as np
from dateutil.parser import parse
import holidays
from rapidfuzz import process
from scipy import stats
import json

class DataProfilingEngine:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.report = {}
        # 预定义空值占位符
        self.null_placeholders = ['N/A', 'NULL', '-', 'nan', 'null', '']
        # 节假日查找器 (默认中国)
        self.cn_holidays = holidays.CN()

    def profile_all(self):
        """执行全量探查主入口"""
        for col in self.df.columns:
            self.report[col] = self._profile_column(self.df[col])
        return self.report

    def _profile_column(self, series: pd.Series) -> dict:
        """单列探测逻辑 - 兼容性版本"""
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
                
                # Level 5: String/Object
                else:
                    col_report["inferred_type"] = "String"
                    col_report.update(self._profile_string(cleaned_series, unique_values))

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
            return pd.to_datetime(series, errors='coerce')
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

    def _profile_string(self, series, unique_vals) -> dict:
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