# Data Merge Utility (DMU) - AI Coding Guidelines

## Architecture Overview
DMU is a PySide6-based GUI application for industrial data merging with a two-phase workflow:
- **Template Learning**: Import sample data → DataProfilingEngine profiles columns → Configure cleaning rules → Export JSON template
- **Multi-File Merge**: Load template → Select files → Async merge with rules → Styled Excel export

Key components: `MainApp.py` (navigation), `TemplateLearningPage.py` (profiling UI), `MultiFileMergePage.py` (merge UI), `DataProfilingEngine.py` (core profiling logic).

## Critical Patterns
- **Async Processing**: Use QThread for data operations (profiling/merging) to prevent UI blocking. Connect signals: `result_ready`, `progress`, `log`, `task_finished`.
- **Type Inference Waterfall**: Boolean → Numeric → DateTime → Categorical → String. Use `DataProfilingEngine._profile_column()` as reference.
- **Configuration Persistence**: Store column rules in JSON: `{"headers": [...], "column_settings": {"col": {"null_policy": "...", "duplicate_policy": "..."}}}`
- **UI Styling**: Morandi industrial theme (#2F3542 bg, #70A1FF accent). Fixed sizes: main window 1840x800, pages 1600x800. Use QSS for consistency.
- **Data Handling**: Pandas for processing, subset matching for flexible column alignment. Clean null placeholders: `['N/A', 'NULL', '-', 'nan', 'null', '']`

## Developer Workflows
- **Run App**: `python MainApp.py` (requires PySide6, pandas, openpyxl, rapidfuzz, scipy, dateutil, holidays)
- **Test Profiling**: Run `test.py` to validate `DataProfilingEngine` on sample data with mixed types and anomalies
- **Add New Rule**: Extend `column_settings` in JSON, update `MergeWorker.run()` logic for null/duplicate handling
- **Export Styling**: Use openpyxl with '等线' font, auto-width columns, no borders. Reference `MultiFileMergePage.export_data()`

## Code Conventions
- **Threading**: Subclass QThread, emit signals for UI updates. Avoid direct UI manipulation in worker threads.
- **Error Handling**: Use try/except in threads, emit error logs via signals. Show QMessageBox for user-facing errors.
- **File I/O**: Support both .xlsx/.csv. Use `pd.read_excel/csv()` with `nrows=500` for sampling.
- **Internationalization**: UI text in Chinese, JSON keys in English. Use `ensure_ascii=False` for JSON dumps.
- **Dependencies**: Pin to compatible versions. Test with Python 3.10+.

## Common Pitfalls
- Don't block UI - always use QThread for I/O or computation
- Validate column subset before merging: `set(target_headers).issubset(set(current_headers))`
- Handle fuzzy duplicates in strings using rapidfuzz for suggestions
- Ensure QSS styles apply to custom widgets via `setStyleSheet()`</content>
<parameter name="filePath">d:\Git clone projects\Data-Merge-Utility-DMU-\.github\copilot-instructions.md