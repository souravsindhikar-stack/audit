import os
import pandas as pd
import hashlib
import json
import re
import shutil
 
# ----------------- Load Config -----------------
CONFIG_PATH = r"D:\Salesforce\UAT_Migration\Case\Case Arrow Verical RecordType\config.json"
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)
 
input_file = config["input_file"]
cleaned_file = config["cleaned_file"]
split_dir = config["split_dir"]
report_file = config["report_file"]
max_rows = config.get("max_rows", 100000)
max_size_mb = config.get("max_size_mb", 200)
required_columns = config.get("required_columns", [])
rich_text_columns = config.get("rich_text_columns", [])
 
# ----------------- Regex Patterns -----------------
ISO_TZ_REGEX = re.compile(r'^\s*\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})?\s*$')
 
DATE_PATTERNS = [
    re.compile(r'^\s*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?\s*$'),
    re.compile(r'^\s*\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?\s*$'),
    re.compile(r'^\s*\d{1,2}[-.]\w{3}[-.]\d{2,4}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?\s*$', re.I)
]
 
TIME_REGEX = re.compile(r'(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?')
 
def _clamp_2d(val_str: str, maxv: int) -> str:
    try:
        v = int(val_str)
    except Exception:
        v = 0
    v = max(0, min(maxv, v))
    return f"{v:02d}"
 
def _fix_invalid_time(text: str) -> str:
    def _repl(m: re.Match) -> str:
        h = _clamp_2d(m.group(1), 23)
        mi = _clamp_2d(m.group(2), 59)
        s = m.group(3)
        if s is not None:
            s = _clamp_2d(s, 59)
            return f"{h}:{mi}:{s}"
        else:
            return f"{h}:{mi}"
    return TIME_REGEX.sub(_repl, str(text), count=1)
 
def _looks_like_date(s: str) -> bool:
    if ISO_TZ_REGEX.match(s):
        return True
    return any(pat.match(s) for pat in DATE_PATTERNS)
 
def normalize_cell(val):
    """Normalize only if it looks like a valid date/time string."""
    if isinstance(val, (list, pd.Series)):
        if len(val) == 0:
            return val
        val = val.iloc[0] if isinstance(val, pd.Series) else val[0]
 
    if pd.isna(val):
        return val
 
    s = str(val).strip()
    if s == "":
        return s
    if not _looks_like_date(s):
        return val
    if ISO_TZ_REGEX.match(s):
        return s
 
    s_fixed = _fix_invalid_time(s)
 
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = pd.to_datetime(s_fixed, format=fmt, errors='raise')
            has_time = any(x in fmt for x in ["%H", "%M", "%S"])
            return dt.strftime("%Y-%m-%d %H:%M:%S") if has_time else dt.strftime("%Y-%m-%d")
        except:
            continue
 
    dt = pd.to_datetime(s_fixed, errors="coerce")
    if pd.isna(dt):
        return val
    has_time = (":" in s_fixed) or (dt.hour or dt.minute or dt.second)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if has_time else dt.strftime("%Y-%m-%d")
 
# ----------------- Split CSV -----------------
def split_csv(df, base_name, output_dir, max_rows=10000, max_size_mb=10):
    rows = len(df)
    part = 1
    start = 0
    max_allowed_size = max_size_mb * 0.99  # Keep under 9.9 MB for a 10 MB limit
 
    print(f"File Size: {df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB | Total Rows: {rows}")
 
    while start < rows:
        end = min(start + max_rows, rows)
        chunk = df.iloc[start:end]
 
        output_file = os.path.join(output_dir, f"{base_name}_part{part}.csv")
        chunk.to_csv(output_file, index=False, encoding="utf-8")
 
        size_mb = os.path.getsize(output_file) / (1024 * 1024)
 
        # Reduce rows until file size < max_allowed_size
        while size_mb > max_allowed_size and (end - start) > 1:
            overshoot_factor = size_mb / max_allowed_size
            new_chunk_size = int((end - start) / overshoot_factor)
            if new_chunk_size < 1:
                new_chunk_size = 1
 
            end = start + new_chunk_size
            chunk = df.iloc[start:end]
            chunk.to_csv(output_file, index=False, encoding="utf-8")
            size_mb = os.path.getsize(output_file) / (1024 * 1024)
 
        print(f"✅ Saved {output_file} ({size_mb:.2f} MB, {len(chunk)} rows)")
 
        start = end
        part += 1
 
# ----------------- Validation -----------------
def hash_row(row):
    row_str = "|".join(str(v) if pd.notna(v) else "NULL" for v in row)
    return hashlib.md5(row_str.encode()).hexdigest()
 
def validate_data(cleaned_file, split_dir, report_file):
    report_lines = []
 
    def log(msg):
        print(msg)
        report_lines.append(msg)
 
    log("\n===== VALIDATION REPORT =====")
    df_original = pd.read_csv(cleaned_file, dtype=str, encoding="utf-8")
    orig_rows, orig_cols = df_original.shape
    log(f"{os.path.basename(cleaned_file)}: {orig_rows} rows, {orig_cols} cols, {os.path.getsize(cleaned_file)/(1024*1024):.2f} MB")
    orig_hashes = set(df_original.apply(hash_row, axis=1))
 
    split_files = [f for f in os.listdir(split_dir) if f.endswith(".csv")]
    combined_rows, combined_hashes = 0, set()
    for f in split_files:
        path = os.path.join(split_dir, f)
        df_split = pd.read_csv(path, dtype=str, encoding="utf-8")
        rows, cols = df_split.shape
        combined_rows += rows
        if list(df_split.columns) != list(df_original.columns):
            log(f"❌ Column mismatch in {f} (expected {len(df_original.columns)} cols, found {len(df_split.columns)} cols)")
        combined_hashes.update(df_split.apply(hash_row, axis=1))
        log(f"{f}: {rows} rows, {cols} cols, {os.path.getsize(path)/(1024*1024):.2f} MB")
 
    log(f"\nOriginal rows: {orig_rows} | Split total rows: {combined_rows}")
    log("✅ Row count matches." if orig_rows == combined_rows else "❌ Row count mismatch!")
    log("✅ Data integrity passed." if orig_hashes == combined_hashes else "❌ Data mismatch detected!")
    log("===== VALIDATION COMPLETED =====")
 
    with open(report_file, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"\n📄 Validation report saved at: {report_file}")
 
# ----------------- Main -----------------
def main():
    print("===== DATA MIGRATION STARTED =====")
 
    try:
        df = pd.read_csv(input_file, dtype=str, on_bad_lines='skip', engine='python', encoding='utf-8')
    except UnicodeDecodeError:
        print("⚠ UTF-8 failed, trying ISO-8859-1 to preserve all data")
        df = pd.read_csv(input_file, dtype=str, on_bad_lines='skip', engine='python', encoding='ISO-8859-1')
 
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return
 
    # ✅ Keep only required columns that exist in source
    if required_columns:
        existing_cols = [c for c in required_columns if c in df.columns]
        df = df[existing_cols]
 
    # ❌ Remove completely empty columns
    df = df.dropna(axis=1, how='all')  # remove columns where all values are NaN
    df = df.loc[:, ~(df.apply(lambda x: x.astype(str).str.strip().eq('').all()))]
 
    # Normalize date-like fields
    for col in df.columns:
        df[col] = df[col].apply(normalize_cell)
 
    # Wrap Rich Text fields in quotes and escape internal quotes
    for col in rich_text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('"', '""', regex=False)
            df[col] = '"' + df[col] + '"'
 
    # Rename "Id" column to "Legacy_SF_Record_ID__c" if present
    if "Id" in df.columns:
        df.rename(columns={"Id": "Legacy_SF_Record_ID__c"}, inplace=True)
    else:
        print("⚠ 'Id' column not found...check Id column.")

    # Save cleaned file
    df.to_csv(cleaned_file, index=False, encoding="utf-8")
    print(f"✅ Cleaned file saved at: {cleaned_file}")
 
    # Clean and recreate split_dir
    if os.path.exists(split_dir):
        shutil.rmtree(split_dir)
    os.makedirs(split_dir, exist_ok=True)
 
    # Split
    base_name = os.path.splitext(os.path.basename(cleaned_file))[0]
    split_csv(df, base_name, split_dir, max_rows=max_rows, max_size_mb=max_size_mb)
 
    # Validate
    validate_data(cleaned_file, split_dir, report_file)
    print("===== DATA MIGRATION COMPLETED =====")
 
if __name__ == "__main__":
    main()