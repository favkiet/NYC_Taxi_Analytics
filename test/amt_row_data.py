import os
import pandas as pd

def count_rows_in_parquet_files(base_dir="data"):
    total_rows = 0
    file_count = 0

    # Duyệt tất cả thư mục con và file .parquet
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_parquet(file_path, engine="pyarrow")
                    rows = len(df)
                    total_rows += rows
                    file_count += 1
                    print(f"📄 {file} - {rows} rows")
                except Exception as e:
                    print(f"⚠️ Lỗi khi đọc {file_path}: {e}")

    print(f"\n✅ Tổng số file: {file_count}")
    print(f"📊 Tổng số dòng dữ liệu: {total_rows:,}")
    return total_rows

# Gọi hàm
count_rows_in_parquet_files("data")
# Gọi hàm với đường dẫn mặc định