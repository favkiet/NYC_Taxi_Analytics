import os
import requests

# Base URL nguồn dữ liệu (file .parquet)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Cấu hình
TAXI_TYPES = ["yellow", "green"]
YEARS = [2021, 2022]
MONTHS = range(1, 13)

def download_file(url, save_path):
    """Tải file từ URL và lưu vào save_path"""
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Đã tải: {save_path}")
    except Exception as e:
        print(f"❌ Lỗi khi tải {url}: {e}")

for year in YEARS:
    year_dir = os.path.join("data", str(year))
    os.makedirs(year_dir, exist_ok=True)

    for month in MONTHS:
        month_str = f"{month:02d}"
        for taxi_type in TAXI_TYPES:
            file_name = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
            url = f"{BASE_URL}/{file_name}"
            save_path = os.path.join(year_dir, file_name)

            if not os.path.exists(save_path):
                print(f"⬇️  Đang tải: {file_name} → {save_path}")
                download_file(url, save_path)
            else:
                print(f"✔️  Đã tồn tại: {file_name}")
