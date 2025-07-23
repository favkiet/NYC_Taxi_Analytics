import pandas as pd
import requests
import os
from datetime import datetime
import time

def download_file(url, filename):
    """Download file từ URL với progress tracking"""
    print(f"Đang tải {filename}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"\rTiến trình: {percent:.1f}%", end="", flush=True)
        
        print(f"\n✓ Hoàn thành tải {filename}")
        return True
    except Exception as e:
        print(f"\n✗ Lỗi khi tải {filename}: {e}")
        return False

def generate_urls(taxi_type, years):
    """Tạo danh sách URLs cho taxi type và years"""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    urls = []
    
    for year in years:
        for month in range(1, 13):
            if taxi_type == "yellow":
                filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
            else:  # green
                filename = f"green_tripdata_{year}-{month:02d}.parquet"
            
            url = f"{base_url}/{filename}"
            urls.append((url, filename))
    
    return urls

def standardize_column_names(df):
    """Chuẩn hóa tên cột để đảm bảo consistency"""
    # Dictionary mapping các tên cột khác nhau về tên chuẩn
    column_mappings = {
        # Airport fee variations
        'Airport_fee': 'airport_fee',
        'Aiport_fee': 'airport_fee',  # Có thể có typo
        
        # Congestion surcharge variations
        'Congestion_Surcharge': 'congestion_surcharge',
        'congestion_surcharge': 'congestion_surcharge',
        
        # Improvement surcharge variations
        'Improvement_surcharge': 'improvement_surcharge',
        'improvement_surcharge': 'improvement_surcharge',
        
        # Thêm các mappings khác nếu cần
    }
    
    # Rename columns nếu tìm thấy
    columns_renamed = []
    for old_name, new_name in column_mappings.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
            columns_renamed.append(f"{old_name} -> {new_name}")
    
    if columns_renamed:
        print(f"    Đã chuẩn hóa cột: {', '.join(columns_renamed)}")
    
    return df

def download_and_save_taxi_data(taxi_type, years):
    """Tải và lưu dữ liệu taxi theo từng file riêng biệt vào folder theo năm"""
    print(f"\n=== Bắt đầu xử lý {taxi_type.upper()} taxi data ===")
    
    # Tạo folders cho từng năm
    for year in years:
        folder_path = f"data/{year}"
        os.makedirs(folder_path, exist_ok=True)
        print(f"✓ Tạo folder: {folder_path}")
    
    # Tạo URLs
    urls = generate_urls(taxi_type, years)
    
    # Download và save từng file
    successful_downloads = 0
    total_files = len(urls)
    
    for i, (url, original_filename) in enumerate(urls, 1):
        # Parse năm từ filename
        if taxi_type == "yellow":
            # yellow_tripdata_2023-01.parquet -> 2023
            year = original_filename.split('_')[2][:4]
        else:
            # green_tripdata_2023-01.parquet -> 2023
            year = original_filename.split('_')[2][:4]
        
        # Đường dẫn cuối cùng
        final_path = os.path.join("data", year, original_filename)
        
        print(f"\n[{i}/{total_files}] Đang xử lý {original_filename}...")
        
        # Kiểm tra file đã tồn tại chưa
        if os.path.exists(final_path):
            print(f"  ⚠️  File đã tồn tại: {final_path}")
            choice = input("  Ghi đè? (y/n): ").lower().strip()
            if choice != 'y':
                print("  ⏭️  Bỏ qua file này")
                continue
        
        # Download về temp file
        temp_filename = f"temp_{original_filename}"
        if download_file(url, temp_filename):
            try:
                # Đọc và chuẩn hóa
                print("  📋 Đang chuẩn hóa cột...")
                df = pd.read_parquet(temp_filename)
                original_columns = len(df.columns)
                
                # Chuẩn hóa tên cột
                df = standardize_column_names(df)
                
                # Lưu vào folder đích
                df.to_parquet(final_path, index=False)
                
                # Xóa temp file
                os.remove(temp_filename)
                
                print(f"  ✅ Lưu thành công: {final_path}")
                print(f"     📊 {len(df):,} rows, {len(df.columns)} columns")
                
                successful_downloads += 1
                
            except Exception as e:
                print(f"  ❌ Lỗi xử lý file: {e}")
                # Cleanup temp file
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
        
        # Delay để tránh spam server
        if i < total_files:
            time.sleep(0.5)
    
    print(f"\n📈 Kết quả {taxi_type.upper()} taxi:")
    print(f"   Thành công: {successful_downloads}/{total_files} files")
    
    return successful_downloads > 0

def inspect_schema_differences(file_paths, taxi_type):
    """Kiểm tra sự khác biệt trong schema giữa các files"""
    print(f"\n🔍 Kiểm tra schema cho {taxi_type} taxi files...")
    
    schemas = {}
    for i, file_path in enumerate(file_paths[:3]):  # Chỉ check 3 files đầu để tránh quá nhiều output
        try:
            df = pd.read_parquet(file_path)
            filename = os.path.basename(file_path)
            schemas[filename] = set(df.columns)
            print(f"  {filename}: {len(df.columns)} columns")
        except Exception as e:
            print(f"  Lỗi đọc {file_path}: {e}")
    
    if len(schemas) > 1:
        # So sánh schemas
        all_columns = set().union(*schemas.values())
        base_schema = list(schemas.values())[0]
        
        print(f"\nSo sánh schema (base: {list(schemas.keys())[0]}):")
        for filename, columns in list(schemas.items())[1:]:
            missing = base_schema - columns
            extra = columns - base_schema
            
            if missing or extra:
                print(f"  {filename}:")
                if missing:
                    print(f"    Thiếu: {sorted(missing)}")
                if extra:
                    print(f"    Thừa: {sorted(extra)}")
            else:
                print(f"  {filename}: ✓ Schema khớp")

def list_downloaded_files():
    """Liệt kê các file đã download"""
    print("\n📁 DANH SÁCH FILES ĐÃ TẢI:")
    print("=" * 50)
    
    total_files = 0
    total_size = 0
    
    for year in [2023, 2024]:
        folder_path = f"data/{year}"
        if os.path.exists(folder_path):
            files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
            if files:
                print(f"\n📂 {year} ({len(files)} files):")
                year_size = 0
                for file in sorted(files):
                    file_path = os.path.join(folder_path, file)
                    size_mb = os.path.getsize(file_path) / (1024*1024)
                    year_size += size_mb
                    total_size += size_mb
                    print(f"   📄 {file} ({size_mb:.1f} MB)")
                    total_files += 1
                print(f"   💾 Tổng {year}: {year_size:.1f} MB")
        else:
            print(f"\n📂 {year}: Chưa có folder")
    
    print(f"\n🎯 TỔNG KẾT:")
    print(f"   📊 Tổng files: {total_files}")
    print(f"   💾 Tổng dung lượng: {total_size:.1f} MB")

def main():
    """Hàm chính để tải Yellow và Green taxi data vào folders riêng biệt"""
    years = [2023, 2024]
    
    print("NYC Taxi Data Downloader - Individual Files")
    print("=" * 60)
    print(f"Sẽ tải dữ liệu cho các năm: {years}")
    print("Taxi types: Yellow và Green")
    print("Lưu riêng lẻ vào: data/2023/ và data/2024/")
    print("Tự động chuẩn hóa tên cột: Airport_fee -> airport_fee")
    
    # Tạo folder data chính
    os.makedirs("data", exist_ok=True)
    
    # Tải Yellow taxi data
    success_yellow = download_and_save_taxi_data(
        taxi_type="yellow", 
        years=years
    )
    
    # Tải Green taxi data  
    success_green = download_and_save_taxi_data(
        taxi_type="green", 
        years=years
    )
    
    print("\n" + "=" * 60)
    print("KẾT QUẢ DOWNLOAD:")
    print(f"Yellow taxi: {'✅ Thành công' if success_yellow else '❌ Thất bại'}")
    print(f"Green taxi: {'✅ Thành công' if success_green else '❌ Thất bại'}")
    
def check_schemas():
    """Kiểm tra schema của các files đã download"""
    print("\n🔍 KIỂM TRA SCHEMA:")
    print("=" * 50)
    
    for year in [2023, 2024]:
        folder_path = f"data/{year}"
        if os.path.exists(folder_path):
            files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
            if files:
                print(f"\n📂 {year}:")
                
                # Lấy 2 files đầu để so sánh schema
                sample_files = sorted(files)[:2]
                schemas = {}
                
                for file in sample_files:
                    try:
                        file_path = os.path.join(folder_path, file)
                        df = pd.read_parquet(file_path)
                        schemas[file] = set(df.columns)
                        
                        # Check airport fee column
                        airport_cols = [col for col in df.columns if 'airport' in col.lower()]
                        print(f"   📄 {file}: {len(df.columns)} columns, {len(df):,} rows")
                        if airport_cols:
                            print(f"      🛫 Airport columns: {airport_cols}")
                        
                    except Exception as e:
                        print(f"   ❌ Lỗi đọc {file}: {e}")
                
                # So sánh schema nếu có nhiều file
                if len(schemas) > 1:
                    files_list = list(schemas.keys())
                    schema1 = schemas[files_list[0]]
                    schema2 = schemas[files_list[1]]
                    
                    if schema1 == schema2:
                        print(f"   ✅ Schema nhất quán giữa {files_list[0]} và {files_list[1]}")
                    else:
                        diff1 = schema1 - schema2
                        diff2 = schema2 - schema1
                        print(f"   ⚠️  Schema khác nhau:")
                        if diff1:
                            print(f"      {files_list[0]} có thêm: {sorted(diff1)}")
                        if diff2:
                            print(f"      {files_list[1]} có thêm: {sorted(diff2)}")

    # Liệt kê files đã tải
    list_downloaded_files()
    
    # Kiểm tra schema
    check_schemas()

if __name__ == "__main__":
    main()