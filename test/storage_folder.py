import os

def get_folder_size(folder_path):
    """Tính tổng dung lượng (bytes) của tất cả file trong folder_path"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):  # kiểm tra file tồn tại
                total_size += os.path.getsize(fp)
    return total_size

def format_size(size_in_bytes):
    """Chuyển bytes thành đơn vị dễ đọc"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024
    return f"{size_in_bytes:.2f} PB"

# Đường dẫn đến folder "data"
base_folder = "data"
subfolders = ["2021", "2022", "2023", "2024"]

# Tính size cho từng folder con
for folder in subfolders:
    path = os.path.join(base_folder, folder)
    size = get_folder_size(path)
    print(f"Dung lượng thư mục {folder}: {format_size(size)}")

# Tính size toàn bộ folder data
total_size = get_folder_size(base_folder)
print(f"Tổng dung lượng thư mục {base_folder}: {format_size(total_size)}")
