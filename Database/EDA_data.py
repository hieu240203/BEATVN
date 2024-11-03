import os 
import numpy as np
import pandas as pd
from datetime import datetime

def read_page_tiktok(path, today):
    # Đọc file Excel với đường dẫn được tạo
    file_path = os.path.join(path, f"page_tiktok_{today}.xlsx")
    data = pd.read_excel(file_path)

    # Dictionary chứa tên cột cần đổi
    column_mapping = {
        "Hệ kênh": "HeKenh", 
        "Tên page": "TenChannel",
        "Link page": "LinkChannel",    
        "Follow": "TotalFollower",       
        "Thích": "TotalLike",               
        "Ngày": "NgaySave"
    }

    # Đổi tên các cột theo mapping
    data = data.rename(columns=column_mapping)

    # Thay đổi tên kênh 'Techlife28' thành 'Theanh28 Review'
    data['TenChannel'] = data['TenChannel'].replace('Techlife28', 'Theanh28 Review')

    # Sắp xếp các cột theo thứ tự cần thiết
    columns_needed = list(column_mapping.values())  # ['HeKenh', 'TenChannel', 'LinkChannel', 'TotalFollower', 'TotalLike', 'NgaySave']
    filtered_data = data[columns_needed]  # Chỉ giữ lại và sắp xếp các cột theo thứ tự

    return filtered_data


# Hàm đọc dữ liệu page_youtube
def read_page_youtube(path, today):
    file_path = os.path.join(path, f"page_youtube_{today}.xlsx")
    data = pd.read_excel(file_path)

    # Đổi tên các cột để khớp với bảng Channel
    data = data.rename(columns={
        "Hệ Kênh" : "HeKenh",
        "Name Channel": "TenChannel",
        "Channel Link": "LinkChannel",
        "Total Sub": "TotalFollower",
        "Total View": "TotalView",
        "Date get data": "NgaySave"
    })

    # Lọc các cột cần thiết
    columns_needed = ["HeKenh","TenChannel", "LinkChannel", "TotalFollower", "TotalView", "NgaySave"]
    filtered_data = data[columns_needed]
    
    return filtered_data

# Hàm đọc dữ liệu page_facebook
def read_page_facebook(path, today):
    file_path = os.path.join(path, f"page_facebook_{today}.xlsx")
    data = pd.read_excel(file_path)

    # Đổi tên các cột để khớp với cấu trúc database
    data = data.rename(columns={
        "Hệ Kênh": "HeKenh",               # Hệ kênh
        "Tên page": "TenChannel",          # Tên kênh
        "Link page": "LinkChannel",        # Đường dẫn kênh
        "Số Follower": "TotalFollower",    # Tổng số người theo dõi
        "Số like": "SoLike",               # Tổng số lượt thích
        "Số Người tham gia": "SoNguoiThamGia",  # Số người tham gia (nếu có)
        "Ngày save" : "NgaySave",
    })

    # Chỉ giữ lại các cột cần thiết cho database
    columns_needed = ["HeKenh", "TenChannel", "LinkChannel", "TotalFollower", "SoLike", "NgaySave"]
    filtered_data = data[columns_needed]

    return filtered_data

# Hàm đọc dữ liệu group_facebook
def read_group_facebook(path, today):
    file_path = os.path.join(path, f"group_facebook_{today}.xlsx")
    data = pd.read_excel(file_path)

    # Đổi tên các cột để khớp với cấu trúc database
    data = data.rename(columns={
        "Hệ Kênh": "HeKenh",               # Hệ kênh
        "Tên page": "TenChannel",          # Tên kênh
        "Link page": "LinkChannel",        # Đường dẫn kênh
        "Số like": "SoLike",               # Tổng số lượt thích
        "Số Người tham gia": "TotalFollower",  # Số người tham gia (nếu có)
        "Ngày save" : "NgaySave",
    })

    # Chỉ giữ lại các cột cần thiết cho database
    columns_needed = ["HeKenh", "TenChannel", "LinkChannel", "SoLike", "TotalFollower", "NgaySave"]
    filtered_data = data[columns_needed]

    return filtered_data

# Hàm đọc dữ liệu tiktok
def read_post_tiktok(path, today):
    # Đọc file Excel với đường dẫn được tạo
    file_path = os.path.join(path, f"post_tiktok_{today}.xlsx")
    data = pd.read_excel(file_path)

    # Đổi tên các cột cho khớp với bảng Post trong MySQL
    data = data.rename(columns={
        "Url": "LinkBaiViet",
        "Content": "Content",
        "Ngày đăng": "NgayDang",
        "Lượt xem": "SoLuotXem",
        "Tym": "SoLike",
        "Share": "SoShare",
        "Save": "Save",  # Không đổi vì đã trùng
        "Tổng số comment": "SoCmt",
        "Tên Page": "TenChannel",
        "Ngày save": "NgaySave"
    })

    # Lọc các cột cần thiết
    columns_needed = [
        "LinkBaiViet", "Content", "NgayDang", "SoLuotXem", "SoLike",
        "SoShare", "Save", "SoCmt", "TenChannel", "NgaySave"
    ]
    filtered_data = data[columns_needed]

    # Thay thế các giá trị NaN trong cột số bằng 0 (hoặc None nếu cần)
    numeric_columns = ["SoLuotXem", "SoLike", "SoShare", "Save", "SoCmt"]
    filtered_data[numeric_columns] = filtered_data[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)

    # Thay thế NaN trong các cột còn lại bằng chuỗi rỗng
    filtered_data = filtered_data.fillna("")

    filtered_data['TenChannel'] = filtered_data['TenChannel'].replace('Techlife28', 'Theanh28 Review')

    return filtered_data

# Hàm đọc dữ liệu post_youtube
def read_post_youtube(path, today):
    file_path = os.path.join(path, f"post_youtube_{today}.xlsx")
    data = pd.read_excel(file_path)

    # Đổi tên các cột để khớp với cấu trúc database
    data = data.rename(columns={
        "Video URL": "LinkBaiViet",       # Đường dẫn bài viết (video)
        "Title": "Content",               # Nội dung tiêu đề video
        "Published Date": "NgayDang",     # Ngày đăng video
        "View Count": "SoLuotXem",        # Số lượt xem
        "Like Count": "SoLike",           # Số lượt thích
        "Comment Count": "SoCmt",         # Số bình luận
        "Channel Title": "TenChannel",    # Tên kênh
        "Ngày save" : "NgaySave"
    })

    # Chỉ giữ lại các cột cần thiết
    columns_needed = ["LinkBaiViet", "Content", "NgayDang", "SoLuotXem", "SoLike", "SoCmt", "TenChannel", "NgaySave"]
    filtered_data = data[columns_needed]

    return filtered_data
