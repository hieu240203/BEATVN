import mysql.connector
from connect_database import *
# Kết nối đến database

conn = connect_to_database()

cursor = conn.cursor()

# Tạo bảng HeCongDong (Hệ cộng đồng)
create_he_cong_dong_table = """
CREATE TABLE IF NOT EXISTS HeCongDong (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);
"""
cursor.execute(create_he_cong_dong_table)

# Tạo bảng HeSinhThai (Hệ sinh thái)
create_he_sinh_thai_table = """
CREATE TABLE IF NOT EXISTS HeSinhThai (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);
"""
cursor.execute(create_he_sinh_thai_table)

# Tạo bảng NenTang (Nền tảng)
create_nen_tang_table = """
CREATE TABLE IF NOT EXISTS NenTang (
    IdNenTang INT AUTO_INCREMENT PRIMARY KEY,
    TenNenTang VARCHAR(255) NOT NULL
);
"""
cursor.execute(create_nen_tang_table)

# Tạo bảng Channel
create_channel_table = """
CREATE TABLE IF NOT EXISTS Channel (
    IdChannel INT AUTO_INCREMENT PRIMARY KEY,
    TenChannel VARCHAR(255) NOT NULL,
    NenTangId INT,
    HeCongDongId INT,
    HeSinhThaiId INT,
    LinkChannel VARCHAR(255),
    TotalFollower BIGINT DEFAULT 0,
    TotalLike BIGINT DEFAULT 0, 
    TotalView BIGINT DEFAULT 0, 
    NgaySave DATE,
    FOREIGN KEY (NenTangId) REFERENCES NenTang(IdNenTang),
    FOREIGN KEY (HeCongDongId) REFERENCES HeCongDong(Id),
    FOREIGN KEY (HeSinhThaiId) REFERENCES HeSinhThai(Id)

);
"""
cursor.execute(create_channel_table)

# Tạo bảng Post
create_post_table = """
CREATE TABLE IF NOT EXISTS Post (
    IdPost INT AUTO_INCREMENT PRIMARY KEY,
    ChannelId INT,
    Content TEXT,
    NgayDang DATE,
    SoLike BIGINT DEFAULT 0,
    SoShare BIGINT DEFAULT 0,
    SoCmt BIGINT DEFAULT 0,
    SoLuotXem BIGINT DEFAULT 0,
    LinkBaiViet VARCHAR(255),
    NgaySave DATE,
    FOREIGN KEY (ChannelId) REFERENCES Channel(IdChannel)
);
"""
cursor.execute(create_post_table)

# Lưu các thay đổi
conn.commit()

# Hiển thị danh sách bảng
cursor.execute("SHOW TABLES")
for i in cursor:
    print(i)

# Đóng kết nối
cursor.close()
conn.close()
