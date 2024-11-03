# Hàm này thực hiện mục đích thêm dữ liệu và trong database

import pandas as pd
import mysql.connector
from datetime import datetime

# hàm thêm dữ liệu chuẩn vào bảng nền tảng
def insert_nen_tang_data(conn):
    '''
        Hàm này thực hiện việc thêm dữ liệu vào bảng nền tảng. 
        Hàm này sẽ có 5 giá trị mặc định là Tiktok, Youtube, Fanpage, Group, Instagram. 
        Nếu muốn thêm hoặc xóa các giá trị thì sửa lại ở giá trị nen_tang_data. 
    '''
    cursor = conn.cursor()

    # Xóa dữ liệu trong bảng NenTang và reset AUTO_INCREMENT về 1
    try:
        cursor.execute("DELETE FROM NenTang;")  # Xóa tất cả dữ liệu
        cursor.execute("ALTER TABLE NenTang AUTO_INCREMENT = 1;")  # Reset AUTO_INCREMENT về 1
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Lỗi khi xóa và reset bảng: {err}")
        conn.rollback()
    
    # Dữ liệu nền tảng mới cần chèn 
    # Khi muốn thêm hoặc xóa, sửa dữ liệu thì sửa trực tiếp ở đây. 
    nen_tang_data = [
        ("TikTok",),
        ("YouTube",),
        ("Fanpage",),
        ("Group",),
        ("Instagram",)
    ]

    insert_query = """
    INSERT INTO NenTang (TenNenTang)
    VALUES (%s);
    """

    select_query = "SELECT TenNenTang FROM NenTang WHERE TenNenTang = %s"

    try:
        added_count = 0  # Đếm số lượng bản ghi được thêm

        for nen_tang in nen_tang_data:
            cursor.execute(select_query, nen_tang)  # Kiểm tra sự tồn tại

            # Nếu không tìm thấy bản ghi, thêm vào database
            if cursor.fetchone() is None:
                cursor.execute(insert_query, nen_tang)
                added_count += 1

        conn.commit()  # Xác nhận thay đổi
    except mysql.connector.Error as err:
        print(f"Lỗi khi chèn dữ liệu: {err}")
        conn.rollback()

# Hàm thêm dữ liệu chuẩn vào bảng cộng đồng
def insert_he_cong_dong_data(conn):
    '''
        Hàm này thêm dữ liệu mặc định vào hệ cộng đồng. 
        Khi muốn thêm dữ liệu vào hệ cộng đồng thì cần thêm vào he_cong_dong_data.
    '''
    cursor = conn.cursor()


    he_cong_dong_data = [
        ("VTC Media",), ("Sunny",), ("Theanh28",), ("GDL",),
        ("Halo Travel",), ("360 group",), ("KSC Group",),
        ("Box studio",), ("Weibo",), ("Adsota",), ("UBG",),
        ("ANT GROUP",), ("NGOA",), ("ORGANE AGENCY",), 
        ("BEAT Network",), ("Anh da đen",), ("VKR",), 
        ("Trường người ta",), ("Suzu Studio",)
    ]

    insert_query = """
    INSERT INTO HeCongDong (Name)
    SELECT %s
    WHERE NOT EXISTS (
        SELECT 1 FROM HeCongDong WHERE Name = %s
    );
    """

    try:
        for data in he_cong_dong_data:
            cursor.execute(insert_query, (data[0], data[0]))  # Truyền dữ liệu vào câu lệnh SQL
        conn.commit()
        print(f"Đã chèn {cursor.rowcount} hệ cộng đồng mới vào bảng HeCongDong.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()

# Hàm thêm dữ liệu chuẩn vào bảng hệ sinh thái 
def insert_he_sinh_thai_data(conn):
    
    '''
        Hàm này thêm dữ liệu trực tiếp vào hệ sinh thái. 
        Muốn thêm dữ liệu mặc định vào hệ sinh thái thì cần thêm vài list he_sinh_thai_data
    '''
    cursor = conn.cursor()

    he_sinh_thai_data = [
        ("Admicro",), ("Yeah1",), ("Yan",)
    ]

    insert_query = """
    INSERT INTO HeSinhThai (Name)
    SELECT %s
    WHERE NOT EXISTS (
        SELECT 1 FROM HeSinhThai WHERE Name = %s
    );
    """

    try:
        for data in he_sinh_thai_data:
            cursor.execute(insert_query, (data[0], data[0]))  # Truyền dữ liệu vào câu lệnh SQL
        conn.commit()
        print(f"Đã chèn {cursor.rowcount} hệ sinh thái mới vào bảng HeSinhThai.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()

def convert_date_format(date_value):
    """Chuyển đổi ngày từ MM-DD-YYYY sang YYYY-MM-DD hoặc giữ nguyên nếu là Timestamp."""
    try:
        # Nếu là Timestamp, chuyển thành chuỗi định dạng 'YYYY-MM-DD'
        if isinstance(date_value, pd.Timestamp):
            return date_value.strftime('%Y-%m-%d')

        # Nếu là chuỗi, chuyển từ định dạng MM-DD-YYYY sang YYYY-MM-DD
        if isinstance(date_value, str) and date_value.strip() != "":
            return datetime.strptime(date_value, '%m-%d-%Y').strftime('%Y-%m-%d')

        # Nếu giá trị rỗng hoặc không hợp lệ, trả về None
        return None
    except ValueError as e:
        print(f"Lỗi chuyển đổi ngày: {e} - Giá trị: {date_value}")
        return None  # Trả về None nếu gặp lỗi


# hàm thêm dữ liệu youtube vào bảng channel. 
def insert_data_page_youtube(conn, data):
    '''
        Hàm này thực hiện mục đích thêm dữ liệu bảo bảng channel, dữ liệu được lưu lại thuộc nền tảng youtube.
    '''
    cursor = conn.cursor()

    try:
        # Lấy id của nền tảng YouTube từ bảng NenTang
        cursor.execute("SELECT IdNenTang FROM NenTang WHERE TenNenTang = 'YouTube';")
        nen_tang_id = cursor.fetchone()[0]

        # Câu lệnh lấy ID hệ cộng đồng và hệ sinh thái
        he_cong_dong_query = "SELECT Id FROM HeCongDong WHERE Name = %s"
        he_sinh_thai_query = "SELECT Id FROM HeSinhThai WHERE Name = %s"

        for _, row in data.iterrows():
            # Lấy ID hệ cộng đồng và hệ sinh thái dựa vào tên hệ kênh
            cursor.execute(he_cong_dong_query, (row["HeKenh"],))
            he_cong_dong_id = cursor.fetchone()

            cursor.execute(he_sinh_thai_query, (row["HeKenh"],))
            he_sinh_thai_id = cursor.fetchone()

            # Nếu không tìm thấy, đặt giá trị là None
            he_cong_dong_id = he_cong_dong_id[0] if he_cong_dong_id else None
            he_sinh_thai_id = he_sinh_thai_id[0] if he_sinh_thai_id else None

            # Chuyển đổi định dạng ngày
            ngay_save = convert_date_format(row["NgaySave"])

            # Thêm mới dữ liệu vào bảng Channel
            insert_query = """
            INSERT INTO Channel (
                TenChannel, LinkChannel, TotalFollower, TotalView, NgaySave, 
                NenTangId, HeCongDongId, HeSinhThaiId
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (
                row["TenChannel"], row["LinkChannel"], row["TotalFollower"],
                row["TotalView"], ngay_save, nen_tang_id, 
                he_cong_dong_id, he_sinh_thai_id
            ))

        conn.commit()  # Lưu thay đổi
        print(f"Đã thêm {cursor.rowcount} bản ghi vào bảng Channel.")

    except mysql.connector.Error as err:
        print(f"Lỗi khi thêm dữ liệu: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi

def insert_data_page_facebook(conn, data):
    '''
        Hàm này thực hiện mục đích thêm dữ liệu vào bảng channel, dữ liệu được lưu lại thuộc nền tảng page  facebook. 
    '''
    cursor = conn.cursor()

    try:
        # Lấy IdNenTang của Fanpage từ bảng NenTang
        cursor.execute("SELECT IdNenTang FROM NenTang WHERE TenNenTang = 'Fanpage';")
        nen_tang_id = cursor.fetchone()[0]

        # Câu lệnh lấy ID hệ cộng đồng và hệ sinh thái
        he_cong_dong_query = "SELECT Id FROM HeCongDong WHERE Name = %s"
        he_sinh_thai_query = "SELECT Id FROM HeSinhThai WHERE Name = %s"

        # Xử lý NaN trong DataFrame bằng giá trị mặc định
        data = data.fillna({
            "HeKenh": "",
            "TenChannel": "",
            "LinkChannel": "",
            "TotalFollower": 0,
            "SoLike": 0,
            "NgaySave": datetime.now().strftime('%m-%d-%Y')
        })

        # Câu lệnh kiểm tra nếu Channel đã tồn tại
        select_query = """
        SELECT IdChannel FROM Channel 
        WHERE TenChannel = %s AND LinkChannel = %s;
        """

        # Câu lệnh thêm dữ liệu mới
        insert_query = """
        INSERT INTO Channel (
            TenChannel, LinkChannel, TotalFollower, TotalLike, NgaySave, 
            NenTangId, HeCongDongId, HeSinhThaiId
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        added_count = 0  # Đếm số lượng bản ghi được thêm mới

        for _, row in data.iterrows():
            # Lấy ID hệ cộng đồng và hệ sinh thái dựa vào tên hệ kênh
            cursor.execute(he_cong_dong_query, (row["HeKenh"],))
            he_cong_dong_id = cursor.fetchone()
            he_cong_dong_id = he_cong_dong_id[0] if he_cong_dong_id else None

            cursor.execute(he_sinh_thai_query, (row["HeKenh"],))
            he_sinh_thai_id = cursor.fetchone()
            he_sinh_thai_id = he_sinh_thai_id[0] if he_sinh_thai_id else None

            # Chuyển đổi định dạng ngày
            ngay_save = convert_date_format(row["NgaySave"])

            # Kiểm tra nếu channel đã tồn tại
            cursor.execute(select_query, (row["TenChannel"], row["LinkChannel"]))
            if cursor.fetchone() is None:
                # Thêm mới nếu không tồn tại
                cursor.execute(insert_query, (
                    row["TenChannel"], row["LinkChannel"], row["TotalFollower"],
                    row["SoLike"], ngay_save, nen_tang_id, 
                    he_cong_dong_id, he_sinh_thai_id
                ))
                added_count += 1

        conn.commit()  # Lưu thay đổi
        print(f"Đã thêm {added_count} bản ghi mới vào bảng Channel.")

    except mysql.connector.Error as err:
        print(f"Lỗi khi thêm dữ liệu: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi

def insert_data_group_facebook(conn, data):
    '''
        Hàm này thêm dữ liệu vào bảng channel, dữ liệu được thêm vào thuộc nền tảng group facebook
    '''
    cursor = conn.cursor()

    try:
        # Lấy IdNenTang của Group từ bảng NenTang
        cursor.execute("SELECT IdNenTang FROM NenTang WHERE TenNenTang = 'Group';")
        nen_tang_id = cursor.fetchone()[0]

        # Câu lệnh lấy ID hệ cộng đồng và hệ sinh thái
        he_cong_dong_query = "SELECT Id FROM HeCongDong WHERE Name = %s"
        he_sinh_thai_query = "SELECT Id FROM HeSinhThai WHERE Name = %s"

        # Xử lý NaN trong DataFrame bằng giá trị mặc định
        data = data.fillna({
            "HeKenh": "",
            "TenChannel": "",
            "LinkChannel": "",
            "TotalFollower": 0,
            "SoLike": 0,
            "NgaySave": datetime.now().strftime('%m-%d-%Y')
        })

        # Câu lệnh kiểm tra nếu Channel đã tồn tại
        select_query = """
        SELECT IdChannel FROM Channel 
        WHERE TenChannel = %s AND LinkChannel = %s;
        """

        # Câu lệnh thêm mới dữ liệu
        insert_query = """
        INSERT INTO Channel (
            TenChannel, LinkChannel, TotalFollower, TotalLike, NgaySave, 
            NenTangId, HeCongDongId, HeSinhThaiId
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        added_count = 0  # Đếm số lượng bản ghi được thêm mới

        for _, row in data.iterrows():
            # Lấy ID hệ cộng đồng và hệ sinh thái dựa vào tên hệ kênh
            cursor.execute(he_cong_dong_query, (row["HeKenh"],))
            he_cong_dong_id = cursor.fetchone()
            he_cong_dong_id = he_cong_dong_id[0] if he_cong_dong_id else None

            cursor.execute(he_sinh_thai_query, (row["HeKenh"],))
            he_sinh_thai_id = cursor.fetchone()
            he_sinh_thai_id = he_sinh_thai_id[0] if he_sinh_thai_id else None

            # Chuyển đổi định dạng ngày
            ngay_save = convert_date_format(row["NgaySave"])

            # Kiểm tra nếu Channel đã tồn tại
            cursor.execute(select_query, (row["TenChannel"], row["LinkChannel"]))
            if cursor.fetchone() is None:
                # Thêm mới nếu không tồn tại
                cursor.execute(insert_query, (
                    row["TenChannel"], row["LinkChannel"], row["TotalFollower"],
                    row["SoLike"], ngay_save, nen_tang_id, 
                    he_cong_dong_id, he_sinh_thai_id
                ))
                added_count += 1

        conn.commit()  # Lưu thay đổi
        print(f"Đã thêm {added_count} bản ghi mới vào bảng Channel.")

    except mysql.connector.Error as err:
        print(f"Lỗi khi thêm dữ liệu: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi

def insert_data_page_tiktok(conn, data):
    
    '''
        Hàm này thực hiện việc thêm dữ liệu vào bảng channel, dữ liệu được thêm vào thuộc nền tảng tiktok
    '''
    cursor = conn.cursor()

    try:
        # Lấy IdNenTang của TikTok từ bảng NenTang
        cursor.execute("SELECT IdNenTang FROM NenTang WHERE TenNenTang = 'Tiktok';")
        nen_tang_id = cursor.fetchone()[0]

        # Câu lệnh lấy ID hệ cộng đồng và hệ sinh thái
        he_cong_dong_query = "SELECT Id FROM HeCongDong WHERE Name = %s"
        he_sinh_thai_query = "SELECT Id FROM HeSinhThai WHERE Name = %s"

        # Xử lý NaN trong DataFrame bằng giá trị mặc định
        data = data.fillna({
            "HeKenh": "",
            "TenChannel": "",
            "LinkChannel": "",
            "TotalFollower": 0,
            "TotalLike": 0,  # Đảm bảo cột TotalLike có giá trị mặc định nếu thiếu
            "NgaySave": datetime.now().strftime('%m-%d-%Y')
        })

        # Câu lệnh kiểm tra nếu Channel đã tồn tại
        select_query = """
        SELECT IdChannel FROM Channel 
        WHERE TenChannel = %s AND LinkChannel = %s;
        """

        # Câu lệnh thêm mới dữ liệu
        insert_query = """
        INSERT INTO Channel (
            TenChannel, LinkChannel, TotalFollower, TotalLike, NgaySave, 
            NenTangId, HeCongDongId, HeSinhThaiId
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        added_count = 0  # Đếm số lượng bản ghi được thêm mới

        for _, row in data.iterrows():
            # Lấy ID hệ cộng đồng và hệ sinh thái dựa vào tên hệ kênh
            cursor.execute(he_cong_dong_query, (row["HeKenh"],))
            he_cong_dong_id = cursor.fetchone()
            he_cong_dong_id = he_cong_dong_id[0] if he_cong_dong_id else None

            cursor.execute(he_sinh_thai_query, (row["HeKenh"],))
            he_sinh_thai_id = cursor.fetchone()
            he_sinh_thai_id = he_sinh_thai_id[0] if he_sinh_thai_id else None

            # Chuyển đổi định dạng ngày
            ngay_save = convert_date_format(row["NgaySave"])

            # Kiểm tra nếu Channel đã tồn tại
            cursor.execute(select_query, (row["TenChannel"], row["LinkChannel"]))
            if cursor.fetchone() is None:
                # Thêm mới nếu không tồn tại
                cursor.execute(insert_query, (
                    row["TenChannel"], row["LinkChannel"], row["TotalFollower"],
                    row["TotalLike"], ngay_save, nen_tang_id, 
                    he_cong_dong_id, he_sinh_thai_id
                ))
                added_count += 1

        conn.commit()  # Lưu thay đổi
        print(f"Đã thêm {added_count} bản ghi mới vào bảng Channel.")

    except mysql.connector.Error as err:
        print(f"Lỗi khi thêm dữ liệu: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi


def insert_data_post_youtube(conn, data):
    '''
        hàm này thêm dữ liệu vào bảng post , dữ liệu được thêm vào là dữ liệu post của các kênh youtube
    '''
    cursor = conn.cursor(buffered=True)  # Tránh lỗi unread result

    try:
        # Lấy id của nền tảng YouTube từ bảng NenTang
        cursor.execute("SELECT IdNenTang FROM NenTang WHERE TenNenTang = 'YouTube';")
        nen_tang_id = cursor.fetchone()[0]

        # Câu lệnh lấy ID của Channel dựa vào tên channel
        select_channel_query = """
        SELECT IdChannel FROM Channel WHERE TenChannel = %s;
        """

        # Câu lệnh kiểm tra xem bản ghi đã tồn tại chưa
        check_post_query = """
        SELECT 1 FROM Post 
        WHERE ChannelId = %s AND Content = %s AND NgayDang = %s;
        """

        # Câu lệnh thêm dữ liệu vào bảng Post
        insert_post_query = """
        INSERT INTO Post (
            ChannelId, Content, NgayDang, SoLuotXem, SoLike, SoCmt, 
            LinkBaiViet, NgaySave
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Điền giá trị mặc định cho NaN trong DataFrame
        data = data.fillna({
            "Content": "",
            "NgayDang": datetime.now().strftime('%Y-%m-%d'),
            "SoLuotXem": 0,
            "SoLike": 0,
            "SoCmt": 0,
            "NgaySave": datetime.now().strftime('%Y-%m-%d')
        })

        added_count = 0  # Đếm số lượng bản ghi thêm mới

        for _, row in data.iterrows():
            # Lấy IdChannel dựa vào TenChannel
            cursor.execute(select_channel_query, (row["TenChannel"],))
            channel_id = cursor.fetchone()

            if channel_id:
                # Chuyển đổi định dạng ngày cho NgayDang và NgaySave
                ngay_dang = convert_date_format(row["NgayDang"])
                ngay_save = convert_date_format(row["NgaySave"])

                # Kiểm tra xem bản ghi đã tồn tại chưa
                cursor.execute(check_post_query, (channel_id[0], row["Content"], ngay_dang))
                if cursor.fetchone():
                    continue  # Bỏ qua nếu bản ghi đã tồn tại

                # Thêm mới dữ liệu vào bảng Post
                cursor.execute(insert_post_query, (
                    channel_id[0], row["Content"], ngay_dang, 
                    row["SoLuotXem"], row["SoLike"], row["SoCmt"], 
                    row["LinkBaiViet"], ngay_save
                ))
                added_count += 1
            else:
                print(f"Không tìm thấy Channel: {row['TenChannel']}")

        conn.commit()  # Lưu thay đổi
        print(f"Đã thêm {added_count} bản ghi mới vào bảng Post.")

    except mysql.connector.Error as err:
        print(f"Lỗi khi thêm dữ liệu: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi
        
    
def insert_data_post_tiktok(conn, data):
    
    '''
        Hàm này thêm dữ liệu vào bảng post , dữ liệu được thêm vào là các post của các kênh thuộc nền tảng tiktok. 
    '''
    cursor = conn.cursor(buffered=True)

    try:
        # Lấy id của nền tảng TikTok từ bảng NenTang
        cursor.execute("SELECT IdNenTang FROM NenTang WHERE TenNenTang = 'Tiktok';")
        nen_tang_id = cursor.fetchone()

        if nen_tang_id is None:
            print("Không tìm thấy nền tảng TikTok.")
            return

        nen_tang_id = nen_tang_id[0]

        # Câu lệnh lấy ID của Channel dựa vào tên channel
        select_channel_query = "SELECT IdChannel FROM Channel WHERE TenChannel = %s;"

        # Câu lệnh thêm dữ liệu vào bảng Post
        insert_post_query = """
        INSERT INTO Post (
            ChannelId, Content, NgayDang, SoLuotXem, SoLike, SoShare, 
            SoCmt, LinkBaiViet, NgaySave
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Điền giá trị mặc định cho NaN trong DataFrame
        data = data.fillna({
            "Content": "",
            "NgayDang": datetime.now().strftime('%Y-%m-%d'),
            "SoLuotXem": 0,
            "SoLike": 0,
            "SoShare": 0,
            "Save": 0,
            "SoCmt": 0,
            "NgaySave": datetime.now().strftime('%Y-%m-%d'),
            "LinkBaiViet": ""
        })

        added_count = 0  # Đếm số lượng bản ghi được thêm

        for _, row in data.iterrows():
            # Lấy IdChannel dựa vào TenChannel
            cursor.execute(select_channel_query, (row["TenChannel"],))
            channel_id = cursor.fetchone()

            if channel_id is None:
                print(f"Không tìm thấy Channel: {row['TenChannel']}")
                continue  # Bỏ qua nếu không tìm thấy channel

            # Chuyển đổi định dạng ngày
            ngay_dang = convert_date_format(row["NgayDang"])
            ngay_save = convert_date_format(row["NgaySave"])

            # Thực hiện thêm dữ liệu vào bảng Post
            cursor.execute(insert_post_query, (
                channel_id[0], row["Content"], ngay_dang, 
                row["SoLuotXem"], row["SoLike"], row["SoShare"], 
                row["SoCmt"], row["LinkBaiViet"], ngay_save
            ))
            added_count += 1

        conn.commit()  # Lưu thay đổi vào cơ sở dữ liệu
        print(f"Đã thêm {added_count} bản ghi mới vào bảng Post.")

    except mysql.connector.Error as err:
        print(f"Lỗi khi thêm dữ liệu: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi
    
