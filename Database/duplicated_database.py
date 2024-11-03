import mysql.connector

def remove_channel_duplicates_and_reset_increment(conn):
    """
    Hàm xóa các bản ghi trùng lặp trong bảng Channel và reset lại AUTO_INCREMENT cho IdChannel.
    """
    cursor = conn.cursor()

    try:
        # Bước 1: Xóa các bản ghi trùng lặp dựa trên tất cả các cột ngoại trừ IdChannel
        delete_sql = """
        DELETE t1
        FROM Channel t1
        JOIN (
            SELECT 
                MIN(IdChannel) AS min_id,
                TenChannel, NenTangId, HeCongDongId, HeSinhThaiId, 
                LinkChannel, TotalFollower, TotalLike, TotalView, NgaySave
            FROM Channel
            GROUP BY 
                TenChannel, NenTangId, HeCongDongId, HeSinhThaiId, 
                LinkChannel, TotalFollower, TotalLike, TotalView, NgaySave
        ) t2 ON t1.IdChannel > t2.min_id
           AND (t1.TenChannel = t2.TenChannel OR (t1.TenChannel IS NULL AND t2.TenChannel IS NULL))
           AND (t1.NenTangId <=> t2.NenTangId)
           AND (t1.HeCongDongId <=> t2.HeCongDongId)
           AND (t1.HeSinhThaiId <=> t2.HeSinhThaiId)
           AND (t1.LinkChannel = t2.LinkChannel OR (t1.LinkChannel IS NULL AND t2.LinkChannel IS NULL))
        """
        print("Đang xóa các bản ghi trùng lặp trong bảng Channel...")
        cursor.execute(delete_sql)

        # Bước 2: Reset AUTO_INCREMENT cho cột IdChannel
        cursor.execute("SELECT MAX(IdChannel) FROM Channel;")
        max_id = cursor.fetchone()[0] or 0  # Nếu bảng rỗng, max_id là 0
        new_increment = max_id + 1

        reset_increment_sql = f"ALTER TABLE Channel AUTO_INCREMENT = {new_increment};"
        print("Reset AUTO_INCREMENT cho bảng Channel...")
        cursor.execute(reset_increment_sql)

        # Commit các thay đổi
        conn.commit()
        print("Đã xóa các bản ghi trùng lặp và reset AUTO_INCREMENT thành công.")

    except mysql.connector.Error as err:
        print(f"Lỗi: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi


def remove_post_duplicates_and_reset_increment(conn):
    """
    Hàm xóa các bản ghi trùng lặp trong bảng Post và reset lại AUTO_INCREMENT cho IdPost.
    """
    cursor = conn.cursor()

    try:
        # Bước 1: Xóa các bản ghi trùng lặp, giữ lại bản ghi đầu tiên
        delete_sql = """
        DELETE t1
        FROM Post t1
        JOIN (
            SELECT 
                MIN(IdPost) AS min_id,
                ChannelId, Content, NgayDang, SoLike, SoShare, SoCmt, 
                SoLuotXem, LinkBaiViet, NgaySave
            FROM Post
            GROUP BY 
                ChannelId, Content, NgayDang, SoLike, SoShare, SoCmt, 
                SoLuotXem, LinkBaiViet, NgaySave
        ) t2 ON t1.IdPost > t2.min_id
           AND t1.ChannelId <=> t2.ChannelId
           AND t1.Content = t2.Content
           AND t1.NgayDang <=> t2.NgayDang
           AND t1.LinkBaiViet = t2.LinkBaiViet
        """
        print("Đang xóa các bản ghi trùng lặp trong bảng Post...")
        cursor.execute(delete_sql)

        # Bước 2: Lấy giá trị AUTO_INCREMENT mới
        cursor.execute("SELECT MAX(IdPost) FROM Post;")
        max_id = cursor.fetchone()[0] or 0  # Nếu bảng rỗng, max_id là 0
        new_increment = max_id + 1

        # Bước 3: Reset AUTO_INCREMENT cho bảng Post
        reset_increment_sql = f"ALTER TABLE Post AUTO_INCREMENT = {new_increment};"
        print("Reset AUTO_INCREMENT cho bảng Post...")
        cursor.execute(reset_increment_sql)

        # Commit các thay đổi
        conn.commit()
        print("Đã xóa các bản ghi trùng lặp và reset AUTO_INCREMENT thành công.")

    except mysql.connector.Error as err:
        print(f"Lỗi: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi