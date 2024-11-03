import mysql.connector

def delete_table_data(conn, table_name):
    """
    Hàm xóa toàn bộ dữ liệu trong một bảng và reset AUTO_INCREMENT nếu có.
    """
    cursor = conn.cursor()

    try:
        # Xóa toàn bộ dữ liệu trong bảng
        delete_query = f"DELETE FROM {table_name};"
        cursor.execute(delete_query)
        conn.commit()
        print(f"Đã xóa {cursor.rowcount} bản ghi từ bảng {table_name}.")

        # Kiểm tra xem bảng có cột AUTO_INCREMENT không
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}' 
            AND EXTRA LIKE '%auto_increment%'
        """)
        result = cursor.fetchone()

        # Nếu bảng có cột AUTO_INCREMENT, reset lại về 1
        if result:
            reset_query = f"ALTER TABLE {table_name} AUTO_INCREMENT = 1;"
            cursor.execute(reset_query)
            conn.commit()
            print(f"Đã reset AUTO_INCREMENT cho bảng {table_name}.")
        else:
            print(f"Bảng {table_name} không có cột AUTO_INCREMENT.")

    except mysql.connector.Error as err:
        print(f"Lỗi khi xóa dữ liệu: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi


def delete_all_tables(conn):
    '''
        Hàm này thực hiện việc xóa toàn bộ các bảng trong database.
        - conn : data được liên kết
    '''
    cursor = conn.cursor()
    try:
        # Tắt kiểm tra khóa ngoại để có thể xóa bảng liên quan
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

        # Lấy danh sách các bảng trong database
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        # Thực hiện lệnh DROP TABLE cho từng bảng
        for (table_name,) in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                print(f"Đã xóa bảng: {table_name}")
            except mysql.connector.Error as err:
                print(f"Lỗi khi xóa bảng {table_name}: {err}")

        conn.commit()  # Xác nhận thay đổi
        print("Đã xóa toàn bộ các bảng trong database.")

    except mysql.connector.Error as err:
        print(f"Lỗi khi thực thi: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi

    finally:
        # Bật lại kiểm tra khóa ngoại
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    

def delete_column_from_table(conn, table_name, column_name):
    '''
        Hàm này thực hiện việc xóa một cột trong database 
        - conn : kết nối với database
        - table_name: Tên bảng có cột được xóa
        - Column_ name: Tên cột trong bảng được xóa
    '''
    cursor = conn.cursor()
    try:
        # Tạo câu lệnh ALTER TABLE để xóa cột
        drop_column_query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
        cursor.execute(drop_column_query)  # Thực thi câu lệnh
        conn.commit()  # Xác nhận thay đổi
        print(f"Đã xóa cột '{column_name}' khỏi bảng '{table_name}'.")
    except mysql.connector.Error as err:
        print(f"Lỗi khi xóa cột: {err}")
        conn.rollback()  # Hoàn tác nếu có lỗi
