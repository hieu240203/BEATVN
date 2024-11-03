# FIle này thực hiện việc lấy dữ liệu từ database

import pandas as pd
import mysql.connector

def get_channel_data(conn):
    """
    Hàm lấy dữ liệu từ bảng Channel và trả về DataFrame.
    """
    query = "SELECT * FROM Channel;"
    
    try:
        # Thực hiện truy vấn và lấy dữ liệu từ bảng Channel
        df = pd.read_sql(query, conn)
        print(f"Lấy thành công {len(df)} bản ghi từ bảng Channel.")
        return df

    except mysql.connector.Error as err:
        print(f"Lỗi khi lấy dữ liệu: {err}")
        return None
    
def get_post_data(conn):
    """
    Hàm lấy dữ liệu từ bảng Post và trả về DataFrame.
    """
    query = "SELECT * FROM Post;"
    
    try:
        # Thực hiện truy vấn và lấy dữ liệu từ bảng Post
        df = pd.read_sql(query, conn)
        print(f"Lấy thành công {len(df)} bản ghi từ bảng Post.")
        return df

    except mysql.connector.Error as err:
        print(f"Lỗi khi lấy dữ liệu: {err}")
        return None
    

def get_he_cong_dong_data(conn):
    """
    Hàm lấy dữ liệu từ bảng HeCongDong và trả về DataFrame.
    """
    query = "SELECT * FROM HeCongDong;"

    try:
        # Thực hiện truy vấn SQL và trả về DataFrame
        df = pd.read_sql(query, conn)
        print(f"Lấy thành công {len(df)} bản ghi từ bảng HeCongDong.")
        return df

    except mysql.connector.Error as err:
        print(f"Lỗi khi lấy dữ liệu: {err}")
        return None
    
def get_nen_tang_data(conn):
    """
    Hàm lấy dữ liệu từ bảng NenTang và trả về DataFrame.
    """
    query = "SELECT * FROM NenTang;"

    try:
        # Thực hiện truy vấn SQL và trả về DataFrame
        df = pd.read_sql(query, conn)
        print(f"Lấy thành công {len(df)} bản ghi từ bảng NenTang.")
        return df

    except mysql.connector.Error as err:
        print(f"Lỗi khi lấy dữ liệu: {err}")
        return None
    
def get_he_sinh_thai_data(conn):
    """
    Hàm lấy dữ liệu từ bảng HeSinhThai và trả về DataFrame.
    """
    query = "SELECT * FROM HeSinhThai;"

    try:
        # Thực hiện truy vấn SQL và trả về DataFrame
        df = pd.read_sql(query, conn)
        print(f"Lấy thành công {len(df)} bản ghi từ bảng HeSinhThai.")
        return df

    except mysql.connector.Error as err:
        print(f"Lỗi khi lấy dữ liệu: {err}")
        return None
    
def get_channel_in_time(conn, start_date, end_date): 
    '''
        Hàm này mục tiêu là lấy dữ liệu trong một khoảng thời gian nhất định trong bảng channel
        start_date: Ngày bắt đầu (định dạng 'YYYY-MM-DD')
        end_date: Ngày kết thúc (định dạng 'YYYY-MM-DD')
    '''
    
    cursor = conn.cursor()
    query = '''
        SELECT *
        FROM Channel
        WHERE NgaySave BETWEEN %s AND %s;
    '''
    
    try:
        # Thực thi truy vấn với các tham số start_date và end_date
        cursor.execute(query, (start_date, end_date))
        
        # Lấy tất cả kết quả
        results = cursor.fetchall()
        
        # Đóng con trỏ và trả về kết quả
        cursor.close()
        return results
    
    except mysql.connector.Error as err:
        print(f"Lỗi khi truy vấn dữ liệu: {err}")
        cursor.close()
        return None
    
def get_post_in_time(conn, start_date, end_date):
    '''
    Hàm này mục tiêu là lấy dữ liệu trong một khoảng thời gian nhất định trong bảng Post.
    start_date: Ngày bắt đầu (định dạng 'YYYY-MM-DD')
    end_date: Ngày kết thúc (định dạng 'YYYY-MM-DD')
    '''
    cursor = conn.cursor()
    query = '''
        SELECT *
        FROM Post
        WHERE NgayDang BETWEEN %s AND %s;
    '''
    
    try:
        # Thực thi truy vấn với các tham số start_date và end_date
        cursor.execute(query, (start_date, end_date))
        
        # Lấy tất cả kết quả
        results = cursor.fetchall()
        
        # Đóng con trỏ và trả về kết quả
        cursor.close()
        return results
    
    except mysql.connector.Error as err:
        print(f"Lỗi khi truy vấn dữ liệu: {err}")
        cursor.close()
        return None