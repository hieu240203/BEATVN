import mysql.connector

import mysql.connector
from mysql.connector import Error

def connect_to_database(
    host="127.0.0.1", 
    user="root", 
    password="DataBeatVN", 
    database="DataBeatVN"
):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Không thể kết nối đến cơ sở dữ liệu. Lỗi: {e}")
        return None
