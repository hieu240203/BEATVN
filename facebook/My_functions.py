import os
import re
import json
import time
import random
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def read_file(file_path_read):   # hàm độc dữ liệu từ file
    with open(file_path_read, 'r', encoding='utf-8') as file:
        content = file.readlines()  # Đọc từng dòng
    return [line.strip() for line in content] 

def normalize_number(value):
    if value is None:
        return None
    value = value.strip().lower()  # Bỏ khoảng trắng và chuyển chữ cái sang chữ thường
    if 'm' in value:  # Xử lý giá trị triệu
        return float(value.replace('m', '')) * 1_000_000
    elif 'k' in value:  # Xử lý giá trị nghìn
        return float(value.replace('k', '')) * 1_000
    else:
        try:
            return float(value)  # Trường hợp là số không có hậu tố
        except ValueError:
            return None  # Trả về None nếu không phải là số hợp lệ
        
def convert_to_dataframe_from_txt(file_path):
    # Mở file TXT và đọc dữ liệu
    with open(file_path, 'r', encoding='utf-8') as f:
        data = f.read()

    data_page = []
    data_dict = {
        "Hệ Kênh": "", 
        "Tên page": "",
        "Link page": "",          
        "Số like": "",
        "Số Follower": "",
        "Số Người tham gia": "", 
        "Nền tảng": "", 
        "Ngày save": "",
    }

    for line in data.split('\n'):
        if line.startswith("Hệ Kênh:"):
            if data_dict["Hệ Kênh"]:
                data_page.append(data_dict.copy())
                data_dict = {key: None for key in data_dict}
            data_dict["Hệ Kênh"] = line.split(":")[1].strip()

        elif line.startswith("Tên Page:") or line.startswith("Tên page:"):
            data_dict["Tên page"] = line.split(":")[1].strip()

        elif line.startswith("Link page:"):
            link_page = line.split("Link page:", 1)[1].strip()
            data_dict["Link page"] = link_page

        elif line.startswith("Số like:"):
            data_dict["Số like"] = line.split(":")[1].strip()

        elif line.startswith("Số Follower:"):
            data_dict["Số Follower"] = line.split(":")[1].strip()

        elif line.startswith("Số Người tham gia:"):
            data_dict["Số Người tham gia"] = line.split(":")[1].strip()

        elif line.startswith("Nền tảng:"):
            data_dict["Nền tảng"] = line.split(":")[1].strip()

        elif line.startswith("Ngày save:"):
            data_dict["Ngày save"] = line.split(":")[1].strip() 

    if data_dict["Hệ Kênh"]:
        data_page.append(data_dict.copy())

    # Chuyển đổi danh sách dictionary thành DataFrame
    df = pd.DataFrame(data_page)

    # Chuẩn hóa dữ liệu cho các cột "Số like", "Số Follower", và "Số Người tham gia"
    df["Số like"] = df["Số like"].apply(normalize_number)
    df["Số Follower"] = df["Số Follower"].apply(normalize_number)
    df["Số Người tham gia"] = df["Số Người tham gia"].apply(normalize_number)

    return df

def save_file(data, Type_data): 
    # Tạo DataFrame từ dữ liệu
    df = pd.DataFrame(data)
    
    # Lấy ngày hiện tại và định dạng thành chuỗi MM-DD-YYYY
    current_date = datetime.now().strftime('%m-%d-%Y')
    
    # Đường dẫn chính để lưu file (cùng một thư mục cho tất cả đoạn mã)
    base_dir = r"D:\BeatVn\Data"  # Chỉ định thư mục gốc

    # Tạo thư mục theo ngày, ví dụ: Data_10-10-2024 (chỉ tạo một thư mục cho cả 3 đoạn mã)
    folder_name = f"Data_{current_date.replace('-', '-')}"  # Tên thư mục theo ngày
    folder_path = os.path.join(base_dir, folder_name)

    # Kiểm tra và tạo thư mục nếu chưa tồn tại (chỉ tạo một lần)
    os.makedirs(folder_path, exist_ok=True)
    
    # Kiểm tra dạng dữ liệu và tạo tên file phù hợp
    if Type_data.lower() == "post":
        file_name = f"post_facebook_{current_date}.xlsx"
    elif Type_data.lower() == "page":
        file_name = f"page_facebook_{current_date}.xlsx"
    elif Type_data.lower() == "video":
        file_name = f"video_facebook_{current_date}.xlsx"
    elif Type_data.lower() == "group": 
        file_name = f"group_facebook_{current_date}.xlsx"
    else:
        # Trường hợp không khớp với "post", "page" hoặc "video", tạo tên file mặc định
        file_name = f"data_facebook_{current_date}.xlsx"
    
    # Đường dẫn đầy đủ tới file (trong cùng thư mục)
    file_path = os.path.join(folder_path, file_name)
    
    # Lưu DataFrame dưới dạng file Excel
    df.to_excel(file_path, index=False)
    
    return f"File đã được lưu với tên: {file_path}"

def scroll_until_end(driver, duration=3):  # Giới hạn thời gian kéo là 60 giây (1 phút)
    # Lưu thời gian bắt đầu
    start_time = time.time()
    
    # Lưu vị trí cũ của cuộn trang
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:
        # Kiểm tra nếu thời gian kéo đã vượt quá thời gian giới hạn
        if time.time() - start_time > duration:
            break
        
        # Cuộn xuống dưới cùng
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Chờ một chút để nội dung mới tải
        time.sleep(random.randint(3, 6))
        
        # Lấy chiều cao mới của trang
        new_height = driver.execute_script("return document.body.scrollHeight")
        
        # Nếu chiều cao không thay đổi, có nghĩa là đã đến cuối trang
        if new_height == last_height:
            break
        
        last_height = new_height

def login(browser, email, password):
    browser.get('https://www.facebook.com/login')
    email_input = browser.find_element("id", "email")
    password_input = browser.find_element("id", "pass")
    email_input.send_keys(email)
    password_input.send_keys(password)
    login_button = browser.find_element("name", "login")
    login_button.click()
    time.sleep(random.randint(5, 10))