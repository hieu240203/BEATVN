import re
import os
import json
import time
import random
import pathlib
import aiohttp  
import asyncio
import selenium
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def extract_first_class(soup, data_e2e_value='user-post-item-list'):
    # Tìm phần tử có thuộc tính data-e2e
    user_post_item_list = soup.find('div', {'data-e2e': data_e2e_value})

    # Lấy thẻ <div> con đầu tiên bên trong
    if user_post_item_list:
        first_div = user_post_item_list.find('div')

        # Lấy giá trị của thuộc tính class từ thẻ <div> đầu tiên
        if first_div:
            class_value = first_div.get('class')
            return ' '.join(class_value)  # Trả về giá trị của class từ thẻ đầu tiên
    return None  # Trả về None nếu không tìm thấy phần tử phù hợp


def read_file(file_path_read):   # hàm độc dữ liệu từ file
    with open(file_path_read, 'r', encoding='utf-8') as file:
        content = file.readlines()  # Đọc từng dòng
    return [line.strip() for line in content] 

def write_file(file_path, content):
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(content + '\n')
    except Exception as e:
        print(f"Đã xảy ra lỗi khi ghi vào file {file_path}: {e}")

def scroll_until_end(driver, duration=60):  # Giới hạn thời gian kéo là 60 giây (1 phút)
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
        time.sleep(random.randint(2, 3))
        
        # Lấy chiều cao mới của trang
        new_height = driver.execute_script("return document.body.scrollHeight")
        
        # Nếu chiều cao không thay đổi, có nghĩa là đã đến cuối trang
        if new_height == last_height:
            break
        
        last_height = new_height

def get_page_source_after_scroll(driver):  # hàm lấy page_source
    # Lấy mã nguồn của trang sau khi cuộn
    page_source = driver.page_source

    # Sử dụng BeautifulSoup để phân tích cú pháp HTML
    soup = BeautifulSoup(page_source, 'html.parser')
    
    return soup

def extract_user_info(soup, link_page, He_kenh):
    # Khởi tạo dictionary chứa thông tin, bao gồm ngày hiện tại
    data_page_1 = {
        "Hệ kênh" : He_kenh,
        "Ngày": datetime.now().strftime("%m-%d-%Y"),  # Lấy ngày hiện tại
        "Tên Page": "", 
        "Link page": link_page, 
        "Dang follow": "", 
        "Follow": "", 
        "Thích": "", 
    }
    print(data_page_1['Link page'])
    # Lấy tên page
    Ten_page = soup.find("h2", {"data-e2e": "user-subtitle"})
    data_page_1['Tên Page'] = Ten_page.text.strip() if Ten_page else ""

    # Lấy số lượng "Dang follow"
    following_tag = soup.find("strong", {"data-e2e": "following-count"})
    data_page_1["Dang follow"] = following_tag.text.strip() if following_tag else ""

    # Lấy số lượng "Follow"
    followers_tag = soup.find("strong", {"data-e2e": "followers-count"})
    data_page_1["Follow"] = followers_tag.text.strip() if followers_tag else ""

    # Lấy số lượng "Thích"
    likes_tag = soup.find("strong", {"data-e2e": "likes-count"})
    data_page_1["Thích"] = likes_tag.text.strip() if likes_tag else ""  
    
    return data_page_1

def extract_links(elements):
    video_links = []
    for element in elements:
        link_tag = element.find('a')
        if link_tag:
            video_links.append(link_tag['href'])
    return video_links

def extract_view_video(elements): 
    Video_views = []
    for element in elements: 
        view_count_tag = element.find('strong', {'data-e2e': 'video-views'})
        if view_count_tag:
            Video_views.append(view_count_tag.text.strip())

    return Video_views

def extract_ghim(elements):
    video_ghim = []

    for element in elements:
        # Tìm thẻ strong với thuộc tính 'data-e2e' là 'video-card-badge'
        view_count_tag = element.find('div', {'data-e2e': 'video-card-badge'})
        
        # Kiểm tra nếu thẻ tồn tại và nội dung là 'Đã ghim' hoặc 'pinned'
        if view_count_tag and (view_count_tag.text.strip().lower() == "đã ghim" or view_count_tag.text.strip().lower() == "pinned"):
            video_ghim.append(True)
        else:
            video_ghim.append(False)

    return video_ghim


def chuan_hoa_thoi_gian(thoi_gian_str):
    today = datetime.today()  # Lấy ngày và giờ hiện tại
    year = today.year  # Lấy năm hiện tại

    # Kiểm tra nếu thời gian có định dạng MM-DD-YYYY
    try:
        return datetime.strptime(thoi_gian_str, "%m-%d-%Y").strftime("%m-%d-%Y")
    except ValueError:
        pass

    # Kiểm tra nếu thời gian có định dạng YYYY-M-D (ví dụ: "2018-9-15")
    try:
        date_obj = datetime.strptime(thoi_gian_str, "%Y-%m-%d")
        return date_obj.strftime("%m-%d-%Y")  # Giữ nguyên năm
    except ValueError:
        pass

    # Kiểm tra nếu thời gian có định dạng MM-DD (ví dụ: "9-2")
    match = re.match(r"(\d{1,2})-(\d{1,2})", thoi_gian_str)
    if match:
        month, day = match.groups()
        return f"{month.zfill(2)}-{day.zfill(2)}-{year}"  # Thêm năm hiện tại

    # Các kiểm tra khác
    match = re.match(r"(\d+)d ago", thoi_gian_str)
    if match:
        days_ago = int(match.group(1))
        target_date = today - timedelta(days=days_ago)
        return target_date.strftime("%m-%d-%Y")

    match = re.match(r"(\d+)w ago", thoi_gian_str)
    if match:
        weeks_ago = int(match.group(1))
        target_date = today - timedelta(weeks=weeks_ago)
        return target_date.strftime("%m-%d-%Y")

    match = re.match(r"(\d+)h ago", thoi_gian_str)
    if match:
        hours_ago = int(match.group(1))
        target_date = today - timedelta(hours=hours_ago)
        return target_date.strftime("%m-%d-%Y")

    match = re.match(r"(\d+)m ago", thoi_gian_str)
    if match:
        minutes_ago = int(match.group(1))
        target_date = today - timedelta(minutes=minutes_ago)
        return target_date.strftime("%m-%d-%Y")

    match = re.match(r"(\d+)s ago", thoi_gian_str)
    if match:
        seconds_ago = int(match.group(1))
        target_date = today - timedelta(seconds=seconds_ago)
        return target_date.strftime("%m-%d-%Y")

    # Nếu không phù hợp với bất kỳ định dạng nào, trả về chuỗi gốc
    return thoi_gian_str


def chuan_hoa_luot_thich(like_str):
    # Kiểm tra nếu chuỗi có định dạng k (nghìn)
    match = re.match(r"([\d\.]+)k", like_str, re.IGNORECASE)
    if match:
        value = float(match.group(1))
        return int(value * 1000)  # Chuyển đổi thành số nguyên

    # Kiểm tra nếu chuỗi có định dạng m (triệu)
    match = re.match(r"([\d\.]+)m", like_str, re.IGNORECASE)
    if match:
        value = float(match.group(1))
        return int(value * 1000000)  # Chuyển đổi thành số nguyên

    # Nếu không có "k" hoặc "m", trả về giá trị ban đầu dưới dạng số nguyên
    try:
        return int(like_str)
    except ValueError:
        return like_str  # Trả về giá trị ban đầu nếu không chuyển đổi được

def collect_data_from_link(driver, link, view,ghim):

    current_day = datetime.now().strftime('%m-%d-%Y')
    # Tạo một dictionary để lưu trữ dữ liệu
    data_post = {
        "Url": link,
        "Ngày đăng": "",
        "Lượt xem": view,
        "Tym": "",
        "Share": "",
        "Save" : "", 
        "Tổng số comment": "",
        "Content": "",
        "Ghim" : ghim, 
        "Thời gian video": "",
        "Tên Page": "",
        "Hashtags": [],
        "Ngày save": current_day,
    }
    
    try:
        driver.get(link)
        time.sleep(0.5)
        # Lấy mã nguồn của trang    
        page_source_link = driver.page_source
        soup_link = BeautifulSoup(page_source_link, 'html.parser')

        # Lấy thời gian video
        try:
            seek_bar = soup_link.find("div", class_="css-1cuqcrm-DivSeekBarTimeContainer e1ya9dnw1")
            if seek_bar:
                thoi_gian_video = seek_bar.get_text(strip=True)
                data_post["Thời gian video"] = chuan_hoa_thoi_gian(thoi_gian_video)  # Chuẩn hóa thời gian
        except Exception as e:
            print(f"Lỗi khi lấy thời gian video: {e}")

        # Lấy số lượt thích
        try:
            tym = soup_link.find("strong", {"data-e2e": "like-count"})
            data_post["Tym"] = convert_to_number(tym.text.strip()) if tym else ""
        except Exception as e:
            print(f"Lỗi khi lấy số lượt thích: {e}")
        
        # Lấy số lượt save
        try:
            Video_save = soup_link.find("strong", {"data-e2e": "undefined-count"})
            data_post["Save"] = convert_to_number(Video_save.text.strip()) if Video_save else ""
        except Exception as e:
            print(f"Lỗi khi lấy số lượt thích: {e}")

        # Lấy Tên Page
        try:
            sologan = soup_link.find("span", class_="css-1xccqfx-SpanNickName e17fzhrb1")
            data_post["Tên Page"] = sologan.text.strip() if sologan else ""
        except Exception as e:
            print(f"Lỗi khi lấy slogan: {e}")

        try:
            content = soup_link.find("span", class_="css-j2a19r-SpanText efbd9f0")
            hashtags_in_content = []  # Khởi tạo danh sách chứa hashtags trong nội dung
            if content:
                post_content = content.text.strip()  # Lấy toàn bộ nội dung bài đăng
                hashtags_in_content = re.findall(r"#\w+", post_content)  # Tìm tất cả hashtags
                
                # Không thay thế hashtags mà giữ lại nội dung bài đăng gốc
                data_post["Content"] = post_content
            else:
                data_post["Content"] = ""
            
            # Lấy hashtags từ các thẻ <a>
            hashtags_from_a_tags = soup_link.find_all("a", class_="ejg0rhn6 css-g8ml1x-StyledLink-StyledCommonLink er1vbsz0")
            hashtags_a_text = [tag.text.strip() for tag in hashtags_from_a_tags]
            
            # Hợp nhất danh sách hashtags từ nội dung và từ các thẻ <a>, loại bỏ trùng lặp
            combined_hashtags = list(set(hashtags_in_content + hashtags_a_text))

            # Chuyển đổi danh sách hashtags thành chuỗi và loại bỏ []
            data_post["Hashtags"] = ', '.join(combined_hashtags)

        except Exception as e:
            print(f"Lỗi khi lấy nội dung bài đăng và hashtag: {e}")

        # Lấy số lượng bình luận
        try:
            comment = soup_link.find("strong", {"class": "css-n6wn07-StrongText edu4zum2", "data-e2e": "comment-count"})
            data_post["Tổng số comment"] = convert_to_number(comment.text.strip()) if comment else ""
        except Exception as e:
            print(f"Lỗi khi lấy số lượng bình luận: {e}")

        # Lấy số lượng chia sẻ
        try:
            share = soup_link.find("strong", {"class": "css-n6wn07-StrongText edu4zum2", "data-e2e": "share-count"})
            data_post["Share"] = convert_to_number(share.text.strip()) if share else ""
        except Exception as e:
            print(f"Lỗi khi lấy số lượng chia sẻ: {e}")

        # Lấy ngày đăng
        try:
            outer_span = soup_link.find('span', {'data-e2e': 'browser-nickname'})
            if outer_span:
                inner_spans = outer_span.find_all('span')
                date_value = inner_spans[-1].get_text(strip=True) if inner_spans else ""
                post_date = chuan_hoa_thoi_gian(date_value)  # Giả sử hàm này chuẩn hóa thành chuỗi thời gian
                post_date_dt = datetime.strptime(post_date, "%m-%d-%Y")  # Chuyển đổi thành đối tượng datetime
                
                # # Chuyển đổi start_time và end_time sang đối tượng datetime
                # start_time_dt = datetime.strptime(start_time, "%m-%d-%Y")
                # end_time_dt = datetime.strptime(end_time, "%m-%d-%Y")

                # # Kiểm tra nếu post_date nằm trong khoảng start_time và end_time
                # if start_time_dt <= post_date_dt <= end_time_dt:
                data_post["Ngày đăng"] = post_date_dt
                # else:
                #     print(f"Bài đăng không nằm trong khoảng thời gian yêu cầu: {start_time} - {end_time}")
                #     return None  # Bỏ qua nếu bài đăng không nằm trong khoảng thời gian yêu cầu
        except Exception as e:
            print(f"Lỗi khi lấy ngày đăng: {e}")

        # sửa đoạn này thêm vào ngày bắt đầu crawl và ngày kết thúc

        return data_post
    
    except Exception as e:
        print(f"Đã xảy ra lỗi khi mở liên kết {link}: {e}")
        return None  # Trả về None nếu có lỗi

def convert_to_number(value):
    if isinstance(value, str):  # Kiểm tra nếu value là chuỗi
        value = value.replace(",", "").strip()  # Loại bỏ dấu phẩy và khoảng trắng
        if 'M' in value:
            return float(value.replace('M', '')) * 1e6
        elif 'K' in value:
            return float(value.replace('K', '')) * 1e3
        elif "B" in value:
            return float(value.replace("B", "")) * 1e9
        else:
            try:
                return float(value)  # Nếu không có 'K', 'M', 'B' thì chuyển thành số thực
            except ValueError:
                return None  # Nếu không thể chuyển đổi, trả về None
    return value  # Trả về giá trị nếu không phải chuỗi hoặc không có K, M, B

def convert_to_dataframe_from_txt(data):
    # Nếu data là một danh sách, chuyển nó thành chuỗi
    if isinstance(data, list):
        data = '\n'.join(data)

    data_page = []
    data_dict = {
        "Hệ kênh": "",
        "Ngày": "",
        "Tên page": "", 
        "Link page": "", 
        "Dang follow": "",
        "Follow": "",
        "Thích": "",
    }

    for line in data.split('\n'):
        if line.startswith("Ngày:"):
            if data_dict["Ngày"]:
                data_page.append(data_dict.copy())  # Lưu data_dict hiện tại vào danh sách
                # Reset lại dictionary, chỉ giữ "Hệ kênh" để không bị lặp
                he_kenh_value = data_dict["Hệ kênh"]
                data_dict = {
                    "Hệ kênh": he_kenh_value,  # Giữ lại giá trị hệ kênh cho lần tiếp theo
                    "Ngày": "",
                    "Tên page": "", 
                    "Link page": "", 
                    "Dang follow": "",
                    "Follow": "",
                    "Thích": "",
                }
            data_dict["Ngày"] = line.split("Ngày:")[1].strip() if len(line.split("Ngày:")) > 1 else ""

        elif line.startswith("Hệ kênh:"):
            data_dict["Hệ kênh"] = line.split(":")[1].strip() if len(line.split(":")) > 1 else ""

        elif line.startswith("Tên Page:") or line.startswith("Tên page:"):
            data_dict["Tên page"] = line.split(":")[1].strip() if len(line.split(":")) > 1 else ""

        elif line.startswith("Link page:"):
            data_dict["Link page"] = line.split("Link page:")[1].strip() if len(line.split("Link page:")) > 1 else ""

        elif line.startswith("Dang follow:"):
            data_dict["Dang follow"] = line.split("Dang follow:")[1].strip() if len(line.split("Dang follow:")) > 1 else ""

        elif line.startswith("Follow:"):
            data_dict["Follow"] = line.split("Follow:")[1].strip() if len(line.split("Follow:")) > 1 else ""

        elif line.startswith("Thích:"):
            data_dict["Thích"] = line.split("Thích:")[1].strip() if len(line.split("Thích:")) > 1 else ""

    # Thêm phần cuối cùng nếu có dữ liệu chưa thêm vào
    if data_dict["Ngày"]:
        data_page.append(data_dict)

    # Chuyển danh sách dictionary thành DataFrame
    df = pd.DataFrame(data_page)

    # Xử lý điền giá trị thiếu cho cột 'Hệ kênh'
    df['Hệ kênh'] = df['Hệ kênh'].ffill()


    # Chuyển đổi các cột số thành kiểu int
    df['Dang follow'] = df['Dang follow'].apply(convert_to_number).fillna(0).astype(int)
    df['Follow'] = df['Follow'].apply(convert_to_number).fillna(0).astype(int)
    df['Thích'] = df['Thích'].apply(convert_to_number).fillna(0).astype(int)

    return df


def convert_datetime_to_str(data):
    """Chuyển đổi các đối tượng datetime trong data sang chuỗi định dạng %m-%d-%Y để có thể JSON serialize."""
    if isinstance(data, dict):
        return {key: convert_datetime_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_datetime_to_str(item) for item in data]
    elif isinstance(data, datetime):
        return data.strftime('%m-%d-%Y')  # Định dạng chuỗi ngày tháng theo kiểu %m-%d-%Y
    else:
        return data
    
def save_file(data, Type_data): 
    # Tạo DataFrame từ dữ liệu
    df = pd.DataFrame(data)
    
    if 'Ngày đăng' in df.columns:
        df['Ngày đăng'] = pd.to_datetime(df['Ngày đăng']).dt.date  # Chỉ lấy ngày, bỏ phần giờ
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
        file_name = f"post_tiktok_{current_date}.xlsx"
    elif Type_data.lower() == "page":
        file_name = f"page_tiktok_{current_date}.xlsx"
    else:
        # Trường hợp không khớp với "post" hoặc "page", tạo tên file mặc định
        file_name = f"data_tiktok_{current_date}.xlsx"
    
    # Đường dẫn đầy đủ tới file (trong cùng thư mục)
    file_path = os.path.join(folder_path, file_name)
    
    # Lưu DataFrame dưới dạng file Excel
    df.to_excel(file_path, index=False)
    
    return f"File đã được lưu với tên: {file_path}"

def remove_illegal_characters(text):
    """Loại bỏ các ký tự điều khiển không hợp lệ cho Excel."""
    if isinstance(text, str):
        # Loại bỏ các ký tự điều khiển như backspace (\b)
        return re.sub(r"[\x00-\x1F\x7F-\x9F]", "", text)
    return text

def convert_to_dataframe_data_video_txt(file_path):
    """
    Đọc dữ liệu từ file .txt, mỗi dòng là một chuỗi JSON,
    và chuyển đổi thành DataFrame để phân tích và xử lý.
    """
    # Đọc nội dung file .txt
    with open(file_path, 'r', encoding='utf-8') as file:
        data_lines = file.readlines()

    # Chuyển đổi từng dòng JSON thành dictionary
    data_page = []
    for line in data_lines:
        try:
            data_dict = json.loads(line)  # Chuyển chuỗi JSON thành dictionary
            data_page.append(data_dict)
        except json.JSONDecodeError as e:
            print(f"Lỗi khi phân tích dòng: {line}\nLỗi: {e}")

    # Tạo DataFrame từ danh sách các dictionary
    df = pd.DataFrame(data_page)

    # Đảm bảo tất cả các cột cần thiết đều tồn tại
    required_columns = [
        'Url', 'Ngày đăng', 'Lượt xem', 'Tym', 'Share', 
        'Save', 'Tổng số comment', 'Content', 'Ghim', 
        'Thời gian video', 'Tên Page', 'Hashtags'
    ]
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Áp dụng hàm làm sạch cho cột Content
    df['Content'] = df['Content'].map(remove_illegal_characters)

    # Chuyển các cột số sang kiểu numeric
    numeric_columns = ['Lượt xem', 'Tym', 'Share', 'Save', 'Tổng số comment']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Xử lý các giá trị NaN và gán giá trị mặc định
    df['Content'] = df['Content'].fillna("")
    df['Ghim'] = df['Ghim'].fillna(False)
    df['Thời gian video'] = df['Thời gian video'].fillna("")
    df['Tên Page'] = df['Tên Page'].fillna("")

    # Xử lý cột 'Hashtags' nếu nó là danh sách
    df['Hashtags'] = df['Hashtags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else "").fillna("")

    return df

def setup_driver():
    """
    Thiết lập Selenium WebDriver.
    """
    chrome_options = Options()
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

async def setup_async_driver():
    driver = await setup_driver()  # Giả định setup_driver() hỗ trợ async
    return driver

# Hàm mở link và thu thập dữ liệu từ link
def fetch_data_from_link_sync(driver, link, view_video, is_ghim):
    """
    Hàm đồng bộ dùng Selenium để mở link và thu thập dữ liệu với chờ tải chính xác.
    """
    try:
        driver.get(link)

        # Chờ phần tử chính được tải hoàn toàn
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )

        # Đợi thêm một chút để đảm bảo dữ liệu động đã được tải
        time.sleep(0.5)

        data_post = collect_data_from_link(driver, link, view_video, is_ghim)
        return data_post
    except Exception as e:
        print(f"Lỗi khi mở link {link}: {e}")
        return None
    
# async def process_video_data(semaphore, driver, key, value, start_time, end_time):
#     """
#     Hàm bất đồng bộ để xử lý dữ liệu video.
#     """
#     async with semaphore:  # Giới hạn số tác vụ đồng thời
#         data_posts = []
#         video_links = value['video_link']
#         views = value['view']
#         ghim = value['ghim']

#         for link, view, is_ghim in zip(video_links, views, ghim):
#             view_video = chuan_hoa_luot_thich(view)

#             # Chạy hàm đồng bộ trong thread để không chặn vòng lặp sự kiện
#             data_post = await asyncio.to_thread(
#                 fetch_data_from_link_sync, driver, link, view_video, is_ghim
#             )

#             if not data_post:
#                 continue

#             post_date = data_post.get("Ngày đăng")

#             if post_date is None or not isinstance(post_date, datetime):
#                 continue

#             if (post_date < start_time) and not is_ghim:
#                 break

#             if is_ghim and (post_date < start_time or post_date > end_time):
#                 continue

#             if post_date < start_time or post_date > end_time:
#                 continue

#             data_posts.append(convert_datetime_to_str(data_post))

#         return data_posts



# async def main_async(driver, dict_data_path, output_file, start_time, end_time):
#     """
#     Hàm chính để thu thập dữ liệu từ nhiều liên kết video.
#     """
#     dict_Data = {}
#     try:
#         with open(dict_data_path, 'r', encoding='utf-8') as f:
#             dict_Data = json.load(f)
#     except FileNotFoundError:
#         print("File dict_data.json chưa tồn tại.")
#         return

#     all_data = []
#     semaphore = asyncio.Semaphore(10)  # Giới hạn tối đa 10 tác vụ đồng thời

#     # Tạo và chạy các tác vụ
#     tasks = [
#         process_video_data(semaphore, driver, key, value, start_time, end_time)
#         for key, value in dict_Data.items()
#     ]
#     results = await asyncio.gather(*tasks)

#     # Gom tất cả kết quả
#     for result in results:
#         if result:
#             all_data.extend(result)

#     # Ghi tất cả dữ liệu vào file sau khi hoàn tất
#     with open(output_file, 'a', encoding='utf-8') as file:
#         for data in all_data:
#             file.write(json.dumps(data, ensure_ascii=False) + '\n')


class DriverPool:
    def __init__(self, size=3):
        self.drivers = []
        self.size = size
        self.lock = asyncio.Lock()
        
    async def get_driver(self):
        async with self.lock:
            if not self.drivers:
                if len(self.drivers) < self.size:
                    driver = setup_driver()
                    return driver
                else:
                    # Đợi cho đến khi có driver khả dụng
                    await asyncio.sleep(1)
                    return await self.get_driver()
            return self.drivers.pop()
            
    async def return_driver(self, driver):
        async with self.lock:
            self.drivers.append(driver)
            
    def cleanup(self):
        for driver in self.drivers:
            try:
                driver.quit()
            except:
                pass

async def process_video_data(semaphore, driver_pool, key, value, start_time, end_time):
    async with semaphore:
        driver = await driver_pool.get_driver()
        try:
            data_posts = []
            video_links = value['video_link']
            views = value['view']
            ghim = value['ghim']

            for link, view, is_ghim in zip(video_links, views, ghim):
                view_video = chuan_hoa_luot_thich(view)
                
                data_post = await asyncio.to_thread(
                    fetch_data_from_link_sync, driver, link, view_video, is_ghim
                )
                
                if not data_post:
                    continue

                post_date = data_post.get("Ngày đăng")
                
                if post_date is None or not isinstance(post_date, datetime):
                    continue

                if (post_date < start_time) and not is_ghim:
                    break

                if is_ghim and (post_date < start_time or post_date > end_time):
                    continue

                if post_date < start_time or post_date > end_time:
                    continue

                data_posts.append(convert_datetime_to_str(data_post))
                
            return data_posts
        finally:
            await driver_pool.return_driver(driver)

async def main_async(dict_data_path, output_file, start_time, end_time):
    driver_pool = DriverPool(size=3)  # Tạo pool với 3 driver
    dict_Data = {}
    
    try:
        with open(dict_data_path, 'r', encoding='utf-8') as f:
            dict_Data = json.load(f)
    except FileNotFoundError:
        print("File dict_data.json chưa tồn tại.")
        return

    semaphore = asyncio.Semaphore(5)
    tasks = [
        process_video_data(semaphore, driver_pool, key, value, start_time, end_time)
        for key, value in dict_Data.items()
    ]
    
    try:
        results = await asyncio.gather(*tasks)
        
        all_data = []
        for result in results:
            if result:
                all_data.extend(result)
                
        with open(output_file, 'a', encoding='utf-8') as file:
            for data in all_data:
                file.write(json.dumps(data, ensure_ascii=False) + '\n')
    finally:
        driver_pool.cleanup()