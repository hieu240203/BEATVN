import re
import os
import isodate
import requests
import pandas as pd
from datetime import datetime
import google_auth_oauthlib.flow
from googleapiclient.discovery import *
import googleapiclient.errors
from googleapiclient.discovery import build


def extract_contact_info(text):
    # Mẫu regex cho các loại thông tin
    patterns = {
        "Facebook": r"(https?://(?:www\.)?facebook\.com/[^\s]+)",
        "Email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        "Phone": r"\b\d{9,11}\b",
        "Instagram": r"(https?://(?:www\.)?instagram\.com/[^\s]+)",
    }

    # Khởi tạo dictionary chứa thông tin được trích xuất
    extracted_data = {key: [] for key in patterns}
    extracted_data['content'] = []  # Để lưu trữ phần còn lại không thuộc các nhóm trên

    # Trích xuất từng loại thông tin theo regex
    for key, pattern in patterns.items():
        matches = re.findall(pattern, text)
        if matches:
            extracted_data[key].extend(matches)

    # Xóa các phần đã trích xuất từ text để lấy phần còn lại
    for key, pattern in patterns.items():
        text = re.sub(pattern, '', text)

    # Loại bỏ số điện thoại sau khi xử lý
    text = re.sub(r"\b\d{9,11}\b", '', text)

    # Loại bỏ các từ khóa "Facebook:", "Email:", "Số điện thoại:"
    text = re.sub(r"\bFacebook:|\bEmail:|\bSố điện thoại:", '', text)

    # Thêm phần nội dung còn lại vào mục content
    remaining_content = text.strip()
    if remaining_content:
        extracted_data['content'].append(remaining_content)

    return extracted_data


def get_channel_states(youtube, channel_id, he_kenh):
    # Tạo yêu cầu API để lấy thông tin kênh
    request = youtube.channels().list(
        part="contentDetails,snippet,statistics",  # Đảm bảo phần chính xác
        id=channel_id  # Sửa lỗi tên biến
    )
    # Thực thi yêu cầu và nhận phản hồi
    response = request.execute()
    
    # Lấy dữ liệu từ response
    channel_data = response['items'][0]
    
    # Lấy thông tin liên hệ từ phần mô tả
    description = channel_data['snippet'].get('description', '')
    contact_info = extract_contact_info(description)

    # Sử dụng join để loại bỏ dấu ngoặc vuông
    content = ' '.join(contact_info.get('content', ['']))

    # Chuẩn bị dữ liệu
    custom_url = channel_data['snippet'].get('customUrl', "")
    if custom_url:
        channel_link = f"https://www.youtube.com/{custom_url}"
    else:
        channel_link = ""
    
    short_video_id = channel_data['id'].replace("UC", "UUSH", 1)
    live_video_id = channel_data['id'].replace("UC", "UULV", 1)
    
    # Chuẩn bị dữ liệu và kiểm tra nếu có phần tử trong danh sách trước khi lấy phần tử đầu tiên
    data = {
        "Hệ Kênh" : he_kenh ,
        "Channel Link": channel_link, 
        "Name Channel": channel_data['snippet']['title'],
        "Total View": channel_data['statistics']['viewCount'],
        "Total Sub": channel_data['statistics']['subscriberCount'],
        "Video count": channel_data['statistics']['videoCount'],
        "Date get data": datetime.now().strftime('%m-%d-%Y'),
        "Content": content, 
        "Email": contact_info['Email'][0] if contact_info['Email'] else '',  # Kiểm tra trước khi lấy email
        "Facebook": contact_info['Facebook'][0] if contact_info['Facebook'] else '',  # Kiểm tra trước khi lấy Facebook
        "Instagram": contact_info['Instagram'][0] if contact_info['Instagram'] else '',  # Kiểm tra trước khi lấy Instagram
        "Phone number": contact_info['Phone'][0] if contact_info['Phone'] else '',  # Kiểm tra trước khi lấy số điện thoại
        "Playlist video id": channel_data['contentDetails']['relatedPlaylists']['uploads'],
        "Playlist short id": short_video_id,  # Thường không có thông tin trực tiếp cho short video
        "playlist live id": live_video_id,  # Tùy kênh có hoặc không có phát trực tiếp
    }

    return data


def get_video_ids(youtube, playlist_id):
    video_ids = []  # Danh sách để lưu trữ video ids
    next_page_token = None

    while True:
        # Gửi yêu cầu lấy danh sách video từ playlist với token trang tiếp theo (nếu có)
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        # Lặp qua các video trong response và thêm videoId vào danh sách
        new_video_ids = [item['contentDetails']['videoId'] for item in response['items']]
        video_ids.extend(new_video_ids)

        # In ra các videoId sau mỗi lần lấy

        # Kiểm tra nếu còn trang tiếp theo
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break  # Không còn trang tiếp theo thì thoát vòng lặp

    return video_ids

def get_custom_date_range(start_time_str, end_time_str):
    start_time = datetime.strptime(start_time_str, '%Y-%m-%d')  # Chuyển đổi chuỗi ngày bắt đầu
    end_time = datetime.strptime(end_time_str, '%Y-%m-%d')  # Chuyển đổi chuỗi ngày kết thúc
    return start_time.strftime('%Y-%m-%d'), end_time.strftime('%Y-%m-%d')

def get_video_details(youtube, video_ids, start_time, end_time, video_type):
    all_video_stats = []

    # Chuyển đổi start_time và end_time từ chuỗi ngày thành datetime để so sánh
    start_time_dt = datetime.strptime(start_time, '%Y-%m-%d')
    end_time_dt = datetime.strptime(end_time, '%Y-%m-%d')

    # Lấy ngày hiện tại với định dạng chính xác
    current_date = datetime.now().strftime('%m-%d-%Y') 

    for i in range(0, len(video_ids), 50): 
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails,status",  # Thêm contentDetails để lấy thời lượng video
            id=','.join(video_ids[i : i + 50])
        )
        response = request.execute()
        
        for video in response.get('items', []):
            # Lấy giá trị 'Published At'
            published_at = video['snippet'].get('publishedAt')
            
            # Tách ngày và giờ từ chuỗi 'Published At'
            date, time = published_at.split('T')
            
            # Chuyển đổi ngày từ định dạng '%Y-%m-%d' thành '%m-%d-%Y'
            published_date_dt = datetime.strptime(date, '%Y-%m-%d').strftime('%m-%d-%Y')
            
            # So sánh với start_time và end_time (vẫn giữ định dạng datetime để so sánh)
            if start_time_dt <= datetime.strptime(date, '%Y-%m-%d') <= end_time_dt:
                # Lấy thời gian của video (ISO 8601) và chuyển đổi sang giây hoặc định dạng phù hợp
                duration_iso = video['contentDetails'].get('duration')

                if duration_iso:  # Kiểm tra nếu duration_iso không phải là None
                    try:
                        duration = isodate.parse_duration(duration_iso)
                        duration_seconds = int(duration.total_seconds())  # Chuyển đổi thành giây
                    except Exception as e:  # Bắt lỗi nếu có bất kỳ ngoại lệ nào trong quá trình parse
                        print(f"Lỗi khi xử lý thời lượng video: {e}")
                        duration_seconds = 0  # Đặt giá trị mặc định nếu gặp lỗi
                else:
                    duration_seconds = 0  # Giá trị mặc định nếu không có thời lượng video

                # Tạo URL video
                video_url = f"https://www.youtube.com/watch?v={video.get('id')}"

                # Chỉ lấy các trường dữ liệu cần thiết nếu video nằm trong khoảng thời gian yêu cầu
                essential_details = {
                    "Video URL": video_url,  # URL của video
                    "Title": video['snippet'].get('title'),
                    "Published Date": published_date_dt,  # Ngày phát hành
                    "View Count": video['statistics'].get('viewCount'),
                    "Like Count": video['statistics'].get('likeCount'),
                    "Comment Count": video['statistics'].get('commentCount'),
                    "Channel Title": video['snippet'].get('channelTitle'),
                    "Ngày save" : current_date , 
                }
                all_video_stats.append(essential_details)

    return all_video_stats

def youtube_build(api_service_name, api_version, API_KEY):
    youtube = build(api_service_name, api_version, developerKey=API_KEY)
    return youtube

def save_file(data, Type_data): 
    # Tạo DataFrame từ dữ liệu
    df = pd.DataFrame(data)
    
    # Lấy ngày hiện tại và định dạng thành chuỗi MM-DD-YYYY
    current_date = datetime.now().strftime('%m-%d-%Y')  # Sửa định dạng
    
    # Đường dẫn thư mục chính để lưu file
    base_dir = r"D:\BeatVn\Data"  # Chỉ định thư mục gốc

    # Tạo thư mục theo ngày, ví dụ: Data_10-10-2024
    folder_name = f"Data_{current_date}"
    folder_path = os.path.join(base_dir, folder_name)

    # Kiểm tra và tạo thư mục nếu chưa tồn tại
    os.makedirs(folder_path, exist_ok=True)
    
    # Kiểm tra dạng dữ liệu và tạo tên file phù hợp
    if Type_data.lower() == "page_youtube":
        file_name = f"page_youtube_{current_date}.xlsx"
    elif Type_data.lower() == "post_youtube":
        file_name = f"post_youtube_{current_date}.xlsx"
    else:
        # Trường hợp không khớp với "page_youtube" hoặc "post_youtube", tạo tên file mặc định
        file_name = f"data_{current_date}.xlsx"
    
    # Đường dẫn đầy đủ tới file
    file_path = os.path.join(folder_path, file_name)
    
    # Lưu DataFrame dưới dạng file Excel
    df.to_excel(file_path, index=False)
    
    return f"File đã được lưu với tên: {file_path}"
