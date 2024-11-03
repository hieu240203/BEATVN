from My_functions import *

API_Key = "AIzaSyDZNWGO3CSMlyyn4kSPBe6Av3nw-HmaOp4"
Channel_id = "UCRsd_L7wBGdHLhfacuy3QVw"
api_service_name = "youtube"
api_version = "v3"


data = pd.read_excel("D:\BeatVn\youtube\Link_youtube.xlsx")

start_time_str = "2024-7-1" # ngày bắt đầu lấy dữ liệu
end_time_str = "2024-10-21  " # ngày kết thúc lấy dữ liệu

Data_channel = []

Data_video = []

# Channel_ids = ["UCxVbAgOxi2HtYFmptzvILrA", #YEAH1TV
#                "UCI2OiZs5aVcyBUBVsgovzng", #YEAH1 MUSIC
#                "UCk9ft9Cy-uh0I2goL48KASg", #YEAH1 MOVIE
#                 "UCh_zF2FsiCflCPgYDudtcqg", #YEAH1 SHOW
#                 "UCI1MEY35cpjdRMnxSc3dp2w", #Yeah1 Kids
#                 "UC8FP0dPlPBtybikzPvxChTA", # Lẩu phim
#                 "UC-gfYPmX-4hagkRmUF5YxTA", #cháo trắng
#                 "UCsdRS7fVfJqkXFsyOiC79og", #New Films
#                 "UCp-Gej7X1tvN8lqomkceoyg", # Động tình
#                 "UCq6ApdQI0roaprMAY1gZTgw", #nghiền mì gõ
#                 "UC_np0YZuvNMIJceE6jHeggQ", #La La school
#                 "UCugWlzSUX_84r2bT6oQOrNg", #VTC channel
#                 "UC0So2-fnCVOoxSPbD4ENO1g", #Review xe
#                 "UC7oBMOWrupYlMDEAEdTAinQ", #THEANH28 FUNFACT 
#                 "UCOlrILECzfqGY58-KB0avew",#THEANH28 TV
#                 "UC_STX9XSCdObBUtY-65dw6A", #Kiến không ngủ
#                 "UCglso25k7TGFtTng0_42Xfw", #HÓNG TO THE HEART
#                 "UC7H8tmJSSn-N3I1IDPXZPxQ", #Inside The Box
#                 "UCKSITbCnHEhqFMwVlB1XKaw", #BEAT NOW
#                 "UCK1Y8uFq5cSobX1fOCW3EzA", #SHOWBEAT
#                 "UCyaT8xqIJZ8jT0iiaOpDX_A", #Nhà Văn Hoá
#                 "UCKzd7f0hakViEL9wmiyh7yQ", #KLTN
#                ]

Channel_ids = data["Channel Ids"]

youtube = youtube_build(api_service_name, api_version, API_Key )

for i in range(len(Channel_ids)): 
    print(Channel_ids[i] )
    Channel_states = get_channel_states(youtube, Channel_ids[i] , data['Hệ Network'][i])

    Data_channel.append(Channel_states)

save_file(Data_channel, "page_youtube")

start_time, end_time = get_custom_date_range(start_time_str, end_time_str)

# Khởi tạo danh sách để lưu các thông tin lỗi

for channel in Data_channel:
    # Khởi tạo các biến để lưu dữ liệu video
    video_Normal = []
    video_short = []
    video_live = []

    # Lấy video thông thường (Normal)
    try:
        Video_Bt_id = channel['Playlist video id']
        if Video_Bt_id:  # Kiểm tra nếu tồn tại danh sách phát video thường
            video_normal_ids = get_video_ids(youtube, Video_Bt_id)
            video_Normal = get_video_details(youtube, video_normal_ids, start_time, end_time, "Normal")
    except googleapiclient.errors.HttpError as e:
        if e.resp.status == 404:
            print(f"Đã xảy ra lỗi 404 khi lấy video thường cho playlist: {Video_Bt_id}")
        else:
            print(f"Đã xảy ra lỗi khác khi lấy video thường: {e}")

    # Lấy video ngắn (Short)
    try:
        Video_short_id = channel.get('Playlist short id')  # Sử dụng .get để tránh lỗi nếu không tồn tại
        if Video_short_id:  # Kiểm tra nếu tồn tại danh sách phát video ngắn
            video_short_ids = get_video_ids(youtube, Video_short_id)
            video_short = get_video_details(youtube, video_short_ids, start_time, end_time, "short")
    except googleapiclient.errors.HttpError as e:
        if e.resp.status == 404:
            print(f"Đã xảy ra lỗi 404 khi lấy video ngắn cho playlist: {Video_short_id}")
        else:
            print(f"Đã xảy ra lỗi khác khi lấy video ngắn: {e}")

    # Lấy video phát trực tiếp (Live)
    try:
        Video_live_id = channel.get('playlist live id')  # Sử dụng .get để tránh lỗi nếu không tồn tại
        if Video_live_id:  # Kiểm tra nếu tồn tại danh sách phát video phát trực tiếp
            video_live_ids = get_video_ids(youtube, Video_live_id)
            video_live = get_video_details(youtube, video_live_ids, start_time, end_time, "live")
        else:
            print("Kênh không có danh sách phát video phát trực tiếp.")
    except googleapiclient.errors.HttpError as e:
        if e.resp.status == 404:
            print(f"Đã xảy ra lỗi 404 khi lấy video phát trực tiếp cho playlist: {Video_live_id}")
        else:
            print(f"Đã xảy ra lỗi khác khi lấy video phát trực tiếp: {e}")

    # Kiểm tra và thêm các video đã lấy được vào Data_video
    if video_Normal:
        Data_video.extend(video_Normal)
    if video_short:
        Data_video.extend(video_short)
    if video_live:
        Data_video.extend(video_live)


save_file(Data_video,"post_youtube")

