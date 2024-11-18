import requests
import json
import os
from datetime import datetime, timedelta
import pandas as pd
import json

AUTH_FILE = "auth.json"
subject="2321CHIN141807"
EXCEL_FILE = "2321CHIN141807.xlsx"


def fetch_new_auth_token():
    url = "https://onlineapi.hcmue.edu.vn/api/authenticate/authpsc"
    payload = {
        "username": "9111",
        "password": "9111"
    }
    headers = {
        "Content-Type": "application/json",
        "Apikey": "hcmuepscRBF0zT2Mqo6vMw69YMOH43IrB2RtXBS0EHit2kzvL2auxaFJBvw==",
        "ClientId": "hcmue"
    }
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        token = data.get("Token")
        
        auth_data = {
            "token": token,
            "expires_at": (datetime.now() + timedelta(hours=1)).isoformat()  
        }
        with open(AUTH_FILE, "w") as f:
            json.dump(auth_data, f)
        
        print("New auth token fetched and saved.")
        return token
    else:
        print("Failed to fetch auth token.")
        return None

# Hàm để lấy mã auth từ file hoặc lấy mới nếu hết hạn
def get_auth_token():
    if os.path.exists(AUTH_FILE):
        with open(AUTH_FILE, "r") as f:
            auth_data = json.load(f)
        
        expires_at = datetime.fromisoformat(auth_data["expires_at"])
        if expires_at > datetime.now():
            print("Using saved auth token.")
            return auth_data["token"]
        else:
            print("Auth token expired, fetching a new one.")
            return fetch_new_auth_token()
    else:
        return fetch_new_auth_token()

def download_report():
    url = "https://onlineapi.hcmue.edu.vn/api/professor/DownLoadReport"
    
    token = get_auth_token()
    
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
        "Apikey": "hcmuepscRBF0zT2Mqo6vMw69YMOH43IrB2RtXBS0EHit2kzvL2auxaFJBvw==",
        "Authorization": f"Bearer {token}",
        "Clientid": "hcmue",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
    }
    data = {
        "typeFile": "excel",
        "typeReport": "1",
        "fileName": f"{subject}.xlsx",
        "p1": subject,
        "p2": "9111"
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        with open(EXCEL_FILE, "wb") as file:
            file.write(response.content)
        print("Report downloaded successfully!")
    else:
        if response.status_code == 401:
            print("Auth token expired, fetching a new one and retrying.")
            token = fetch_new_auth_token()
            headers["Authorization"] = f"Bearer {token}"
            response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 200:
                with open(EXCEL_FILE, "wb") as file:
                    file.write(response.content)
                print("Report downloaded successfully on retry!")
            else:
                print(f"Failed to download report on retry. Status code: {response.status_code}")
        else:
            print(f"Failed to download report. Status code: {response.status_code}")

def xlsxtoJson():
    file_path = '2321CHIN141807.xlsx'  # Thay đổi đường dẫn này thành đường dẫn của file Excel của bạn
    full_df = pd.read_excel(file_path, sheet_name=0, header=None)  # Đọc tất cả các dòng và không gán tiêu đề

    # Tìm dòng chứa "STT"
    start_row = None
    for i, row in full_df.iterrows():
        if row.str.contains("STT", na=False).any():
            start_row = i
            break

    # Kiểm tra nếu tìm thấy dòng chứa "STT"
    if start_row is not None:
        # Đọc lại từ dòng chứa "STT" và đặt dòng đó làm header
        df = pd.read_excel(file_path, sheet_name=0, header=start_row)

        # Lọc các cột liên quan đến bảng điểm, chỉ giữ lại các cột không có "Unnamed"
        score_df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # Xóa các dòng trống hoặc không có giá trị STT để lấy dữ liệu thực
        score_df = score_df.dropna(subset=["STT"])

        # Chuyển đổi dữ liệu thành JSON với định dạng dễ đọc
        json_data = score_df.to_dict(orient="records")
        formatted_json = json.dumps(json_data, indent=4, ensure_ascii=False)

        # Hiển thị kết quả JSON
        print(formatted_json)

        # Nếu bạn muốn lưu JSON vào file
        with open("2321CHIN141807.json", "w", encoding="utf-8") as f:
            f.write(formatted_json)
    else:
        print("Không tìm thấy cột 'STT' trong file Excel.")



download_report()
xlsxtoJson()