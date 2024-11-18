import json
import mysql.connector
from mysql.connector import Error
import os
import math

def create_database():
    """Tạo database nếu chưa tồn tại"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password=''
        )
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS CrawlingScores")
        print("Đã tạo database 'CrawlingScores' (nếu chưa tồn tại)")
    except Error as e:
        print(f"Lỗi khi tạo database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def connect_mysql():
    """Kết nối đến MySQL database"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='CrawlingScores'
        )
        if connection.is_connected():
            print("Kết nối MySQL thành công!")
            return connection
    except Error as e:
        print(f"Lỗi khi kết nối MySQL: {e}")
        return None

def create_subject_table(connection, subject_code):
    """Tạo bảng cho môn học trong database"""
    try:
        cursor = connection.cursor()
        table_name = f"{subject_code.lower()}"
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        print(f"Đang tạo bảng {table_name}...")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                class_code VARCHAR(20),
                STT FLOAT,
                ma_sv VARCHAR(20),
                lop_sv VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                ho_lot VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                ten VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                ngay_sinh VARCHAR(20),
                diem_kiem_tra_hs1 FLOAT,
                diem_thi_ket_thuc_hoc_phan FLOAT,
                diem_hp FLOAT,
                ghi_chu VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                UNIQUE KEY unique_student_class (ma_sv, class_code)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        connection.commit()
        print(f"Tạo bảng {table_name} thành công!")
    except Error as e:
        print(f"Lỗi khi tạo bảng: {e}")

def is_nan(value):
    """Kiểm tra giá trị có phải NaN không"""
    if isinstance(value, float):
        return math.isnan(value)
    return False

def import_data_from_json(json_file_path, connection, subject_code):
    """Import dữ liệu từ file JSON vào MySQL"""
    try:
        class_code = os.path.basename(json_file_path).split('COMP')[0]
        
        print(f"Đang đọc file {json_file_path}...")
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        cursor = connection.cursor()
        total_students = len(data)
        imported_students = 0
        table_name = f"{subject_code.lower()}"
        
        for student in data:
            try:
                print(f"Đang import sinh viên {student['Mã SV']} - {student['Họ lót']} {student['Tên']}...")
                
                # Xử lý các giá trị NaN
                diem_kiem_tra = student.get('Điểm kiểm tra (HS1) 1\n(HS 5)')
                diem_kiem_tra = None if is_nan(diem_kiem_tra) else diem_kiem_tra
                
                diem_thi = student.get('Điểm thi kết thúc học phần\n(50%)')
                diem_thi = None if is_nan(diem_thi) else diem_thi
                
                diem_hp = student.get('Điểm HP')
                diem_hp = None if is_nan(diem_hp) else diem_hp
                
                ghi_chu = student.get('Ghi chú')
                ghi_chu = None if is_nan(ghi_chu) else ghi_chu
                
                insert_query = f"""
                    INSERT INTO {table_name}
                    (class_code, STT, ma_sv, lop_sv, ho_lot, ten, ngay_sinh, 
                     diem_kiem_tra_hs1, diem_thi_ket_thuc_hoc_phan, diem_hp, ghi_chu)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    STT = VALUES(STT),
                    lop_sv = VALUES(lop_sv),
                    ho_lot = VALUES(ho_lot),
                    ten = VALUES(ten),
                    ngay_sinh = VALUES(ngay_sinh),
                    diem_kiem_tra_hs1 = VALUES(diem_kiem_tra_hs1),
                    diem_thi_ket_thuc_hoc_phan = VALUES(diem_thi_ket_thuc_hoc_phan),
                    diem_hp = VALUES(diem_hp),
                    ghi_chu = VALUES(ghi_chu)
                """
                
                values = (
                    class_code,
                    float(student['STT']) if not is_nan(student.get('STT')) else None,
                    student['Mã SV'],
                    student['Lớp SV'],
                    student['Họ lót'],
                    student['Tên'],
                    student['Ngày sinh'],
                    float(diem_kiem_tra) if diem_kiem_tra is not None else None,
                    float(diem_thi) if diem_thi is not None else None,
                    float(diem_hp) if diem_hp is not None else None,
                    ghi_chu
                )
                
                cursor.execute(insert_query, values)
                imported_students += 1
                print(f"Tiến độ: {imported_students}/{total_students} sinh viên")
                
            except Error as e:
                print(f"Lỗi khi import sinh viên {student['Mã SV']}: {e}")
                continue
        
        connection.commit()
        print(f"\nImport dữ liệu từ file {json_file_path} thành công!")
        
    except Error as e:
        print(f"Lỗi khi import dữ liệu: {e}")
    except json.JSONDecodeError as e:
        print(f"Lỗi khi đọc file JSON: {e}")
    except Exception as e:
        print(f"Lỗi không xác định: {e}")
        print("Type:", type(e))
        print("Args:", e.args)

def main():
    print("Bắt đầu quá trình import dữ liệu...")
    create_database()
    connection = connect_mysql()
    if connection is None:
        return
    
    try:
        subject_code = "comp1701"
        create_subject_table(connection, subject_code)
        
        json_files = [
            f for f in os.listdir('.') 
            if f.endswith('.json') 
            and ('COMP170101' in f or 'COMP170102' in f)
            and any(class_code in f for class_code in ['2121','2221', ])
        ]
        
        print("\nCác file sẽ được import:")
        for file in sorted(json_files):
            print(f"- {file}")
            
        for json_file in sorted(json_files):
            import_data_from_json(json_file, connection, subject_code)
        
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {subject_code}")
        total_students = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT class_code, COUNT(*) FROM {subject_code} GROUP BY class_code ORDER BY class_code")
        class_counts = cursor.fetchall()
        
        print(f"\nThống kê cuối cùng:")
        print(f"- Tổng số sinh viên đã import: {total_students}")
        print("\nPhân bố theo lớp:")
        for class_code, count in class_counts:
            print(f"- Lớp {class_code}: {count} sinh viên")
        
    finally:
        if connection.is_connected():
            connection.close()
            print("\nĐã đóng kết nối MySQL")

if __name__ == "__main__":
    main()