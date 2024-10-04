import os
from datetime import datetime
class Config:
    def __init__(self):
        self.ipServer = ""
        self.dbName = ""
        self.factory = ""
        self.line = ""
        self.projectName = ""
        self.owner = ""
        self.timeUpdate = '0'
        self.totalMachine = '0'
        self.chamberMonitor = ""
        self.chamberMonitorQUA = ""
        self.UPH = ""


    def writeLog(content):
        try:
            direction_path = ".\\LogFile"
            if not os.path.exists(direction_path):
                os.makedirs(direction_path)
            
            # Tên file log với định dạng ngày hiện tại
            file_path = os.path.join(direction_path, datetime.now().strftime("%Y%m%d") + ".txt")
            
            # Kiểm tra xem file đã tồn tại chưa
            if not os.path.exists(file_path):
                # Ghi nội dung vào file mới
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(content)
            else:
                # Thêm nội dung vào file đã tồn tại
                with open(file_path, 'a', encoding='utf-8') as file:
                    file.write("\n" + content)
        except Exception as e:
            print(f"Lỗi ghi log: {e}")

