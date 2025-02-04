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
            file_path = os.path.join(direction_path, datetime.now().strftime("%Y%m%d") + ".txt")
            if not os.path.exists(file_path):
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(content)
            else:
                with open(file_path, 'a', encoding='utf-8') as file:
                    file.write("\n" + content)
        except Exception as e:
            print(f"Lỗi ghi log: {e}")

