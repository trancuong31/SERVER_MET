import pymcprotocol
import json, datetime, time
# pymc3e = pymcprotocol.Type3E()
# pymc3e.connect("192.168.1.10", 9999)
# pymc3e.batchwrite_wordunits(headdevice="D10", values=[0, 10, 20, 30, 40])
# wordunits_values = pymc3e.batchread_wordunits(headdevice="D10", readsize=10)
# listPLC=clMap()
# try:
#     with open('data.json', 'r') as file:
#         data = json.load(file)
#     print(data['ipServer'])
# except FileNotFoundError:
#     data = {}  # If file doesn't exist, create an empty dictionary
#  {"ip": "192.168.1.25", "port": 9914, "nameMachine":"LB2" , "typeMachine":"ASSEMBLY"}
# ['secrets','asyncio','uuid','cryptography.hazmat.primitives.kdf.pbkdf2'],
# pyinstaller --onefile Main_UI.py
# pyinstaller Main_UI.spec
#pyinstaller --noconsole --onefile Main_UI.py
#,{"ip": "192.168.1.25", "port": 9914, "nameMachine":"LB2" , "typeMachine":"ASSEMBLY"}
# import datetime
# import time
from pymcprotocol import Type3E

# def connect_with_retry(pymc3e, ip, port, retry_delay=5):
#     pymc3e.connect(ip, port)
#     print("Kết nối thành công với PLC.")
# user="pthnew",
# password="pthnew",
# dsn="10.228.114.170:3333/meorcl"
def main():
    pymc3e = Type3E()
    ip = '192.168.1.10'
    port = 9999
    # Thực hiện kết nối đến PLC
    pymc3e.connect(ip, port)

    while True:
        pymc3e.connect(ip, port)
        current_time = datetime.datetime.now()
        bitunits_values = pymc3e.batchread_bitunits(headdevice="L5050", readsize=10)
        print(f'bit : {bitunits_values} ')
        # Kiểm tra và nghỉ giữa các lần đọc
        time.sleep(3)
        work_date = datetime.datetime.now().strftime("%Y-%m-%d %H")
        print(f'{work_date}')
        pymc3e.close()

if __name__ == "__main__":
    main()

# import threading

# def worker():
#     # Đoạn code cần thực hiện đa luồng (ví dụ xử lý số hoặc tải CPU)
#     pass

# threads = []
# for i in range(100):  # Số luồng mong muốn, bạn có thể thử từ nhỏ đến lớn
#     t = threading.Thread(target=worker)
#     threads.append(t)
#     t.start()
# import logging

# logging.debug("This is a debug message")

# logging.info("This is an info message")

# logging.warning("This is a warning message")


# logging.error("This is an error message")


# logging.critical("This is a critical message")
# print('This is a warning message')