import oracledb
import datetime
import threading
class connectDB:
    # Tạo đối tượng lock để đảm bảo an toàn luồng
    lock_DB = threading.Lock()
    def __init__(self):
        self.connection = self.create_connection()
        self.dem = 0
    def create_connection(self):
        try:
            # oracledb.init_oracle_client(lib_dir=r"C:\Users\MET\Downloads\instantclient-basic-windows.x64-23.5.0.24.07 (1)\instantclient_23_5")
            oracledb.init_oracle_client()  # Kiểm tra nếu có Oracle Client sẵn
            print("Oracle Instant Client is available.")
            connection = oracledb.connect(
                user="pthnew",
                password="pthnew",
                dsn="10.228.114.170:3333/meorcl"
                # user="system",
                # password="123456",
                # dsn="localhost:1521/orcl3"
            )
            print('Kết nối thành công SERVER !!')
            return connection
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi kết nối tới cơ sở dữ liệu: {e}")
            return None
        
    # Hàm truy vấn dữ liệu
    def select(self, connection, query, parameters=None):
        if  self.connection is None or not self.is_connection_alive():
            print("Kết nối không khả dụng, đang thử lại...")
            self.connection= self.create_connection()
        cursor = self.connection.cursor()
        try:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            self.dem += 1
            result = cursor.fetchall()
            return result
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi truy vấn dữ liệu: {e}")
            return None
        finally:
            cursor.close()
    # Kiểm tra thử kết nối đến Server bằng 1 query đơn giản
    def is_connection_alive(self):
        try:
            if self.connection is None:
                return False
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")  # Kiểm tra kết nối bằng truy vấn đơn giản
            cursor.fetchone()
            cursor.close()
            return True  # Kết nối còn hoạt động
        except oracledb.DatabaseError as e:
            print(f"Kết nối không khả dụng: {e}")
            return False  # Nếu có lỗi, nghĩa là kết nối không khả dụng

    # Hàm thực thi cácc câu lệnh SQL (INSERT, UPDATE, DELETE)
    def execute_query(self,connection, query, parameters=None):
        if self.connection is None or not self.is_connection_alive():
            print("Kết nối không khả dụng, đang thử lại...")
            self.connection= self.create_connection()
        cursor = self.connection.cursor()
        try:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            self.dem += 1
            # print(f'Số lần truy cập DB: {self.dem}')
            self.connection.commit()
            return True
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi thực thi truy vấn: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def insert_machine_data(self, buffer, uph):
        work_date = datetime.datetime.now().strftime("%Y-%m-%d %H")  # Định dạng thời gian
        try:
            batch_queries = []
            with connectDB.lock_DB:
                for key, values in buffer.items():
                    factory, line, machine_code, hour = key.split(',')
                    machine_no = f"{factory}_{line}_{machine_code}"
                    
                    # Câu lệnh MERGE để kiểm tra và chèn hoặc cập nhật bản ghi
                    merge_query = f"""
                    MERGE INTO CNT_MACHINE_SUMMARY target
                    USING (SELECT '{machine_no}' AS MACHINE_NO, TO_DATE('{work_date}', 'YYYY-MM-DD HH24') AS WORK_DATE FROM dual) source
                    ON (target.MACHINE_NO = source.MACHINE_NO AND target.WORK_DATE = source.WORK_DATE)
                    WHEN MATCHED THEN
                        UPDATE SET
                            RUN_TIME = NVL(target.RUN_TIME, 0) + {values['run_time']},
                            ERROR_TIME = NVL(target.ERROR_TIME, 0) + {values['error_time']},
                            STANDBY_TIME = NVL(target.STANDBY_TIME, 0) + {values['standby_time']},
                            STOP_TIME = NVL(target.STOP_TIME, 0) + {values['stop_time']},
                            OUTPUT = NVL(target.OUTPUT, 0) + {values['output']},
                            THROW_QTY1 = NVL(target.THROW_QTY1, 0) + {values['THROW_QTY1']},
                            THROW_QTY2 = NVL(target.THROW_QTY2, 0) + {values['THROW_QTY2']},
                            PICK_QTY1 = NVL(target.PICK_QTY1, 0) + {values['PICK_QTY1']},
                            PICK_QTY2 = NVL(target.PICK_QTY2, 0) + {values['PICK_QTY2']},
                            THROW_QTY3 = NVL(target.THROW_QTY3, 0) + {values['THROW_QTY3']},
                            THROW_QTY4 = NVL(target.THROW_QTY4, 0) + {values['THROW_QTY4']},
                            PICK_QTY3 = NVL(target.PICK_QTY3, 0) + {values['PICK_QTY3']},
                            PICK_QTY4 = NVL(target.PICK_QTY4, 0) + {values['PICK_QTY4']},
                            NG_QTY = NVL(target.NG_QTY, 0) + {values['NG_QTY']},
                            UPH = {uph}
                    WHEN NOT MATCHED THEN
                        INSERT (MACHINE_NO, WORK_DATE, RUN_TIME, STANDBY_TIME, ERROR_TIME, STOP_TIME, OUTPUT, THROW_QTY1, THROW_QTY2, 
                                PICK_QTY1, PICK_QTY2, THROW_QTY3, THROW_QTY4, PICK_QTY3, PICK_QTY4, NG_QTY, UPH)
                        VALUES ('{machine_no}', TO_DATE('{work_date}', 'YYYY-MM-DD HH24'), {values['run_time']}, {values['standby_time']}, 
                                {values['error_time']}, {values['stop_time']}, {values['output']}, {values['THROW_QTY1']}, {values['THROW_QTY2']}, 
                                {values['PICK_QTY1']}, {values['PICK_QTY2']}, {values['THROW_QTY3']}, {values['THROW_QTY4']}, 
                                {values['PICK_QTY3']}, {values['PICK_QTY4']}, {values['NG_QTY']}, {uph})
                    """
                    batch_queries.append(merge_query)

                # Thực thi tất cả các câu lệnh trong batch
                for query in batch_queries:
                    self.execute_query(self.connection, query)

                self.connection.commit()
                print(f"Batch update thành công máy {machine_no} lên bảng CNT_MACHINE_SUMMARY!")
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False

    def insert_on_time(self, factory, line, machine_code, time_run):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        query = ""
        try:
            with connectDB.lock_DB:
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') = '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        run_time = row[2] 
                        if not run_time:
                            run_time = 0
                        if run_time == "":
                            
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET RUN_TIME = '{time_run}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                           
                            run_time_1 = int(run_time) + int(time_run)
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET RUN_TIME = '{run_time_1}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                   
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, RUN_TIME) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '{time_run}')"
                
                # Thực thi truy vấn
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False

    def insert_error_time(self, factory, line, machine_code, errorTime):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H") 
        query = ""
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                if data and len(data) > 0:
                    for row in data:
                        errorTime1 = row[4]
                        if not errorTime1:
                            errorTime1 = 0
                        if errorTime1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET ERROR_TIME = '{errorTime}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                           
                            run_time_1 = int(errorTime1) + int(errorTime)
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET ERROR_TIME = '{run_time_1}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, ERROR_TIME) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '{errorTime}')"
                
               
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False
        
    def insert_stop_time(self, factory, line, machine_code, timeStop):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H") 
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        stop_time = row[5] 
                        if not stop_time:
                            stop_time = 0
                        if stop_time == "":
                            
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET STOP_TIME = '{timeStop}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                           
                            stop_time_1 = int(stop_time) + int(timeStop)
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET STOP_TIME = '{stop_time_1}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, STOP_TIME) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '{timeStop}')"
                
                # Thực thi truy vấn
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False

    def insert_standby_time(self, factory, line, machine_code, timeStand):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H") 
        query = ""
        try:
            with connectDB.lock_DB: 
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        stand_time = row[3] 
                        if not stand_time:
                            stand_time = 0
                        if stand_time == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET STANDBY_TIME = '{timeStand}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            stand_time_1 = int(stand_time) + int(timeStand)
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET STANDBY_TIME = '{stand_time_1}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, STANDBY_TIME) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '{timeStand}')"
                
                # Thực thi truy vấn
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False

    def insert_production_pass(self, factory, line, machine_code, uph):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H") 
        query = ""
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        output1 = row[6]  
                        if not output1:
                            output1 = 0
                        if output1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET OUTPUT = '5', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            output1_1 = int(output1) + 5
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET OUTPUT = '{output1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, OUTPUT, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '5', '{uph}')"

                # Thực thi truy vấn
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False

    def insert_throw_qty1(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"               
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty1 = row[7]  
                        if throw_qty1 == None or throw_qty1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY1 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty1_1 = int(throw_qty1) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY1 = '{throw_qty1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, THROW_QTY1, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False
    
    def insert_pickup_qty1(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        try:
            with connectDB.lock_DB:
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty1 = row[9]
                        if pick_qty1 == None or pick_qty1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY1 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty1_1 = int(pick_qty1) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY1 = '{pick_qty1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, PICK_QTY1, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def insert_pickup_qty2(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB: 
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty2 = row[10] 
                        if pick_qty2 == None or pick_qty2 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY2 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty2_1 = int(pick_qty2) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY2 = '{pick_qty2_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, PICK_QTY2, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def insert_throw_qty2(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB:
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty2 = row[8] 
                        if throw_qty2 == None or throw_qty2 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY2 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty2_1 = int(throw_qty2) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY2 = '{throw_qty2_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, THROW_QTY2, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def insert_pickup_qty3(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB: 
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty3 = row[13]  
                        if pick_qty3 == None or pick_qty3 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY3 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty3_1 = int(pick_qty3) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY3 = '{pick_qty3_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, PICK_QTY3, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def insert_throw_qty3(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB: 
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty3 = row[11]  
                        if throw_qty3 == None or throw_qty3 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET THROW_QTY3 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty3_1 = int(throw_qty3) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET THROW_QTY3 = '{throw_qty3_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, THROW_QTY3, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def insert_pickup_qty4(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB: 
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty4 = row[14]  
                        if pick_qty4 is None or pick_qty4 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET PICK_QTY4 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty4_1 = int(pick_qty4) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET PICK_QTY4 = '{pick_qty4_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, PICK_QTY4, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False
    
    def insert_throw_qty4(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty4 = row[12]  
                        if throw_qty4 is None or throw_qty4 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET THROW_QTY4 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty4_1 = int(throw_qty4) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET THROW_QTY4 = '{throw_qty4_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, THROW_QTY4, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        
        return False

    def insert_cycle_time(self, factory, line, machine_code, cycle_time, date_time):
        query = ""
        try:
            with connectDB.lock_DB:  # Sử dụng lock để đảm bảo an toàn luồng
                query = f"INSERT INTO CYCLE_TIME_MACHINE (FACTORY, LINE, MACHINE_CODE, CYCLE, DATE_TIME) VALUES ('{factory}', '{line}', '{machine_code}', '{cycle_time}', TO_DATE('{date_time}', 'yyyy/MM/dd HH24:MI:SS'))"
                
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
                    
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")

        return False
   
    def insert_production_fail(self, factory, line, machine_code, uph):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        query = ""
        
        try:
            with connectDB.lock_DB:
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        ngqty1 = row[15] 
                        if ngqty1 is None or ngqty1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET NG_QTY = '1', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            ngqty1_1 = int(ngqty1) + 1
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET NG_QTY = '{ngqty1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, NG_QTY, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '1', '{uph}')"
                
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")

        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
            
        return False

    def update_status(self, factory, line, machine_code, project_name, section_name, uph, db_ip, db_server_name, current_state):
        machine_no = f"{factory}_{line}_{machine_code}"
        
        try:
            with connectDB.lock_DB:
                query = f""" 
                    MERGE INTO CNT_MACHINE_INFO target
                    USING (SELECT '{machine_no}' AS MACHINE_NO, '{factory}' AS BUILDING, '{line}' AS LINE_NAME FROM dual) source
                    ON (target.MACHINE_NO = source.MACHINE_NO AND target.BUILDING = source.BUILDING AND target.LINE_NAME = source.LINE_NAME)
                    
                    WHEN MATCHED THEN
                        UPDATE SET
                            CURRENT_STATE = '{current_state}'
                    
                    WHEN NOT MATCHED THEN
                        INSERT (MACHINE_NO, BUILDING, PROJECT_NAME, SECTION_NAME, LINE_NAME, UPH, DB_IP, DB_SERVER_NAME, CURRENT_STATE)
                        VALUES ('{machine_no}', '{factory}', '{project_name}', '{section_name}', '{line}', '{uph}', '{db_ip}', '{db_server_name}', '{current_state}')
                """
                
                # Thực thi câu lệnh MERGE
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def update_oracle_machine_status(self, factory, line, machine_code, status):
        try:
            with connectDB.lock_DB:
                # Sử dụng MERGE để kết hợp INSERT và UPDATE trong một câu lệnh
                query = f"""
                    MERGE INTO AUTOMATION_STATUS target
                    USING (SELECT '{factory}' AS FACTORY, '{machine_code}' AS MACHINE_CODE, '{line}' AS LINE FROM dual) source
                    ON (target.FACTORY = source.FACTORY AND target.MACHINE_CODE = source.MACHINE_CODE AND target.LINE = source.LINE)
                    
                    WHEN MATCHED THEN
                        UPDATE SET 
                            STATUS = '{status}'
                    
                    WHEN NOT MATCHED THEN
                        INSERT (FACTORY, MACHINE_CODE, LINE, STATUS)
                        VALUES ('{factory}', '{machine_code}', '{line}', '{status}')
                """
                
                # Thực thi câu lệnh MERGE
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def machine_status_update_oracle(self, factory, line, machine_code, status):
        machine_code_full = f"{factory}_{line}_{machine_code}"
        
        try:
            with connectDB.lock_DB:
                query = f"""
                    MERGE INTO STATUS_AUTOMATION target
                    USING (SELECT '{factory}' AS FACTORY, '{machine_code_full}' AS MACHINE_CODE, '{line}' AS LINE FROM dual) source
                    ON (target.FACTORY = source.FACTORY AND target.MACHINE_CODE = source.MACHINE_CODE AND target.LINE = source.LINE)
                    
                    WHEN MATCHED THEN
                        UPDATE SET 
                            STATUS = '{status}'
                    
                    WHEN NOT MATCHED THEN
                        INSERT (FACTORY, MACHINE_CODE, LINE, STATUS)
                        VALUES ('{factory}', '{machine_code_full}', '{line}', '{status}');
                """
                
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def insert_error_timeon(self, factory, line, machine_code, status, error_code, error_name, time_error, owner):
        with connectDB.lock_DB:
            try:
                query = (f"INSERT INTO AUTOMATION_DATA_DETAIL (FACTORY, LINE, MACHINE_CODE, STATUS, ERROR_CODE, ERROR_NAME, TIME_ERROR, OWNER) "
                    f"VALUES ('{factory}', '{line}', '{factory}_{line}_{machine_code}', '{status}', '{error_code}', '{error_name}', "
                    f"TO_DATE('{time_error}', 'yyyy/MM/dd HH24:mi:ss'), '{owner}')")
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
            except Exception as e:
                print(f"ngoại lệ: {str(e)}")
                return False

    def update_error_on(self, factory, line, machine_code, error_code, time_fixed):
        with connectDB.lock_DB:
            try:
                query = (f"UPDATE AUTOMATION_DATA_DETAIL "
                        f"SET TIME_FIXED = TO_DATE('{time_fixed}', 'yyyy/MM/dd HH24:mi:ss'), STATUS = 'OK' "
                        f"WHERE ERROR_CODE = '{error_code}' AND FACTORY = '{factory}' AND LINE = '{line}' "
                        f"AND MACHINE_CODE = '{factory}_{line}_{machine_code}' AND TIME_FIXED IS NULL")
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
            except Exception as e:
                print(f"ngoại lệ: {str(e)}")
                return False
    
    def update_error_on1(self, factory, line, machine_code, error_code):
        with connectDB.lock_DB:
            try:
                query = (f"DELETE FROM AUTOMATION_DATA_DETAIL "
                        f"WHERE ERROR_CODE = '{error_code}' AND FACTORY = '{factory}' AND LINE = '{line}' "
                        f"AND MACHINE_CODE = '{factory}_{line}_{machine_code}' AND TIME_FIXED IS NULL")
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
            except Exception as e:
                print(f"ngoại lệ: {str(e)}")
                return False

    def cnt_insert_error_timeon(self, factory, line, machine_code, project_name, section_name, error_id, error_code, time_error):
        with connectDB.lock_DB:
            try:
                query = (f"INSERT INTO CNT_MACHINE_ERROR_RECORD (MACHINE_NO, PROJECT_NAME, SECTION_NAME, ERROR_ID, ERROR_CODE, START_TIME) "
                    f"VALUES ('{factory}_{line}_{machine_code}', '{project_name}', '{section_name}', {error_id}, '{error_code}', "
                    f"TO_DATE('{time_error}', 'yyyy/MM/dd HH24:mi:ss'))")
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
            except Exception as e:
                print(f"ngoại lệ: {str(e)}")
                return False
            
    def cnt_update_error_on(self, factory, line, machine_code, error_code, time_fixed):
        with connectDB.lock_DB:
            try:
                query = (f"UPDATE CNT_MACHINE_ERROR_RECORD "
                        f"SET END_TIME = TO_DATE('{time_fixed}', 'yyyy/MM/dd HH24:mi:ss') "
                        f"WHERE MACHINE_NO = '{factory}_{line}_{machine_code}' "
                        f"AND ERROR_CODE = '{error_code}' AND END_TIME IS NULL")
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
            except Exception as e:
                print(f"ngoại lệ: {str(e)}")
                return False
    
    def cnt_update_error_on1(self, factory, line, machine_code, error_code):
        with connectDB.lock_DB:
            try:
                query = (f"DELETE FROM CNT_MACHINE_ERROR_RECORD "
                        f"WHERE MACHINE_NO = '{factory}_{line}_{machine_code}' "
                        f"AND ERROR_CODE = '{error_code}' AND END_TIME IS NULL")
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
            except Exception as e:
                print(f"ngoại lệ: {str(e)}")
                return False

    def insert_error_code_info(self, factory, line, machine_code, project_name, section_name, error_id, error_code):
        sql = ""
        try:
            with self.lock_DB:
                query = f"INSERT INTO CNT_MACHINE_ERRORCODE_INFO (MACHINE_NO, PROJECT_NAME, SECTION_NAME, ERROR_ID, ERROR_CODE) " \
                    f"VALUES ('{factory}_{line}_{machine_code}', '{project_name}', '{section_name}', {error_id}, '{error_code}')"   
                try:
                    connectDB.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
            return False

    def insert_production1(self, factory, line, machine_code, status, date_time):
            query = ""
            try:
                with connectDB.lock_DB:
                    query = (f"INSERT INTO PRODUCTION_QUANTITY_FPY (FACTORY, LINE, MACHINE_CODE, STATUS, DATE_TIME) "
                        f"VALUES ('{factory}', '{line}', '{factory}_{line}_{machine_code}', '{status}', "
                        f"TO_DATE('{date_time}', 'yyyy/MM/dd HH24:mi:ss'))")
                    try:
                        self.execute_query(self.connection, query)
                        return True
                    except Exception as ex:
                        print(f"Error executing query: {str(ex)}")
            except Exception as e:
                print(f"ngoại lệ: {str(e)}")
                return False














