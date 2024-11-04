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
                # user="pthnew",               
                # password="pthnew",           
                # dsn="10.228.114.170:3333/meorcl" 
                user="system",
                password="123456",           
                dsn="localhost:1521/orcl3"  
            )
            print('Kết nối thành công SERVER !!')
            return connection
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi kết nối tới cơ sở dữ liệu: {e}")
            return None
    # Hàm truy vấn dữ liệu
    def select(self, connection, query, parameters=None):
        cursor = connection.cursor()
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
    # Hàm thực thi cácc câu lệnh SQL (INSERT, UPDATE, DELETE)
    def execute_query(self,connection, query, parameters=None):
        cursor = connection.cursor()
        try:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            self.dem += 1
            print(f'Số lần truy cập DB: {self.dem}')
            connection.commit()  # Xác nhận thay đổi
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi thực thi truy vấn: {e}")
            connection.rollback()  # Hoàn tác nếu có lỗi
        finally:
            cursor.close()
 
    def insert_machine_data(self, buffer, uph):
        work_date = datetime.datetime.now().strftime("%Y-%m-%d %H")  # Định dạng thời gian
        try:
            batch_queries = []  # Danh sách để lưu các câu lệnh SQL
            with connectDB.lock_DB:
                for key, values in buffer.items():
                    factory, line, machine_code, hour = key.split('_')
                    machine_no = f"{factory}_{line}_{machine_code}"
                    # Truy vấn kiểm tra bản ghi hiện có
                    query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{machine_no}' AND TO_CHAR(WORK_DATE, 'YYYY-MM-DD HH24') LIKE '%{work_date}%'"
                    data = self.select(self.connection, query)

                    if data and len(data) > 0:
                        for row in data:
                            # Cập nhật thời gian và các số liệu khác
                            total_run_time = (row[2] if row[2] is not None else 0) + values['run_time']
                            total_IDLE_time = (row[3] if row[3] is not None else 0) + values['standby_time']
                            total_error_time = (row[4] if row[4] is not None else 0) + values['error_time']
                            total_stop_time = (row[5] if row[5] is not None else 0) + values['stop_time']
                            total_output = (row[6] if row[6] is not None else 0) + values['output']
                            total_throw_qty1 = (row[7] if row[7] is not None else 0) + values['THROW_QTY1']
                            total_throw_qty2 = (row[8] if row[8] is not None else 0) + values['THROW_QTY2']
                            total_pick_qty1 = (row[9] if row[9] is not None else 0) + values['PICK_QTY1']
                            total_pick_qty2 = (row[10] if row[10] is not None else 0) + values['PICK_QTY2']
                            total_throw_qty3 = (row[11] if row[11] is not None else 0) + values['THROW_QTY3']
                            total_throw_qty4 = (row[12] if row[12] is not None else 0) + values['THROW_QTY4']
                            total_pick_qty3 = (row[13] if row[13] is not None else 0) + values['PICK_QTY3']
                            total_pick_qty4 = (row[14] if row[14] is not None else 0) + values['PICK_QTY4']
                            total_ng_qty = (row[15] if row[15] is not None else 0) + values['NG_QTY']
                            
                            update_query = f"""
                            UPDATE CNT_MACHINE_SUMMARY1
                            SET RUN_TIME = {total_run_time}, ERROR_TIME = {total_error_time}, STANDBY_TIME = {total_IDLE_time}, STOP_TIME = {total_stop_time},
                                OUTPUT = {total_output}, THROW_QTY1 = {total_throw_qty1}, THROW_QTY2 = {total_throw_qty2}, PICK_QTY1 = {total_pick_qty1},
                                PICK_QTY2 = {total_pick_qty2}, THROW_QTY3 = {total_throw_qty3}, THROW_QTY4 = {total_throw_qty4}, PICK_QTY3 = {total_pick_qty3},
                                PICK_QTY4 = {total_pick_qty4}, NG_QTY = {total_ng_qty}, UPH = {uph}
                            WHERE MACHINE_NO='{machine_no}' AND TO_CHAR(WORK_DATE, 'YYYY-MM-DD HH24') LIKE '%{work_date}%'
                            """
                            batch_queries.append(update_query)
                    else:
                        insert_query = f"""
                        INSERT INTO CNT_MACHINE_SUMMARY1 
                        (MACHINE_NO, WORK_DATE, RUN_TIME, STANDBY_TIME, ERROR_TIME, STOP_TIME, OUTPUT, THROW_QTY1, THROW_QTY2, PICK_QTY1, PICK_QTY2,
                        THROW_QTY3, THROW_QTY4, PICK_QTY3, PICK_QTY4, NG_QTY, UPH) 
                        VALUES ('{machine_no}', TO_DATE('{work_date}', 'YYYY-MM-DD HH24'), {values['run_time']}, {values['standby_time']}, 
                        {values['error_time']}, {values['stop_time']}, {values['output']}, {values['THROW_QTY1']}, {values['THROW_QTY2']}, 
                        {values['PICK_QTY1']}, {values['PICK_QTY2']}, {values['THROW_QTY3']}, {values['THROW_QTY4']}, 
                        {values['PICK_QTY3']}, {values['PICK_QTY4']}, {values['NG_QTY']}, {uph})
                        """
                        batch_queries.append(insert_query)

                # Thực thi tất cả các câu lệnh trong batch
                for query in batch_queries:
                    self.execute_query(self.connection, query)

                self.connection.commit()
                print("Batch update thành công lên bảng CNT_MACHINE_SUMMARY1!!!")
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False


    def insert_on_time(self, factory, line, machine_code, time_run):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        query = ""
        try:
            with connectDB.lock_DB:
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        run_time = row[2] 
                        if not run_time:
                            run_time = 0
                        if run_time == "":
                            
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET RUN_TIME = '{time_run}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                           
                            run_time_1 = int(run_time) + int(time_run)
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET RUN_TIME = '{run_time_1}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                   
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, RUN_TIME) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '{time_run}')"
                
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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                if data and len(data) > 0:
                    for row in data:
                        errorTime1 = row[4]
                        if not errorTime1:
                            errorTime1 = 0
                        if errorTime1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET ERROR_TIME = '{errorTime}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                           
                            run_time_1 = int(errorTime1) + int(errorTime)
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET ERROR_TIME = '{run_time_1}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, ERROR_TIME) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '{errorTime}')"
                
               
                self.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False
        
    def insert_stop_time(self, factory, line, machine_code, timeStop):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H") 
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        stop_time = row[5] 
                        if not stop_time:
                            stop_time = 0
                        if stop_time == "":
                            
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET STOP_TIME = '{timeStop}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                           
                            stop_time_1 = int(stop_time) + int(timeStop)
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET STOP_TIME = '{stop_time_1}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, STOP_TIME) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '{timeStop}')"
                
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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        stand_time = row[3] 
                        if not stand_time:
                            stand_time = 0
                        if stand_time == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET STANDBY_TIME = '{timeStand}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            stand_time_1 = int(stand_time) + int(timeStand)
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET STANDBY_TIME = '{stand_time_1}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, STANDBY_TIME) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '{timeStand}')"
                
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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        output1 = row[6]  
                        if not output1:
                            output1 = 0
                        if output1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET OUTPUT = '5', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            output1_1 = int(output1) + 5
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET OUTPUT = '{output1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, OUTPUT, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '5', '{uph}')"

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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"               
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty1 = row[7]  
                        if throw_qty1 == None or throw_qty1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET THROW_QTY1 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty1_1 = int(throw_qty1) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET THROW_QTY1 = '{throw_qty1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, THROW_QTY1, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty1 = row[9]
                        if pick_qty1 == None or pick_qty1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET PICK_QTY1 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty1_1 = int(pick_qty1) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET PICK_QTY1 = '{pick_qty1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, PICK_QTY1, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty2 = row[10] 
                        if pick_qty2 == None or pick_qty2 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET PICK_QTY2 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty2_1 = int(pick_qty2) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET PICK_QTY2 = '{pick_qty2_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, PICK_QTY2, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty2 = row[8] 
                        if throw_qty2 == None or throw_qty2 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET THROW_QTY2 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty2_1 = int(throw_qty2) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET THROW_QTY2 = '{throw_qty2_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, THROW_QTY2, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                
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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = self.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty3 = row[13]  
                        if pick_qty3 == None or pick_qty3 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET PICK_QTY3 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty3_1 = int(pick_qty3) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY1 SET PICK_QTY3 = '{pick_qty3_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY1 (MACHINE_NO, WORK_DATE, PICK_QTY3, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                
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
                query = f"INSERT INTO CYCLE_TIME_MACHINE1 (FACTORY, LINE, MACHINE_CODE, CYCLE, DATE_TIME) VALUES ('{factory}', '{line}', '{machine_code}', '{cycle_time}', TO_DATE('{date_time}', 'yyyy/MM/dd HH24:MI:SS'))"
                
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
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        query = ""
        try:
            with connectDB.lock_DB:
                query = f"SELECT * FROM CNT_MACHINE_INFO1 WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND BUILDING='{factory}' AND LINE_NAME='{line}'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        state = row[23]
                        if state is None or state == "" or state is not None:
                            query = f"UPDATE CNT_MACHINE_INFO1 SET CURRENT_STATE = '{current_state}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND BUILDING='{factory}' AND LINE_NAME='{line}'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_INFO1 (MACHINE_NO, BUILDING, PROJECT_NAME, SECTION_NAME, LINE_NAME, UPH, DB_IP, DB_SERVER_NAME, CURRENT_STATE) VALUES ('{factory}_{line}_{machine_code}', '{factory}','{project_name}','{section_name}','{line}' ,'{uph}','{db_ip}','{db_server_name}', '{current_state}')"
                
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")

        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False
    
    def update_oracle_machine_status(self, factory, line, machine_code, status):
        query = ""
        try:
            with connectDB.lock_DB:
                query = f"SELECT * FROM AUTOMATION_STATUS1 WHERE FACTORY='{factory}' AND MACHINE_CODE='{machine_code}' AND LINE='{line}'"
                data = self.select(self.connection, query)
                if data and len(data) > 0:
                    for row in data:
                        state = row[3] 
                        if state is None or state == "" or state is not None:
                            query = f"UPDATE AUTOMATION_STATUS1 SET STATUS='{status}' WHERE FACTORY='{factory}' AND MACHINE_CODE='{machine_code}' AND LINE='{line}'"
                else:
                    # Nếu không có dữ liệu, thực hiện INSERT
                    query = f"INSERT INTO AUTOMATION_STATUS1 (FACTORY, MACHINE_CODE, LINE, STATUS) VALUES ('{factory}', '{machine_code}', '{line}', '{status}')"
                
                # Thực thi câu lệnh SQL
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
            return False
    
    def machine_status_update_oracle(self, factory, line, machine_code, status):
        query = ""      
        try:
            with connectDB.lock_DB:
                query =f"SELECT * FROM STATUS_AUTOMATION1 WHERE FACTORY='{factory}' AND MACHINE_CODE='{factory}_{line}_{machine_code}' AND LINE='{line}'"
                data = self.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        status = row[3] 
                        if status is None or status == "" or status is not None:
                            query = f"UPDATE STATUS_AUTOMATION1 SET STATUS='{status}' WHERE FACTORY='{factory}' AND MACHINE_CODE='{factory}_{line}_{machine_code}' AND LINE='{line}'"
                else:
                    query = f"INSERT INTO STATUS_AUTOMATION1 (FACTORY, MACHINE_CODE, LINE, STATUS) VALUES ('{factory}', '{factory}_{line}_{machine_code}', '{line}', '{status}')"
                try:
                    self.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
            return False
      
    def insert_error_timeon(self, factory, line, machine_code, status, error_code, error_name, time_error, owner):
        with connectDB.lock_DB:
            try:
                query = (f"INSERT INTO AUTOMATION_DATA_DETAIL1 (FACTORY, LINE, MACHINE_CODE, STATUS, ERROR_CODE, ERROR_NAME, TIME_ERROR, OWNER) "
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
                query = (f"UPDATE AUTOMATION_DATA_DETAIL1 "
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

    def cnt_insert_error_timeon(self, factory, line, machine_code, project_name, section_name, error_id, error_code, time_error):
        with connectDB.lock_DB:
            try:
                query = (f"INSERT INTO CNT_MACHINE_ERROR_RECORD1 (MACHINE_NO, PROJECT_NAME, SECTION_NAME, ERROR_ID, ERROR_CODE, START_TIME) "
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
                query = (f"UPDATE CNT_MACHINE_ERROR_RECORD1 "
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
                    query = (f"INSERT INTO PRODUCTION_QUANTITY_FPY1 (FACTORY, LINE, MACHINE_CODE, STATUS, DATE_TIME) "
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














