import oracledb
import datetime
import threading

# Kết nối với cơ sở dữ liệu
class connectDB:
    def __init__(self):
        self.connection = self.create_connection()
    def create_connection(self):
        try:
            connection = oracledb.connect(
                user="system",               # Tên đăng nhập Oracle
                password="123456",           # Mật khẩu
                dsn="localhost:1521/orcl"    # Thông tin kết nối
            )
            print('Kết nối thành công')
            return connection
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi kết nối tới cơ sở dữ liệu: {e}")
            return None

    # Tạo đối tượng lock để đảm bảo an toàn luồng
    lock_DB = threading.Lock()

    # Hàm truy vấn dữ liệu
    def select(connection, query, parameters=None):
        cursor = connection.cursor()
        try:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            result = cursor.fetchall()  # Lấy toàn bộ kết quả
            return result
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi truy vấn dữ liệu: {e}")
            return None
        finally:
            cursor.close()

    # Hàm thực thi câu lệnh SQL (INSERT, UPDATE, DELETE)
    def execute_query(connection, query, parameters=None):
        cursor = connection.cursor()
        try:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            connection.commit()  # Xác nhận thay đổi
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi thực thi truy vấn: {e}")
            connection.rollback()  # Hoàn tác nếu có lỗi
        finally:
            cursor.close()
    # Hàm xử lý insert/update thời gian chạy máy
    def insert_on_time(self, factory, line, machine_code, time_run):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        query = ""
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = connectDB.select(self.connection, query)

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
                connectDB.execute_query(self.connection, query)
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
                data = connectDB.select(self.connection, query)
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
                
               
                connectDB.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"Lỗi: {e}")
            return False

    def insert_stop_time(self, factory, line, machine_code, timeStop):
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H") 
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = connectDB.select(self.connection,query)

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
                connectDB.execute_query(self.connection, query)
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
                data = connectDB.select(self.connection, query)

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
                connectDB.execute_query(self.connection, query)
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
                data = connectDB.select(self.connection, query)

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
                connectDB.execute_query(self.connection, query)
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
                data = connectDB.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty1 = row[7]  
                        if throw_qty1 == None or throw_qty1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY1 = '1', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty1_1 = int(throw_qty1) + 1
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY1 = '{throw_qty1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, THROW_QTY1, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '1', '{uph}')"
                connectDB.execute_query(self.connection, query)
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
                data = connectDB.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty1 = row[11]
                        if pick_qty1 == None or pick_qty1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY1 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty1_1 = int(pick_qty1) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY1 = '{pick_qty1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, PICK_QTY1, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                connectDB.execute_query(self.connection, query)
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
                data = connectDB.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty2 = row[12] 
                        if pick_qty2 == None or pick_qty2 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY2 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty2_1 = int(pick_qty2) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY2 = '{pick_qty2_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, PICK_QTY2, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                connectDB.execute_query(self.connection, query)
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
                data = connectDB.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty2 = row[8] 
                        if throw_qty2 == None or throw_qty2 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY2 = '1', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty2_1 = int(throw_qty2) + 1
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY2 = '{throw_qty2_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, THROW_QTY2, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '1', '{uph}')"
                
                connectDB.execute_query(self.connection, query)
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
                data = connectDB.select(self.connection, query)
                
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
                
                connectDB.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def insert_throw_qty3(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB: 
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = connectDB.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty3 = row[9]  
                        if throw_qty3 == None or throw_qty3 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY3 = '1', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty3_1 = int(throw_qty3) + 1
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY3 = '{throw_qty3_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, THROW_QTY3, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '1', '{uph}')"
                
                connectDB.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False

    def insert_pickup_qty4(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB: 
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = connectDB.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        pick_qty4 = row[14]  
                        if pick_qty4 is None or pick_qty4 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY4 = '10', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            pick_qty4_1 = int(pick_qty4) + 10
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET PICK_QTY4 = '{pick_qty4_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, PICK_QTY4, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '10', '{uph}')"
                
                connectDB.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        return False
    
    def insert_throw_qty4(self, factory, line, machine_code, uph):
        query = ""
        work_date = datetime.datetime.now().strftime("%d-%m-%Y %H")
        
        try:
            with connectDB.lock_DB:  
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = connectDB.select(self.connection, query)
                
                if data and len(data) > 0:
                    for row in data:
                        throw_qty4 = row[10]  
                        if throw_qty4 is None or throw_qty4 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY4 = '1', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            throw_qty4_1 = int(throw_qty4) + 1
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET THROW_QTY4 = '{throw_qty4_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, THROW_QTY4, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '1', '{uph}')"
                
                connectDB.execute_query(self.connection, query)
                return True
        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
        
        return False

    def insert_cycle_time(self, factory, line, machine_code, cycle_time):
        work_date = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        query = ""
        
        try:
            with connectDB.lock_DB:  # Sử dụng lock để đảm bảo an toàn luồng
                query = f"INSERT INTO CYCLE_TIME_MACHINE (FACTORY, LINE, MACHINE_CODE, CYCLE, DATE_TIME) VALUES ('{factory}', '{line}', '{machine_code}', '{cycle_time}', TO_DATE('{work_date}', 'yyyy/MM/dd HH24:MI:SS'))"
                
                try:
                    connectDB.execute_query(self.connection, query)
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
                query = f"SELECT * FROM CNT_MACHINE_SUMMARY WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                data = connectDB.select(self.connection, query)

                if data and len(data) > 0:
                    for row in data:
                        ngqty1 = row[15] 
                        if ngqty1 is None or ngqty1 == "":
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET NG_QTY = '1', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                        else:
                            ngqty1_1 = int(ngqty1) + 1
                            query = f"UPDATE CNT_MACHINE_SUMMARY SET NG_QTY = '{ngqty1_1}', UPH = '{uph}' WHERE MACHINE_NO='{factory}_{line}_{machine_code}' AND TO_CHAR(WORK_DATE, 'dd-MM-yyyy HH24') LIKE '%{work_date}%'"
                else:
                    query = f"INSERT INTO CNT_MACHINE_SUMMARY (MACHINE_NO, WORK_DATE, NG_QTY, UPH) VALUES ('{factory}_{line}_{machine_code}', TO_DATE('{work_date}', 'dd-MM-yyyy HH24'), '1', '{uph}')"
                
                try:
                    connectDB.execute_query(self.connection, query)
                    return True
                except Exception as ex:
                    print(f"Error executing query: {str(ex)}")

        except Exception as e:
            print(f"ngoại lệ: {str(e)}")
            
        return False
