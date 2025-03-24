import oracledb
import datetime
from clConfig import Config 
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
    
    def select(self, connection, query, parameters=None):
        cursor = connection.cursor()
        try:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
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
            connection.commit()  
        except oracledb.DatabaseError as e:
            print(f"Lỗi khi thực thi truy vấn: {e}")
            connection.rollback()
            Config.writeLog(f"Lỗi khi thực thi truy vấn: {e}")
        finally:
            cursor.close()

    def insert_machine_data(self, buffer, uph):
        work_date = datetime.datetime.now().strftime("%Y-%m-%d %H")
        try:
            # Chuẩn bị dữ liệu cho batch
            batch_data = []
            for key, values in buffer.items():
                factory, line, machine_code, hour = key.split(',')
                machine_no = f"{factory}_{line}_{machine_code}"
                batch_data.append({
                    'machine_no': machine_no,
                    'work_date': work_date,
                    'run_time': values['run_time'],
                    'error_time': values['error_time'],
                    'standby_time': values['standby_time'],
                    'stop_time': values['stop_time'],
                    'output': values['output'],
                    'THROW_QTY1': values['THROW_QTY1'],
                    'THROW_QTY2': values['THROW_QTY2'],
                    'PICK_QTY1': values['PICK_QTY1'],
                    'PICK_QTY2': values['PICK_QTY2'],
                    'THROW_QTY3': values['THROW_QTY3'],
                    'THROW_QTY4': values['THROW_QTY4'],
                    'PICK_QTY3': values['PICK_QTY3'],
                    'PICK_QTY4': values['PICK_QTY4'],
                    'NG_QTY': values['NG_QTY'],
                    'uph': uph
                })

            if not batch_data:
                print("No data to insert.")
                return True

            # Câu lệnh MERGE
            merge_query = """
                MERGE INTO CNT_MACHINE_SUMMARY target
                USING (SELECT :machine_no AS MACHINE_NO, TO_DATE(:work_date, 'YYYY-MM-DD HH24') AS WORK_DATE FROM dual) source
                ON (target.MACHINE_NO = source.MACHINE_NO AND target.WORK_DATE = source.WORK_DATE)
                WHEN MATCHED THEN
                    UPDATE SET 
                        RUN_TIME = NVL(target.RUN_TIME, 0) + :run_time,
                        ERROR_TIME = NVL(target.ERROR_TIME, 0) + :error_time,
                        STANDBY_TIME = NVL(target.STANDBY_TIME, 0) + :standby_time,
                        STOP_TIME = NVL(target.STOP_TIME, 0) + :stop_time,
                        OUTPUT = NVL(target.OUTPUT, 0) + :output,
                        THROW_QTY1 = NVL(target.THROW_QTY1, 0) + :THROW_QTY1,
                        THROW_QTY2 = NVL(target.THROW_QTY2, 0) + :THROW_QTY2,
                        PICK_QTY1 = NVL(target.PICK_QTY1, 0) + :PICK_QTY1,
                        PICK_QTY2 = NVL(target.PICK_QTY2, 0) + :PICK_QTY2,
                        THROW_QTY3 = NVL(target.THROW_QTY3, 0) + :THROW_QTY3,
                        THROW_QTY4 = NVL(target.THROW_QTY4, 0) + :THROW_QTY4,
                        PICK_QTY3 = NVL(target.PICK_QTY3, 0) + :PICK_QTY3,
                        PICK_QTY4 = NVL(target.PICK_QTY4, 0) + :PICK_QTY4,
                        NG_QTY = NVL(target.NG_QTY, 0) + :NG_QTY,
                        UPH = :uph
                WHEN NOT MATCHED THEN
                    INSERT (MACHINE_NO, WORK_DATE, RUN_TIME, STANDBY_TIME, ERROR_TIME, STOP_TIME, OUTPUT, 
                            THROW_QTY1, THROW_QTY2, PICK_QTY1, PICK_QTY2, THROW_QTY3, THROW_QTY4, 
                            PICK_QTY3, PICK_QTY4, NG_QTY, UPH)
                    VALUES (:machine_no, TO_DATE(:work_date, 'YYYY-MM-DD HH24'), :run_time, :standby_time, 
                            :error_time, :stop_time, :output, :THROW_QTY1, :THROW_QTY2, :PICK_QTY1, 
                            :PICK_QTY2, :THROW_QTY3, :THROW_QTY4, :PICK_QTY3, :PICK_QTY4, :NG_QTY, :uph)
            """

            # Thực thi batch với executemany
            with connectDB.lock_DB:
                cursor = self.connection.cursor()
                try:
                    cursor.executemany(merge_query, batch_data)
                    self.connection.commit()
                    print(f"Batch update thành công {len(batch_data)} bản ghi lên bảng CNT_MACHINE_SUMMARY!")
                    return True
                except oracledb.DatabaseError as e:
                    print(f"Lỗi khi thực thi batch: {e}")
                    self.connection.rollback()
                    Config.writeLog(f'Error insert_machine_data: {e}')
                    return False
                finally:
                    cursor.close()
        except Exception as e:
            print(f"Lỗi: {e}")
            Config.writeLog(f'Error insert_machine_data: {e}')
            return False
  
    def update_status(self, status_list):
        try:
            batch_data = [
                {
                    'machine_no': f"{s['factory']}_{s['line']}_{s['machine_code']}",
                    'building': s['factory'],
                    'project_name': s['project_name'],
                    'section_name': s['section_name'],
                    'line_name': s['line'],
                    'uph': s['uph'],
                    'db_ip': s['db_ip'],
                    'db_server_name': s['db_server_name'],
                    'current_state': s['current_state']
                } for s in status_list
            ]
            if not batch_data:
                print("No status data to update.")
                return True

            merge_query = """
                MERGE INTO CNT_MACHINE_INFO target
                USING (SELECT :machine_no AS MACHINE_NO FROM dual) source
                ON (target.MACHINE_NO = source.MACHINE_NO)
                WHEN MATCHED THEN
                    UPDATE SET 
                        CURRENT_STATE = :current_state
                WHEN NOT MATCHED THEN
                    INSERT (MACHINE_NO, BUILDING, PROJECT_NAME, SECTION_NAME, LINE_NAME, UPH, DB_IP, DB_SERVER_NAME, CURRENT_STATE)
                    VALUES (:machine_no, :building, :project_name, :section_name, :line_name, :uph, :db_ip, :db_server_name, :current_state)
            """

            with connectDB.lock_DB:
                cursor = self.connection.cursor()
                try:
                    cursor.executemany(merge_query, batch_data)
                    self.connection.commit()
                    print(f"Batch updated {len(batch_data)} machine statuses in CNT_MACHINE_INFO!")
                    # Config.writeLog(f"Batch updated {len(batch_data)} machine statuses in CNT_MACHINE_INFO")
                    return True
                except oracledb.DatabaseError as e:
                    print(f"Error updating batch statuses: {e}")
                    self.connection.rollback()
                    Config.writeLog(f"Error updating batch statuses: {e}")
                    return False
                finally:
                    cursor.close()
        except Exception as e:
            print(f"Exception: {str(e)}")
            Config.writeLog(f"Exception in update_status: {str(e)}")
            return False

    def cnt_process_error_records(self, error_records):
        try:
            if not error_records:
                print("No error records to process.")
                return True

            # Phân loại bản ghi
            insert_records = []
            update_records = []
            for record in error_records:
                machine_no = f"{record['factory']}_{record['line']}_{record['machine_code']}"
                start_time = record['start_time'].strftime('%Y-%m-%d %H:%M:%S') if record.get('start_time') else None
                end_time = record['end_time'].strftime('%Y-%m-%d %H:%M:%S') if record.get('end_time') else None
                
                if end_time is None:
                    # Chỉ lấy các trường cần cho INSERT
                    insert_data = {
                        'machine_no': machine_no,
                        'project_name': record['project_name'],
                        'section_name': record['section_name'],
                        'error_id': record['error_id'],
                        'error_code': record['error_code'],
                        'start_time': start_time
                    }
                    insert_records.append(insert_data)
                else:
                    # Chỉ lấy các trường cần cho UPDATE
                    update_data = {
                        'machine_no': machine_no,
                        'error_code': record['error_code'],
                        'end_time': end_time
                    }
                    update_records.append(update_data)

            with connectDB.lock_DB:
                cursor = self.connection.cursor()
                try:
                    # Batch INSERT
                    if insert_records:
                        insert_query = """
                            INSERT INTO CNT_MACHINE_ERROR_RECORD (
                                MACHINE_NO, PROJECT_NAME, SECTION_NAME, ERROR_ID, ERROR_CODE, START_TIME
                            ) VALUES (
                                :machine_no, :project_name, :section_name, :error_id, :error_code,
                                TO_DATE(:start_time, 'YYYY-MM-DD HH24:MI:SS')
                            )
                        """
                        # print(f"Insert records: {insert_records}") 
                        cursor.executemany(insert_query, insert_records)
                        print(f"Batch inserted {len(insert_records)} new error records.")

                    # Batch UPDATE
                    if update_records:
                        update_query = """
                            UPDATE CNT_MACHINE_ERROR_RECORD
                            SET END_TIME = TO_DATE(:end_time, 'YYYY-MM-DD HH24:MI:SS')
                            WHERE MACHINE_NO = :machine_no
                            AND ERROR_CODE = :error_code
                            AND END_TIME IS NULL
                        """
                        # print(f"Update records: {update_records}") 
                        cursor.executemany(update_query, update_records)
                        print(f"Batch updated {len(update_records)} error records with END_TIME.")

                    self.connection.commit()
                    # Config.writeLog(f"Batch processed {len(insert_records)} inserts and {len(update_records)} updates in CNT_MACHINE_ERROR_RECORD")
                    return True

                except oracledb.DatabaseError as e:
                    print(f"Error processing batch error records: {e}")
                    self.connection.rollback()
                    Config.writeLog(f"Error processing batch error records: {e}")
                    return False
                finally:
                    cursor.close()
        except Exception as e:
            print(f"Exception in cnt_process_error_records: {str(e)}")
            Config.writeLog(f"Exception in cnt_process_error_records: {str(e)}")
            return False
    
    def cnt_update_error_on1(self, factory, line, machine_code, error_code):
            with connectDB.lock_DB:
                try:
                    machine_no = f"{factory}_{line}_{machine_code}"
                    query = """
                    DELETE FROM CNT_MACHINE_ERROR_RECORD
                    WHERE MACHINE_NO = :machine_no
                    AND ERROR_CODE = :error_code
                    AND END_TIME IS NULL
                    """
                    try:
                        self.execute_query(self.connection, query, {"machine_no": machine_no, "error_code": error_code})
                        return True
                    except Exception as ex:
                        print(f"Error executing query: {str(ex)}")
                        return False
                except Exception as e:
                    print(f"Lỗi khi xóa lỗi: {str(e)}")
                    return False


    # def cnt_update_error_on1(self, error_list):
    #     with connectDB.lock_DB:
    #         try:
    #             query = """
    #                 DELETE FROM CNT_MACHINE_ERROR_RECORD
    #                 WHERE MACHINE_NO = :machine_no
    #                 AND ERROR_CODE = :error_code
    #                 AND END_TIME IS NULL
    #             """
    #             data = [
    #                 {
    #                     "machine_no": f"{factory}_{line}_{machine_code}",
    #                     "error_code": error_code
    #                 }
    #                 for factory, line, machine_code, error_code in error_list
    #             ]
    #             try:
    #                 self.connection.cursor().executemany(query, data)
    #                 self.connection.commit()
    #                 print(f"Đã xóa {len(data)} lỗi thành công!")
    #                 return True
    #             except Exception as ex:
    #                 print(f"Error executing batch delete: {str(ex)}")
    #                 self.connection.rollback()
    #                 return False
    #         except Exception as e:
    #             print(f"Lỗi khi xóa lỗi: {str(e)}")
    #             return False

