import sys
from PyQt5.QtCore import QObject, pyqtSignal, QThread
import time
from clMachineStatus import MachineStatus
import pymcprotocol
import json, threading
import time, datetime
from PyQt5.QtWidgets import QApplication
from clConfig import Config
from connectDB import connectDB
from datetime import timedelta
import datetime
class MainLogic(QObject):
    update_signal = pyqtSignal(list)  # Tín hiệu để cập nhật UI
    def __init__(self):
        super().__init__()
        self.plc = MachineStatus()
        self.machines_status = {}
        self.hasPrintedError = False
        self.pass_count = {0: 0, 2: 0}
        self.fail_count = {1: 0, 3: 0}
        self.previous_output = {}
        self.previous_output_fail = {}
        self.previous_pickup = {0: 0, 2: 0,4: 0, 6: 0}
        self.previous_throw = {1: 0, 3: 0,5: 0,7: 0}
        self.pass_pickup_count = {0: 0, 2: 0,4: 0, 6: 0}
        self.fail_throw_count = {1: 0, 3: 0,5: 0,7: 0}
        self.flag = False
        self.conn = connectDB()
        self.previous_cycle_time = 0
        with open('data.json', 'r') as file:
            self.Config = json.load(file)
        with open('plcConfig.json', 'r') as file:
            self.plcList = json.load(file)['plcs']
        for plc in self.plcList:
            self.machines_status[plc["nameMachine"]] = MachineStatus()
            self.machines_status[plc["nameMachine"]].clStartErrorTime = {}
    
    def collect_machine_data(self, ipPLC, ipPort, nameMachine, typeMachine):
        machines = []
        # plc = MachineStatus()
        plc = self.machines_status[nameMachine]
        pymc3e = pymcprotocol.Type3E()
        # kết nối lại nếu bị mất kết nối
        # def check_connect():
        #     try:
        #         # Thử đọc một bit nhỏ để kiểm tra xem kết nối có hoạt động không
        #         pymc3e.batchread_bitunits(headdevice="L5000", readsize=1)
        #         return True
        #     except:
        #         return False
        # def try_connect(retry_delay, try_count):
        #     retries = 0
        #     while retries < try_count:
        #         try:
        #             pymc3e.connect(ipPLC, ipPort)
        #             print(f"Kết nối thành công tới máy {nameMachine} ({ipPLC}:{ipPort})")
        #             self.plc.clflag = True
        #             self.plc.clConnect = 'Đang kết nối'
        #             return True  # Trả về True để báo kết nối thành công
        #         except Exception as ex:
        #             print(f"Lỗi kết nối: {ex}. Đang thử kết nối lại sau {retry_delay}s !!")
        #             Config.writeLog(f'Lỗi kết nối PLC: {ex}')
        #             retries += 1
        #             time.sleep(retry_delay)
        #     self.plc.clflag = False
        #     if retries == try_count:
        #         print(f"Thử kết nối thất bại sau {try_count} lần. Dừng lại.")
        #         self.plc.clConnect = 'Mất kết nối'
        #         return False
        try:
            # Kết nối tới PLC
            current_time = datetime.datetime.now().replace(microsecond=0)
            pymc3e.connect(ipPLC, ipPort)
            plc.clIpaddr = ipPLC
            plc.ipPort = ipPort
            plc.clNameMachine = nameMachine
            plc.clConnect = 'Đang kết nối'
            plc.typeMachine = typeMachine
            plc.Cltime = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            listErrors = self.read_errors_from_txt('errorName.txt')
            listErrorStop = self.read_errors_from_txt('errorStopName.txt')
            listErrorCode = self.read_errors_from_txt('errorCode.txt')
            wordunits_errors = pymc3e.batchread_bitunits(headdevice="L5000", readsize=206)
            word_bit_IDLE = pymc3e.batchread_bitunits(headdevice="L5207", readsize=1)
            word_bit_output = pymc3e.batchread_bitunits(headdevice="L5208", readsize=4)
            word_bit_pick_throw = pymc3e.batchread_bitunits(headdevice="L5212", readsize= 8)                                  
            word_bit_light = pymc3e.batchread_bitunits(headdevice='L5221', readsize=3 )
            word_cycle_time = pymc3e.batchread_wordunits(headdevice="D1", readsize=1)
            self.insert_time_default(plc, current_time)
            self.handle_idle_state(plc, current_time, word_bit_IDLE, wordunits_errors)
            self.handle_error(plc,current_time, word_bit_light)
            self.handle_error_state_combined(plc,current_time, listErrors, listErrorStop, listErrorCode, wordunits_errors)
            self.handle_stop_error(plc,current_time, word_bit_light, wordunits_errors, word_bit_IDLE)
            self.handle_run_state(plc, current_time, wordunits_errors, word_bit_light,word_bit_IDLE )
            self.handle_Product_Output(plc,current_time, word_bit_output)
            self.handl_pickup_throw(plc,current_time, word_bit_pick_throw)
            self.handle_cycle_time(plc,current_time, word_cycle_time , pymc3e)
            machines.append(plc)

        except Exception as ex:                                                                                                      
            print(f'Error Mất kết nối PLC máy {plc["ip"]}:{plc["port"]}: {ex} lúc {current_time}')
            Config.writeLog(f'Error Mất kết nối PLC máy {plc.clNameMachine}: {ex}')
            plc.clConnect = 'Mất kết nối'
            plc.clflag = False
            self.conn.update_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'0')
            self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'PAUSE')
            # if try_connect(1,2):
            #     return self.collect_machine_data(ipPLC, ipPort, nameMachine, typeMachine)  # Gọi lại hàm để thu thập lại ngay
        return machines
    def insert_time_default(self,plc, current_time):
        hours = current_time.hour
        minutes = current_time.minute
        seconds = current_time.second
        if minutes == 59 and 40<= seconds <= 50 and self.flag == False:
            if plc.clStartIDLE is not None:
                idle_time = (current_time - plc.clStartIDLE).total_seconds()
                if  idle_time > 1:
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} IDLE lúc 59:40s : {idle_time:.0f}s")
                    self.conn.insert_standby_time(self.Config['factory'], self.Config['line'], plc.clNameMachine, idle_time)
                    plc.clStartIDLE = None
                    self.flag = True
            if plc.clStartStopTime1 is not None:
                timeerror = (current_time - plc.clStartStopTime1).total_seconds() 
                if timeerror > 1:
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} ERROR lúc 59:40s : {timeerror:.0f}s")
                    plc.clStartStopTime1 = None
                    self.conn.insert_error_time(self.Config['factory'],self.Config['line'], plc.clNameMachine, timeerror)
                    self.flag = True
            if plc.clStartStopTime is not None:
                time_stop = (current_time - plc.clStartStopTime).total_seconds() 
                if time_stop:
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} STOP lúc 59:40s : {time_stop:.0f}s")
                    plc.clStartStopTime = None 
                    self.conn.insert_stop_time(self.Config['factory'], self.Config['line'], plc.clNameMachine, time_stop)
                    self.flag = True
            if plc.clStartRunTime is not None:
                timeRun = (current_time - plc.clStartRunTime).total_seconds()
                if timeRun > 0:
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} RUN lúc 59:40s : {timeRun:.0f}s")
                    plc.clStartRunTime = None  
                    plc.clStatus = "ERROR"
                    self.flag = True
                    self.conn.insert_on_time(self.Config['factory'],self.Config['line'], plc.clNameMachine, timeRun)
        else:
            self.flag == False

    #xử lý trạng thái IDLE
    def handle_idle_state(self,plc, current_time, word_bit_IDLE, wordunits_errors):
        try: 
            has_error = any(value == 1 for value in wordunits_errors)
            if word_bit_IDLE[0] == 1 and not has_error:
                if plc.clStartIDLE is None:
                    plc.clStartIDLE = current_time  # Ghi nhận thời gian bắt đầu IDLE
                    plc.clIDLE = '1'
                    plc.clGreen = '1'
                    plc.clRed = '0'
                    plc.clYellow = '0'
                    plc.clStatus = 'NORMAL'
                    print(f'Máy bắt đầu IDLE {plc.clNameMachine} lúc {plc.clStartIDLE}')
                    self.conn.update_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'2')
                else:
                    plc.clIDLE = '1'
                    plc.clStatus = 'NORMAL'
            elif word_bit_IDLE[0] == 0 or  has_error:
                # Khi máy không còn IDLE
                if plc.clStartIDLE is not None:
                    idle_time = (current_time - plc.clStartIDLE).total_seconds()
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} IDLE: {idle_time:.0f}s")
                    self.conn.insert_standby_time(self.Config['factory'], self.Config['line'], plc.clNameMachine, idle_time)
                    # Reset trạng thái IDLE sau khi xử lý
                    plc.clStartIDLE = None
                else:
                    plc.clIDLE = '0'
        except Exception as ex:
            print(f'Error handle idle state: {ex}')
    
    #Xử lý trạng thái Error nhưng máy vẫn chạy
    def handle_error (self, plc, current_time, word_bit_light):
        try:
            if word_bit_light[0] == 1 and word_bit_light[1] == 1 and word_bit_light[2] == 0 :
                if plc.clStartStopTime1 is None:
                    plc.clGreen = '1'
                    plc.clYellow = '1'
                    plc.clRed = '0'
                    plc.clStartStopTime1 = current_time
                    plc.clStatus = 'WARNING'
                    print(f'Máy bắt đầu Error {plc.clNameMachine} lúc {plc.clStartStopTime1}')
                    self.conn.update_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'3')
                    self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'WARNING')
                else:
                    plc.clGreen = '1'
                    plc.clYellow = '1'
                    plc.clStatus = 'WARNING'
            else:
                if plc.clStartStopTime1 is not None:
                    timeerror = (current_time - plc.clStartStopTime1).total_seconds() 
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} Error  : {timeerror:.0f}s")
                    plc.clStartStopTime1 = None
                    plc.clStatus = 'NORMAL'
                    self.conn.insert_error_time(self.Config['factory'],self.Config['line'], plc.clNameMachine, timeerror)
                else:
                    plc.clStatus = 'NORMAL'
        except Exception as ex:
            print(f'Error handle error state: {ex}')
    #Xử lý trạng thái Stop
    def handle_stop_error(self, plc, current_time, word_bit_light, wordunits_errors, word_bit_IDLE):
        try:
            has_error = any(value == 1 for value in wordunits_errors)
            if (word_bit_light[1] == 1 and word_bit_light[0] == 0 and word_bit_light[2] == 0 and word_bit_IDLE[0] == 0) or word_bit_light[2] == 1 :                  
                if plc.clStartStopTime is None and wordunits_errors[187] == 0:
                    plc.clStartStopTime = current_time
                    plc.clStatus = 'WARNING'
                    plc.clGreen = '0'
                    plc.clYellow = '1'
                    plc.clRed = '0'
                    print(f'Máy bắt đầu Stop {plc.clNameMachine} lúc {plc.clStartStopTime}')
                    self.conn.update_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'4')
                    self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'ERROR')
                else:
                    plc.clGreen = '0'
                    plc.clYellow = '1'
                    plc.clRed = '0'
                    plc.clStatus = 'WARNING'
            elif (word_bit_light[0] == 1 and word_bit_light[1] == 1 and word_bit_light[2] == 0) or (has_error == False and word_bit_light[1] == 0) or word_bit_IDLE[0] == 1:
                if plc.clStartStopTime is not None:
                    time_stop = (current_time - plc.clStartStopTime).total_seconds()  # Tính thời gian IDLE
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} Stop : {time_stop:.0f}s")
                    plc.clStartStopTime = None  # Reset thời gian Stop
                    self.conn.insert_stop_time(self.Config['factory'], self.Config['line'], plc.clNameMachine, time_stop)
                    Config.writeLog(f'Time Error của máy {plc.clNameMachine} : {time_stop}s')
                
        except Exception as ex:
            print(f'Error handle stop error state: {ex}')

    def handle_error_state_combined(self, plc, current_time, listErrors, listErrorStop, listErrorCode, wordunits_errors):
        try:
            listError = []
            if not hasattr(plc, 'clStartErrorTime'):
                plc.clStartErrorTime = {}  # Khởi tạo từ điển để lưu thời gian bắt đầu lỗi
            
            # Kiểm tra có lỗi hay không
            has_error = any(value == 1 for value in wordunits_errors)
            if has_error:
                plc.clYellow = '1'
                for i, error in enumerate(wordunits_errors):
                    if error == 1:
                        listError.append(i + 1)
                        # Lưu thời gian bắt đầu chỉ khi chưa được lưu
                        if (i + 1) not in plc.clStartErrorTime:
                            plc.clStartErrorTime[i + 1] = current_time  # Lưu thời gian bắt đầu
                            print(f'Bắt đầu của lỗi {listErrors[i + 1]} máy {plc.clNameMachine} là {current_time}')
                            # Ghi vào cơ sở dữ liệu
                            self.conn.insert_error_timeon(self.Config['factory'], self.Config['line'], plc.clNameMachine, 'ERROR', listErrorCode[i + 1], listErrors[i + 1], plc.clStartErrorTime[i + 1], self.Config['owner'])
                            self.conn.cnt_insert_error_timeon(self.Config['factory'], self.Config['line'], plc.clNameMachine, self.Config['projectName'], plc.typeMachine, (i + 1), listErrorCode[i + 1], plc.clStartErrorTime[i + 1])

                # Cập nhật lỗi cho PLC
                errorCode = ', '.join(str(error) for error in listError)
                plc.clError = errorCode
            
            # Xử lý lỗi đã kết thúc
            for i in list(plc.clStartErrorTime.keys()):  # Duyệt qua các lỗi hiện có
                # Kiểm tra nếu lỗi đã hết (giá trị 0 tương ứng)
                if i <= len(wordunits_errors) and wordunits_errors[i - 1] == 0:
                    start_time = plc.clStartErrorTime.pop(i)  # Lấy và xóa thời gian bắt đầu
                    end_time = current_time
                    error_duration = (end_time - start_time).total_seconds()  # Tính thời gian đã trôi qua
                    
                    if error_duration < 0:  # Kiểm tra thời gian âm
                        error_duration = 0
                    
                    print(f'Kết thúc lỗi {listErrors[i]}máy {plc.clNameMachine} mã {listErrorCode[i]} tại {end_time}. Thời gian lỗi: {error_duration} giây')
                    # Cập nhật vào cơ sở dữ liệu
                    self.conn.update_error_on(self.Config['factory'], self.Config['line'], plc.clNameMachine, listErrorCode[i], end_time)
                    self.conn.cnt_update_error_on(self.Config['factory'], self.Config['line'], plc.clNameMachine, listErrorCode[i], end_time)
                    Config.writeLog(f'Lỗi {listErrors[i]} mã {listErrorCode[i]} và {i} kết thúc tại {end_time}. Thời gian lỗi: {error_duration} giây')
            
            if not plc.clStartErrorTime:  # Nếu không còn lỗi nào
                plc.clError = ''
        except Exception as ex:
            print(f'Error handle error state combined: {ex}')

    #Xử lý trạng thái Run
    def handle_run_state(self,plc, current_time, wordunits_errors, word_bit_light, word_bit_IDLE):
        try:
            has_error = any(value == 1 for value in wordunits_errors)
            if (word_bit_light[0] ==1 and word_bit_light[1] ==0 and word_bit_light[2] ==0 and not has_error and word_bit_IDLE[0] == 0):
                if plc.clStartRunTime is None:
                    plc.clStatus = 'NORMAL'
                    plc.clGreen = '1'
                    plc.clYellow = '0'
                    plc.clRed = '0'
                    plc.clStartRunTime = current_time
                    print(f"Máy {plc.clNameMachine} bắt đầu chạy lúc {plc.clStartRunTime.strftime('%Y-%m-%d %H:%M:%S')}")
                    self.conn.update_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'1')
                    self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'OFF')
                    plc.hasPrintedError = True
                elif plc.hasPrintedError and plc.clStartRunTime is not None:
                    plc.clStatus = 'NORMAL'
                    plc.clError = '' 
                    plc.clYellow = '0'
                    plc.clRed = '0'
            elif word_bit_IDLE[0] == 1 or (word_bit_light[0] ==0 and word_bit_light[1] ==1 and word_bit_light[2] ==0) or has_error or word_bit_light[2] == 1 or (word_bit_light[0] ==1 and word_bit_light[1] ==1 and word_bit_light[2] ==0) :
                if plc.clStartRunTime is not None:
                    timeRun = (current_time - plc.clStartRunTime).total_seconds()
                    print(f"Máy {plc.clNameMachine} hết chạy, tổng thời gian chạy: {timeRun:.0f}s")
                    plc.clStartRunTime = None  
                    plc.hasPrintedError = False
                    self.conn.insert_on_time(self.Config['factory'],self.Config['line'], plc.clNameMachine, timeRun)
                else:
                    plc.hasPrintedError = False
        except Exception as ex:
            print(f'Error handle run state: {ex}')
    # Sản lượng pass, fail
    def handle_Product_Output(self, plc, currentTime ,word_bit_output):
        try:
            # Khởi tạo previous_output cho PLC nếu chưa có
            if plc.clNameMachine not in self.previous_output:
                self.previous_output[plc.clNameMachine] = {0: 0, 2: 0}
            if plc.clNameMachine not in self.previous_output_fail:
                self.previous_output_fail[plc.clNameMachine] = {1: 0, 3: 0}
            for i, output in enumerate(word_bit_output):
                if i == 0:
                    if output == 1 and self.previous_output[plc.clNameMachine][0] == 0:
                        self.pass_count[0] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} pass làn 1 tại {currentTime}: {self.pass_count[0]}')
                        self.conn.insert_production_pass(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_output[plc.clNameMachine][0] = output
                if i == 1:
                    if output == 1 and self.previous_output_fail[plc.clNameMachine][1] == 0:
                        self.fail_count[1] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} fail làn 1 tại {currentTime}: {self.fail_count[1]}')
                        self.conn.insert_production_fail(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_output_fail[plc.clNameMachine][1] = output
                if i == 2:
                    if output == 1 and self.previous_output[plc.clNameMachine][2]== 0:
                        self.pass_count[2] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} pass làn 2 tại {currentTime}: {self.pass_count[2]}')
                    self.previous_output[plc.clNameMachine][2] = output
                if i == 3:
                    if output == 1 and self.previous_output_fail[plc.clNameMachine][3] == 0:
                        self.fail_count[3] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} fail làn 2 tại {currentTime}: {self.fail_count[3]}')
                    self.previous_output_fail[plc.clNameMachine][3] = output
        except Exception as ex:
            print(f'Error handle Product Output: {ex}')
    # Sản lượng pickup, throw
    def handl_pickup_throw(self, plc, currentTime, word_bit_pick):
        try:
            if plc.clNameMachine not in self.previous_pickup:
                self.previous_pickup[plc.clNameMachine] = {0: 0, 2: 0, 4: 0, 6: 0}
            if plc.clNameMachine not in self.previous_throw:
                self.previous_throw[plc.clNameMachine] = {1: 0, 3: 0, 5: 0, 7: 0}
            for i, pick in enumerate(word_bit_pick):
                if i == 0:
                    if pick == 1 and self.previous_pickup[plc.clNameMachine][0] == 0:
                        self.pass_pickup_count[0] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} Pickup 1 tại {currentTime}: {self.pass_pickup_count[0]}')
                        self.conn.insert_pickup_qty1(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_pickup[plc.clNameMachine][0] = pick
                if i == 1:
                    if pick == 1 and self.previous_throw[plc.clNameMachine][1] == 0:
                        self.fail_throw_count[1] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} Throw 1 tại {currentTime}: {self.fail_throw_count[1]}')
                        self.conn.insert_throw_qty1(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_throw[plc.clNameMachine][1] = pick
                if i == 2:
                    if pick == 1 and self.previous_pickup[plc.clNameMachine][2] == 0:
                        self.pass_pickup_count[2] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} Pickup 2 tại {currentTime}: {self.pass_pickup_count[2]}')
                        self.conn.insert_pickup_qty2(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_pickup[plc.clNameMachine][2] = pick
                if i == 3:
                    if pick == 1 and self.previous_throw[plc.clNameMachine][3] == 0:
                        self.fail_throw_count[3] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} Throw 2 tại {currentTime}: {self.fail_throw_count[3]}')
                        self.conn.insert_throw_qty2(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_throw[plc.clNameMachine][3] = pick
                if i == 4:
                    if pick == 1 and self.previous_pickup[plc.clNameMachine][4] == 0:
                        self.pass_pickup_count[4] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} Pickup 3 tại {currentTime}: {self.pass_pickup_count[4]}')
                        self.conn.insert_pickup_qty3(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_pickup[plc.clNameMachine][4] = pick
                if i == 5:
                    if pick == 1 and self.previous_throw[plc.clNameMachine][5] == 0:
                        self.fail_throw_count[5] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} Throw 3 tại {currentTime}: {self.fail_throw_count[5]}')
                        self.conn.insert_throw_qty3(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_throw[plc.clNameMachine][5] = pick
                if i == 6:
                    if pick == 1 and self.previous_pickup[plc.clNameMachine][6] == 0:
                        self.pass_pickup_count[6] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} Pickup 4 tại {currentTime}: {self.pass_pickup_count[6]}')
                        self.conn.insert_pickup_qty4(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_pickup[plc.clNameMachine][6] = pick
                if i == 7:
                    if pick == 1 and self.previous_throw[plc.clNameMachine][7] == 0:
                        self.fail_throw_count[7] += 1
                        print(f'Máy {plc.clNameMachine} IP: {self.Config['ipServer']} Throw 4 tại {currentTime}: {self.fail_throw_count[7]}')
                        self.conn.insert_throw_qty4(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                    self.previous_throw[plc.clNameMachine][7] = pick   
        except Exception as ex:
            print(f'Error handl pickup throw: {ex}')
    
    def read_errors_from_txt(self, file_path):
        try:
            errors_dict = {}
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    error_name, error_code = line.strip().split(',')
                    errors_dict[int(error_code.strip())] = error_name.strip()
            return errors_dict
        except Exception as ex:
            print(f'Error read file: {ex}')
    # Xử lý cycle time
    def handle_cycle_time(self, plc, current_time, word_cycle_time,pymc3e):
        try:
            if word_cycle_time[0] != 0  and self.previous_cycle_time ==0:
                print(f'Cycle time máy {plc.clNameMachine} là {word_cycle_time[0]} lúc {current_time}')
                self.conn.insert_cycle_time(self.Config['factory'],self.Config['line'], plc.clNameMachine, word_cycle_time[0], current_time)
                #Reset cycle time = 0
                pymc3e.batchwrite_wordunits(headdevice="D1", values=[0])
            self.previous_cycle_time = word_cycle_time[0]
        except Exception as ex:
            print(f'Error handle cycle time: {ex}')
    
    # duyệt xử lý thu thấp data all PLC 
    def threadPLC(self):
        while True:
            all_machines = []
            for plc in self.plcList:
                machine = self.collect_machine_data(plc["ip"], plc["port"], plc["nameMachine"], plc["typeMachine"])
                all_machines.extend(machine)
            self.update_signal.emit(all_machines)
            time.sleep(0.3)
    # def handle_plc(self, plc):
    #     while True:
    #         all_machines = []
    #         for plc in self.plcList:
    #             machine_data = self.collect_machine_data(plc["ip"], plc["port"], plc["nameMachine"], plc["typeMachine"])
    #             all_machines.extend(machine_data)
    #         self.update_signal.emit(all_machines)  # Phát tín hiệu cập nhật dữ liệu của từng PLC
    #         time.sleep(0.3)

    # def threadPLC(self):
    #     # Tạo luồng riêng cho mỗi PLC
    #     for plc in self.plcList:
    #         plc_thread = threading.Thread(target=self.handle_plc, args=(plc,))
    #         plc_thread.daemon = True  # Đảm bảo luồng sẽ tự động dừng khi chương trình chính kết thúc
    #         plc_thread.start()
# class WorkerThread(QThread):
#     def __init__(self, logic):
#         super().__init__()
#         self.logic = logic
#     def run(self):
#         self.logic.threadPLC()
# app = QApplication(sys.argv)
# # Khởi tạo đối tượng MainLogic và WorkerThread để chạy luồng
# main = MainLogic()
# worker_thread = WorkerThread(main)
# worker_thread.start()
# sys.exit(app.exec_())
