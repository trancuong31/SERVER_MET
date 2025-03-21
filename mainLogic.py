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
    update_signal = pyqtSignal(list)
    def __init__(self):
        super().__init__()
        self.plc = MachineStatus()
        self.machines_status = {}
        self.previous_cycle_time = {}
        self.previous_pickup = {}
        self.previous_throw = {}
        self.previous_output = {}
        self.previous_output_fail = {}
        self.buffers = {}
        self.conn = connectDB()
        
        self.start_buffer_flush_thread()
        with open('data.json', 'r') as file:
            self.Config = json.load(file)
        with open('plcConfig.json', 'r') as file:
            self.plcList = json.load(file)['plcs']
        for plc in self.plcList:
            self.machines_status[plc["nameMachine"]] = MachineStatus()
            self.machines_status[plc["nameMachine"]].clStartErrorTime = {}
    lock = threading.Lock()

    def start_buffer_flush_thread(self):
        threading.Thread(target=self.flush_buffer_periodically, daemon=True).start()

    def flush_buffer_periodically(self):
        while True:
            buffers_to_flush = {}
            # Chỉ lock khi copy buffer
            with self.lock:
                buffers_to_flush = self.buffers.copy()
                self.buffers.clear() 

            for nameMachine, buffer in buffers_to_flush.items():
                if buffer:
                    try:
                        self.conn.insert_machine_data(buffer, self.Config['UPH'])
                    except Exception as e:
                        print(f"Error updating CNT_MACHINE_SUMMARY database for {nameMachine}: {e}")

            time.sleep(100)
   
    def update_buffer_list(self, nameMachine, factory, line, machine_code, data_type, value, current_time):
        work_date = current_time.strftime("%Y-%m-%d %H")
        key = f"{factory},{line},{machine_code},{work_date}"
        if nameMachine not in self.buffers: 
            self.buffers[nameMachine] = {}
        with self.lock:
            if key not in self.buffers[nameMachine]:
                self.buffers[nameMachine][key] = {
                    "run_time": 0,
                    "error_time": 0,
                    "stop_time": 0,
                    "standby_time": 0,
                    "output": 0,
                    "THROW_QTY1": 0,
                    "THROW_QTY2": 0,
                    "PICK_QTY1" : 0,
                    "PICK_QTY2" : 0,
                    "THROW_QTY3": 0,
                    "THROW_QTY4": 0,
                    "PICK_QTY3" : 0,
                    "PICK_QTY4" : 0,
                    "NG_QTY" : 0,
                    "uph": 0
                }
            self.buffers[nameMachine][key][data_type] += value

    # Lấy trạng thái mãy định kì mỗi giờ
    def insert_time_default(self, plc, current_time):
        minutes = current_time.minute
        seconds = current_time.second
        if minutes == 59 and 50 <= seconds <= 58:
            if not plc.flag:
                # with self.lock:
                if plc.clStartIDLE is not None:
                    idle_time = (current_time - plc.clStartIDLE).total_seconds()
                    plc.clConnect= 0
                    if idle_time > 1:
                        print(f"Thời gian {current_time} máy {plc.clNameMachine} IDLE lúc 59:59s : {idle_time:.0f}s")
                        Config.writeLog(f'Thời gian {current_time} máy {plc.clNameMachine} IDLE lúc 59:59s : {idle_time:.0f}s')
                        self.update_buffer_list(plc.clNameMachine,self.Config['factory'],self.Config['line'],plc.clNameMachine,"standby_time",idle_time,current_time)
                        self.conn.insert_machine_data(self.buffers[plc.clNameMachine], self.Config['UPH'])
                        print(f'MẶC ĐỊNH 59:59 MÁY {plc.clNameMachine}')
                        self.buffers[plc.clNameMachine].clear()
                        plc.clStartIDLE = current_time

                if plc.clStartStopTime1 is not None:
                    timeerror = (current_time - plc.clStartStopTime1).total_seconds()
                    plc.clConnect= 0
                    if timeerror > 1:
                        print(f"Thời gian {current_time} máy {plc.clNameMachine} ERROR lúc 59:59s : {timeerror:.0f}s")
                        Config.writeLog(f'Thời gian {current_time} máy {plc.clNameMachine} ERROR lúc 59:59s : {timeerror:.0f}s')
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'],self.Config['line'],plc.clNameMachine,"error_time",timeerror,current_time)
                        self.conn.insert_machine_data(self.buffers[plc.clNameMachine], self.Config['UPH'])
                        print(f'MẶC ĐỊNH 59:59 MÁY {plc.clNameMachine}')
                        self.buffers[plc.clNameMachine].clear()
                        plc.clStartStopTime1 = current_time

                if plc.clStartStopTime is not None:
                    time_stop = (current_time - plc.clStartStopTime).total_seconds()
                    plc.clConnect= 0
                    if time_stop > 1:
                        print(f"Thời gian {current_time} máy {plc.clNameMachine} STOP lúc 59:59s : {time_stop:.0f}s")
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'],self.Config['line'],plc.clNameMachine,"stop_time",time_stop,current_time)
                        self.conn.insert_machine_data(self.buffers[plc.clNameMachine], self.Config['UPH'])
                        print(f'MẶC ĐỊNH 59:59 MÁY {plc.clNameMachine}')
                        self.buffers[plc.clNameMachine].clear()
                        plc.clStartStopTime = current_time

                if plc.clStartRunTime is not None:
                    time_run = (current_time - plc.clStartRunTime).total_seconds()
                    plc.clConnect= 0
                    if time_run > 1:
                        print(f"Thời gian {current_time} máy {plc.clNameMachine} RUN lúc 59:59s : {time_run:.0f}s")
                        Config.writeLog(f'Thời gian {current_time} máy {plc.clNameMachine} RUN lúc 59:59s : {time_run:.0f}s')
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'],self.Config['line'],plc.clNameMachine,"run_time",time_run,current_time)
                        self.conn.insert_machine_data(self.buffers[plc.clNameMachine], self.Config['UPH'])
                        print(f'MẶC ĐỊNH 59:59 MÁY {plc.clNameMachine}')
                        self.buffers[plc.clNameMachine].clear()
                        plc.clStartRunTime = current_time
                plc.flag = True
        else:
            plc.flag = False

    #xử lý trạng thái IDLE
    def handle_idle_state(self,plc, current_time, word_bit_IDLE, wordunits_errors ):
        try: 
            has_error = any(value == 1 for value in wordunits_errors)
            if word_bit_IDLE[0] == 1:
                if plc.clStartIDLE is None:
                    plc.clStartIDLE = current_time
                    plc.clIDLE = '1'
                    plc.clGreen = '1'
                    plc.clRed = '0'
                    plc.clYellow = '0'
                    plc.clStatus = 'NORMAL'
                    print(f'Máy bắt đầu IDLE {plc.clNameMachine} lúc {plc.clStartIDLE}')
                    self.conn.update_status(self.Config['factory'], self.Config['line'],plc.clNameMachine,self.Config['projectName'],plc.typeMachine,self.Config['UPH'],self.Config['ipServer'],self.Config['dbName'], '2')
                else:
                    plc.clIDLE = '1'
                    plc.clStatus = 'NORMAL'
            elif word_bit_IDLE[0] == 0 :
                if plc.clStartIDLE is not None:
                    idle_time = (current_time - plc.clStartIDLE).total_seconds()
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} IDLE: {idle_time:.0f}s")
                    self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "standby_time", idle_time, current_time)
                    plc.clStartIDLE = None
                else:

                    plc.clIDLE = '0'
        except Exception as ex:
            print(f'Error handle idle state: {ex}')
            Config.writeLog(f'Error handle idle state: {ex} máy {plc.clNameMachine}     {current_time}')
    #Xử lý trạng thái Error nhưng máy vẫn chạy bình thường
    def handle_error (self, plc, current_time, word_bit_light, wordunits_errors1 , word_bit_IDLE):  
        try:
            has_error = any(value == 1 for value in wordunits_errors1)
            if (word_bit_light[0] == 1 and word_bit_light[2] == 1 and word_bit_light[1] == 0 and word_bit_IDLE[0] == 0 and has_error):
                if plc.clStartStopTime1 is None:
                    plc.clGreen = '1'
                    plc.clYellow = '1'
                    plc.clRed = '0'
                    plc.clStartStopTime1 = current_time
                    plc.clStatus = 'WARNING'
                    print(f'Máy bắt đầu Error {plc.clNameMachine} lúc {plc.clStartStopTime1}')
                    self.conn.update_status(self.Config['factory'], self.Config['line'],plc.clNameMachine, self.Config['projectName'],plc.typeMachine, self.Config['UPH'], self.Config['ipServer'], self.Config['dbName'], '3')
                    # self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'ERROR')
                else:
                    plc.clGreen = '1'
                    plc.clYellow = '1'
                    plc.clStatus = 'WARNING'
            elif word_bit_IDLE[0] == 1 or  word_bit_light[1] == 1 or (word_bit_light[0] == 0 and word_bit_light[1] == 0 and word_bit_light[2] == 1 and word_bit_IDLE[0] == 0 ) or (word_bit_light[0] == 1 and word_bit_light[1] == 0 and word_bit_light[2] == 0 and word_bit_IDLE[0] == 0 ) or (word_bit_light[0] == 1 and word_bit_light[2] == 1 and word_bit_light[1] == 0 and word_bit_IDLE[0] == 0 and not has_error):
                if plc.clStartStopTime1 is not None:
                    timeerror = (current_time - plc.clStartStopTime1).total_seconds() 
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} Error: {timeerror:.0f}s")
                    plc.clStartStopTime1 = None
                    plc.clStatus = 'NORMAL'
                    self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "error_time", timeerror, current_time)
                else:
                    plc.clStatus = 'NORMAL'
        except Exception as ex:
            print(f'Error handle error state: {ex}')
            Config.writeLog(f'Error handle error state: {ex} máy {plc.clNameMachine}     {current_time}')
    #Xử lý trạng thái Stop
    def handle_stop_error(self, plc, current_time, word_bit_light, wordunits_errors1, word_bit_IDLE):
        try:
            has_error = any(value == 1 for value in wordunits_errors1)
            if (word_bit_light[0] == 0 and word_bit_light[1] == 0 and word_bit_light[2] ==1 and word_bit_IDLE[0] == 0 ) or (word_bit_light[1] == 1)  :                  
                if plc.clStartStopTime is None :
                    plc.clStartStopTime = current_time
                    if word_bit_light[1] == 1:
                        plc.clStatus = 'ERROR'
                        plc.clGreen = '0'
                        plc.clYellow = '0'
                        plc.clRed = '1'
                    else:
                        plc.clStatus = 'ERROR'
                        plc.clGreen = '0'
                        plc.clYellow = '1'
                        plc.clRed = '0'
                    print(f'Máy bắt đầu Stop {plc.clNameMachine} lúc {plc.clStartStopTime}')
                    self.conn.update_status(self.Config['factory'], self.Config['line'],plc.clNameMachine, self.Config['projectName'],plc.typeMachine, self.Config['UPH'], self.Config['ipServer'], self.Config['dbName'], '4')
                    # self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'STOP')
                else:
                    if word_bit_light[1] == 1:
                        plc.clStatus = 'ERROR'
                        plc.clGreen = '0'
                        plc.clYellow = '0'
                        plc.clRed = '1'
                    else:
                        plc.clStatus = 'ERROR'
                        plc.clGreen = '0'
                        plc.clYellow = '1'
                        plc.clRed = '0'
            elif (word_bit_light[0] == 1 and word_bit_light[2] == 1 and word_bit_light[1] == 0) or (word_bit_light[0] == 1 and word_bit_light[2] == 0 and word_bit_light[1] == 0) or word_bit_IDLE[0] == 1 or (word_bit_light[0] == 1 and word_bit_light[2] == 0 and word_bit_light[1] == 0 and word_bit_IDLE[0] == 1 ):
                if plc.clStartStopTime is not None:
                    time_stop = (current_time - plc.clStartStopTime).total_seconds()
                    print(f"Thời gian {current_time} máy {plc.clNameMachine} Stop : {time_stop:.0f}s")
                    plc.clStartStopTime = None
                    self.update_buffer_list(plc.clNameMachine, self.Config['factory'],self.Config['line'],plc.clNameMachine,"stop_time",time_stop,current_time)
        except Exception as ex:
            print(f'Error handle stop error state: {ex}')
            Config.writeLog(f'Error handle stop error state: {ex} máy {plc.clNameMachine}     {current_time}')
    #Xử lý lỗi
    def handle_error_state_combined(self, plc, current_time, listErrors, listErrorCode, wordunits_errors):
        try:
            listError = []
            if not hasattr(plc, 'clStartErrorTime'):
                plc.clStartErrorTime = {}
            
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
                            # self.conn.insert_error_timeon(self.Config['factory'], self.Config['line'], plc.clNameMachine, 'ERROR', listErrorCode[i + 1], listErrors[i + 1], plc.clStartErrorTime[i + 1], self.Config['owner'])
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
                    
                    print(f'Kết thúc lỗi {listErrors[i]} máy {plc.clNameMachine} mã {listErrorCode[i]} tại {end_time}. Thời gian lỗi: {error_duration}s')
                    # Cập nhật vào cơ sở dữ liệu
                    if error_duration > 2: 
                        # self.conn.update_error_on(self.Config['factory'], self.Config['line'], plc.clNameMachine, listErrorCode[i], end_time) 
                        self.conn.cnt_update_error_on(self.Config['factory'], self.Config['line'], plc.clNameMachine, listErrorCode[i], end_time)
                    else:
                        # self.conn.update_error_on1(self.Config['factory'], self.Config['line'], plc.clNameMachine, listErrorCode[i])
                        self.conn.cnt_update_error_on1(self.Config['factory'], self.Config['line'], plc.clNameMachine, listErrorCode[i])
                        print('Đã xóa lỗi < 2s khỏi DB!!')
            if not plc.clStartErrorTime:
                plc.clError = ''
        except Exception as ex:
            print(f'Error handle error state combined: {ex}')
            Config.writeLog(f'Error handle error state combined: {ex} máy {plc.clNameMachine}     {current_time}')
    #Xử lý trạng thái Run
    def handle_run_state(self,plc, current_time, wordunits_errors1, word_bit_light, word_bit_IDLE):
        try:
            has_error = any(value == 1 for value in wordunits_errors1)
            if (word_bit_light[0] ==1 and word_bit_light[1] ==0 and word_bit_light[2] ==0 and word_bit_IDLE[0] == 0 and not has_error) or (word_bit_light[0] == 1 and word_bit_light[2] == 1 and word_bit_light[1] == 0 and word_bit_IDLE[0] == 0 and not has_error):
                if plc.clStartRunTime is None:
                    plc.clStatus = 'NORMAL'
                    plc.clGreen = '1'
                    plc.clYellow = '0'
                    plc.clRed = '0'
                    plc.clStartRunTime = current_time
                    print(f"Máy {plc.clNameMachine} bắt đầu chạy lúc {plc.clStartRunTime.strftime('%Y-%m-%d %H:%M:%S')}")
                    self.conn.update_status(self.Config['factory'], self.Config['line'],plc.clNameMachine, self.Config['projectName'],plc.typeMachine, self.Config['UPH'], self.Config['ipServer'], self.Config['dbName'], '1')
                    # self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], plc.clNameMachine,'NORMAL')
                else:
                    plc.clStatus = 'NORMAL'
                    # plc.clError = ''
                    plc.clYellow = '0'
                    plc.clRed = '0'
            elif word_bit_IDLE[0] == 1 or (word_bit_light[0] ==0 and word_bit_light[2] ==1 and word_bit_light[1] ==0) or has_error or word_bit_light[1] == 1 or (word_bit_light[0] ==1 and word_bit_light[2] ==1 and word_bit_light[1] ==0 and has_error)  :
                if plc.clStartRunTime is not None:
                    timeRun = (current_time - plc.clStartRunTime).total_seconds()
                    print(f"Máy {plc.clNameMachine} hết chạy, tổng thời gian chạy: {timeRun:.0f}s")
                    plc.clStartRunTime = None
                    # Thêm bản ghi vào bộ đệm
                    self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "run_time", timeRun, current_time)
        except Exception as ex:
            print(f'Error handle run state: {ex}')
            Config.writeLog(f'Error handle run state: {ex} máy {plc.clNameMachine}     {current_time}')
    # Sản lượng pass, fail
    def handle_Product_Output(self, plc, currentTime ,word_bit_output, pymc3e):
        try:
            # Khởi tạo previous_output cho PLC nếu chưa có
            if plc.clNameMachine not in self.previous_output:
                self.previous_output[plc.clNameMachine] = {0: 0, 2: 0}
            if plc.clNameMachine not in self.previous_output_fail:
                self.previous_output_fail[plc.clNameMachine] = {1: 0, 3: 0}
            for i, output in enumerate(word_bit_output):
                if i == 0:
                    if output == 1 and self.previous_output[plc.clNameMachine][0] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "output", 5, currentTime)
                        # for _ in range(5):
                        #     self.conn.insert_production1(self.Config['factory'],self.Config['line'], plc.clNameMachine, 'PASS', currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} PASS tại {currentTime}')
                        plc.clConnect += 5
                    self.previous_output[plc.clNameMachine][0] = output
                    pymc3e.batchwrite_bitunits(headdevice="L5711", values=[0])
                if i == 1:
                    if output == 1 and self.previous_output_fail[plc.clNameMachine][1] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "NG_QTY", 1, currentTime)
                        # self.conn.insert_production1(self.Config['factory'],self.Config['line'], plc.clNameMachine, 'FAIL', currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} FAIL tại {currentTime}')
                    self.previous_output_fail[plc.clNameMachine][1] = output
                    pymc3e.batchwrite_bitunits(headdevice="L5712", values=[0])
                # if i == 2:
                #     if output == 1 and self.previous_output[plc.clNameMachine][2]== 0:
                #         self.pass_count[2] += 1
                #         # self.conn.insert_production_pass(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                #         self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "output", 5, currentTime)
                #         self.conn.insert_production1(self.Config['factory'],self.Config['line'], plc.clNameMachine, 'PASS', currentTime)
                #         print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} pass làn 2 tại {currentTime}')
                #         plc.clConnect += 1
                #     self.previous_output[plc.clNameMachine][2] = output
                #     pymc3e.batchwrite_bitunits(headdevice="L5213", values=[0])
                # if i == 3:
                #     if output == 1 and self.previous_output_fail[plc.clNameMachine][3] == 0:
                #         self.fail_count[3] += 1
                #         # self.conn.insert_production_fail(self.Config['factory'],self.Config['line'], plc.clNameMachine, self.Config['UPH'])
                #         self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "NG_QTY", 1, currentTime)
                #         self.conn.insert_production1(self.Config['factory'],self.Config['line'], plc.clNameMachine, 'FAIL', currentTime)
                #         print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} fail làn 2 tại {currentTime}')
                #     self.previous_output_fail[plc.clNameMachine][3] = output
                #     pymc3e.batchwrite_bitunits(headdevice="L5214", values=[0])
        except Exception as ex:
            print(f'Error handle Product Output machine {plc.clNameMachine}: {ex}')
            Config.writeLog(f'Error handle Product Output machine {plc.clNameMachine}: {ex}     {currentTime}')
    # Sản lượng pickup, throw
    def handl_pickup_throw(self, plc, currentTime, word_bit_pick, pymc3e):
        if plc.clNameMachine not in self.previous_pickup:
            self.previous_pickup[plc.clNameMachine] = {0: 0, 2: 0, 4: 0, 6: 0}
        if plc.clNameMachine not in self.previous_throw:
            self.previous_throw[plc.clNameMachine] = {1: 0, 3: 0, 5: 0, 7: 0}
        try:
            for i, pick in enumerate(word_bit_pick):
                if i == 0:
                    if pick == 1 and self.previous_pickup[plc.clNameMachine][0] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "PICK_QTY1", 10, currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} Pickup 1 tại {currentTime}')
                    self.previous_pickup[plc.clNameMachine][0] = pick
                    pymc3e.batchwrite_bitunits(headdevice="L5713", values=[0])
                if i == 1:
                    if pick == 1 and self.previous_throw[plc.clNameMachine][1] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "THROW_QTY1", 1, currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} Throw 1 tại {currentTime}')
                    self.previous_throw[plc.clNameMachine][1] = pick
                    pymc3e.batchwrite_bitunits(headdevice="L5714", values=[0])
                if i == 2:
                    if pick == 1 and self.previous_pickup[plc.clNameMachine][2] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "PICK_QTY2", 10, currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} Pickup 2 tại {currentTime}')
                    self.previous_pickup[plc.clNameMachine][2] = pick
                    pymc3e.batchwrite_bitunits(headdevice="L5715", values=[0])
                if i == 3:
                    if pick == 1 and self.previous_throw[plc.clNameMachine][3] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "THROW_QTY2", 1, currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} Throw 2 tại {currentTime}')
                    self.previous_throw[plc.clNameMachine][3] = pick
                    pymc3e.batchwrite_bitunits(headdevice="L5716", values=[0])
                if i == 4:
                    if pick == 1 and self.previous_pickup[plc.clNameMachine][4] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "PICK_QTY3", 10, currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} Pickup 3 tại {currentTime}')
                    self.previous_pickup[plc.clNameMachine][4] = pick
                    pymc3e.batchwrite_bitunits(headdevice="L5717", values=[0])
                if i == 5:
                    if pick == 1 and self.previous_throw[plc.clNameMachine][5] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "THROW_QTY3", 1, currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} Throw 3 tại {currentTime}')
                    self.previous_throw[plc.clNameMachine][5] = pick
                    pymc3e.batchwrite_bitunits(headdevice="L5718", values=[0])
                if i == 6:
                    if pick == 1 and self.previous_pickup[plc.clNameMachine][6] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "PICK_QTY4", 10, currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} Pickup 4 tại {currentTime}')
                    self.previous_pickup[plc.clNameMachine][6] = pick
                    pymc3e.batchwrite_bitunits(headdevice="L5719", values=[0])
                if i == 7:
                    if pick == 1 and self.previous_throw[plc.clNameMachine][7] == 0:
                        self.update_buffer_list(plc.clNameMachine, self.Config['factory'], self.Config['line'], plc.clNameMachine, "THROW_QTY4", 1, currentTime)
                        print(f'Máy {plc.clNameMachine} IP: {plc.clIpaddr} Throw 4 tại {currentTime}')
                    self.previous_throw[plc.clNameMachine][7] = pick  
                    pymc3e.batchwrite_bitunits(headdevice="L5720", values=[0]) 
        except Exception as ex:
            print(f'Error handl pickup throw machine {plc.clNameMachine}: {ex}')
            Config.writeLog(f'Error handl pickup throw machine {plc.clNameMachine}: {ex}      {currentTime}')
    # Read file config
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
        if plc.clNameMachine not in self.previous_cycle_time:
            self.previous_cycle_time[plc.clNameMachine] = 0  # Khởi tạo nếu chưa tồn tại
        try:
            if word_cycle_time[0] != 0  and self.previous_cycle_time[plc.clNameMachine] == 0:
                print(f'Cycle time máy {plc.clNameMachine} là {word_cycle_time[0]} lúc {current_time}')
                # self.conn.insert_cycle_time(self.Config['factory'],self.Config['line'], plc.clNameMachine, word_cycle_time[0], current_time)
                #Reset cycle time
                pymc3e.batchwrite_wordunits(headdevice="R5530", values=[0])
            self.previous_cycle_time[plc.clNameMachine] = word_cycle_time[0]
        except Exception as ex:
            print(f'Error handle cycle time: {ex}')
            
    # Xử lý kết nối lại PLC
    #Reset trạng thái máy 
    def reset_plc_status(self, machine_status):
        machine_status["clStartIDLE"] = None
        machine_status["clStartStopTime"] = None
        machine_status["clStartStopTime1"] = None
        machine_status["clStartRunTime"] = None
        print(f'Đã reset trạng thái máy sau khi kết nối lại thành công!!')
    def retry_connect_plc(self, plc, retry_count=5):
        attempts = 0
        while attempts < retry_count:
            try:
                ipPLC = plc["ip"]
                ipPort = plc["port"]
                nameMachine = plc["nameMachine"]
                typeMachine = plc["typeMachine"]
                pymc3e = pymcprotocol.Type3E()
                pymc3e.connect(ipPLC, ipPort)
                print(f'Kết nối lại thành công với PLC {nameMachine}')
                self.conn.update_status(self.Config['factory'], self.Config['line'],nameMachine, self.Config['projectName'],typeMachine, self.Config['UPH'], self.Config['ipServer'], self.Config['dbName'], '1')
                # self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], nameMachine,'NORMAL')                
                return pymc3e  # Kết nối lại thành công
            except Exception as ex:
                attempts += 1
                time.sleep(10)
                self.conn.update_status(self.Config['factory'], self.Config['line'],nameMachine, self.Config['projectName'],typeMachine, self.Config['UPH'], self.Config['ipServer'], self.Config['dbName'], '0')
                # self.conn.update_oracle_machine_status(self.Config['factory'], self.Config['line'], nameMachine,'PAUSE')
                print(f'Không thể kết nối lại với PLC {nameMachine}: {ex}. Thử lại lần {attempts}/{retry_count}.')
        print(f'Sau {retry_count} lần thử, không thể kết nối với PLC {nameMachine}. Dừng thử lại.')
        Config.writeLog(f'Sau {retry_count} lần thử, không thể kết nối với PLC {nameMachine}. Dừng thử lại.')
        return None
    #Xử lý dữ liệu của từng PLC
    def collect_data_from_plc(self,all_machines, plc, plc_connections, machines_status, Config1):
        while True:
            current_time = datetime.datetime.now().replace(microsecond=0)
            nameMachine = plc["nameMachine"]
            machine_status = machines_status.get(nameMachine)
            if machine_status is None:
                return
            try:
                with self.lock:
                    pymc3e = plc_connections.get(nameMachine)
                if pymc3e:
                    plc_connections[nameMachine] = pymc3e
                    listErrors = self.read_errors_from_txt('errorName.txt')
                    listErrorCode = self.read_errors_from_txt('errorCode.txt')
                    wordunits_errors = pymc3e.batchread_bitunits(headdevice="L5503", readsize=206)
                    word_bit_IDLE = pymc3e.batchread_bitunits(headdevice="L5710", readsize=1)
                    word_bit_output = pymc3e.batchread_bitunits(headdevice="L5711", readsize=2)
                    word_bit_pick_throw = pymc3e.batchread_bitunits(headdevice="L5713", readsize=8)
                    word_bit_light = pymc3e.batchread_bitunits(headdevice='L5500', readsize=3)
                    word_cycle_time = pymc3e.batchread_wordunits(headdevice="R5530", readsize=1)

                    machine_status.Cltime = str(current_time.strftime('%Y-%m-%d %H:%M:%S'))
                    self.insert_time_default(machine_status, current_time)
                    self.handle_idle_state(machine_status, current_time, word_bit_IDLE, wordunits_errors)
                    self.handle_error(machine_status, current_time, word_bit_light, wordunits_errors, word_bit_IDLE)
                    self.handle_error_state_combined(machine_status, current_time, listErrors, listErrorCode, wordunits_errors)
                    self.handle_stop_error(machine_status, current_time, word_bit_light, wordunits_errors, word_bit_IDLE)
                    self.handle_run_state(machine_status, current_time, wordunits_errors, word_bit_light, word_bit_IDLE)
                    self.handle_Product_Output(machine_status, current_time, word_bit_output, pymc3e)
                    self.handl_pickup_throw(machine_status, current_time, word_bit_pick_throw, pymc3e)
                    # self.handle_cycle_time(machine_status, current_time, word_cycle_time, pymc3e)
                    self.update_signal.emit(list(self.machines_status.values()))
                    
                    time.sleep(0.3)
            except Exception as ex:
                    time.sleep(5)
                    print(f'Lỗi kết nối với PLC {nameMachine}: {ex}      {current_time}')
                    Config.writeLog(f'Lỗi kết nối với PLC {nameMachine}: {ex}      {current_time}')
                    self.conn.update_status(Config1['factory'], Config1['line'], machine_status.clNameMachine, Config1['projectName'], machine_status.typeMachine, Config1['UPH'], Config1['ipServer'], Config1['dbName'], '0')
                    print(f'Đã update trạng thái plc {machine_status.clNameMachine}')    
                    # self.conn.update_oracle_machine_status(Config1['factory'], Config1['line'], machine_status.clNameMachine, 'PAUSE')
                    new_connection = self.retry_connect_plc(plc)
                    if new_connection is not None:
                        with self.lock:
                            plc_connections[nameMachine] = new_connection
                            machine_status.clStartIDLE = None
                            machine_status.clStartStopTime = None
                            machine_status.clStartStopTime1 = None
                            machine_status.clStartRunTime = None
                            print('Reset trạng thái máy sau khi kết nối lại thành công!!')
                    else:
                        print(f'Không thể kết nối lại với PLC {nameMachine}.')
                    
    #Khởi tạo kết nối list PLC
    def initialize_connections(self, plc_connections):
        for plc in self.plcList:
            try:
                ipPLC = plc["ip"]
                ipPort = plc["port"]
                typeMachine = plc["typeMachine"]
                nameMachine = plc["nameMachine"]
                pymc3e = pymcprotocol.Type3E()
                pymc3e.connect(ipPLC, ipPort)
                print(f'Kết nối thành công tới PLC {ipPLC}:{ipPort}')
                plc_connections[nameMachine] = pymc3e

                # Khởi tạo thông tin MÁY ban đầu
                machine_status = self.machines_status.get(nameMachine)
                if machine_status:
                    machine_status.clIpaddr = ipPLC
                    machine_status.ipPort = ipPort
                    machine_status.clNameMachine = nameMachine
                    machine_status.typeMachine = typeMachine
            except Exception as ex:
                print(f'Lỗi khởi tạo kết nối PLC {ipPLC}:{ipPort} {ex}')
    #Luồng xử lý all PLC
    def threadPLC(self):
        plc_connections = {}
        self.initialize_connections(plc_connections)
        all_machines = [self.machines_status[plc["nameMachine"]] for plc in self.plcList if plc["nameMachine"] in self.machines_status]
        for plc in self.plcList:
            nameMachine = plc["nameMachine"]
            if nameMachine in plc_connections:
                thread = threading.Thread(target=self.collect_data_from_plc, args=(all_machines, plc, plc_connections, self.machines_status, self.Config))
                thread.start()