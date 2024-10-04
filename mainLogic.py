import sys
from PyQt5.QtCore import QObject, pyqtSignal, QThread
import time
from clMachineStatus import MachineStatus
import pymcprotocol
import json
import time, datetime
from PyQt5.QtWidgets import QApplication
from clConfig import Config
from connectDB import connectDB
class MainLogic(QObject):
    update_signal = pyqtSignal(list)  # Tín hiệu để cập nhật UI
    def __init__(self):
        super().__init__()
        self.plc = MachineStatus()
        self.hasPrintedError = False
        self.pass_count = {0: 0, 2: 0}
        self.fail_count = {1: 0, 3: 0}
        self.previous_output = {0: 0, 2: 0}
        self.previous_output_fail = {1: 0, 3: 0}
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
    def collect_machine_data(self, ipPLC, ipPort, nameMachine):
        machines = []
        pymc3e = pymcprotocol.Type3E()
        #kết nối lại nếu bị mất kết nối
        def try_connect(retry_delay):
            while True:
                try:
                    pymc3e.connect(ipPLC, ipPort)
                    print(f"Kết nối thành công tới máy {nameMachine} ({ipPLC}:{ipPort})")
                    break
                except Exception as ex:
                    print(f"Lỗi kết nối: {ex}. Đang thử kết nối lại sau {retry_delay} giây...")
                    time.sleep(retry_delay)
                    break
    
        try:
            # Kết nối tới PLC
            pymc3e.connect(ipPLC, ipPort)
            self.plc.clIpaddr = ipPLC
            self.plc.ipPort = ipPort
            self.plc.clNameMachine = nameMachine
            self.plc.Cltime = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            current_time = datetime.datetime.now().replace(microsecond=0)
            listErrors = self.read_errors_from_txt('errorName.txt')
            listErrorStop = self.read_errors_from_txt('errorStopName.txt')
            listErrorCode = self.read_errors_from_txt('errorCode.txt')
            wordunits_errors = pymc3e.batchread_bitunits(headdevice="L5000", readsize=206)
            word_bit_IDLE = pymc3e.batchread_bitunits(headdevice="L5207", readsize=1)
            word_bit_output = pymc3e.batchread_bitunits(headdevice="L5208", readsize=4)
            word_bit_pick_throw = pymc3e.batchread_bitunits(headdevice="L5212", readsize= 8)
            word_bit_light = pymc3e.batchread_bitunits(headdevice='L5221', readsize=3 )
            word_cycle_time = pymc3e.batchread_wordunits(headdevice="D1", readsize=1)
            self.handle_idle_state(current_time, word_bit_IDLE, wordunits_errors)
            self.handle_error(current_time, word_bit_light)
            self.handle_error_state_combined(current_time, listErrors, listErrorStop, listErrorCode, wordunits_errors)
            self.handle_stop_error(current_time, word_bit_light, wordunits_errors, word_bit_IDLE)
            self.handle_run_state(current_time, wordunits_errors, word_bit_light,word_bit_IDLE )
            self.handle_Product_Output(current_time, word_bit_output)
            self.handl_pickup_throw(current_time, word_bit_pick_throw)
            self.handle_cycle_time(current_time, word_cycle_time , pymc3e)
            machines.append(self.plc)
        except Exception as ex:
            print(f'Error collect data: {ex}')
            print(f'Đang thử kết nối lại {self.plc.clNameMachine}')
            time.sleep(3)
            try_connect(3)
        return machines
    
    #xử lý trạng thái IDLE
    def handle_idle_state(self, current_time, word_bit_IDLE, wordunits_errors):
        has_error = any(value == 1 for value in wordunits_errors)
        if word_bit_IDLE[0] == 1 and not has_error:
            if self.plc.clStartIDLE is None:
                self.plc.clStartIDLE = current_time  # Ghi nhận thời gian bắt đầu IDLE
                self.plc.clIDLE = '1'
                self.plc.clStatus = 'NORMAL'
                print(f'Máy bắt đầu IDLE {self.plc.clNameMachine} lúc {self.plc.clStartIDLE}')
            else:
                self.plc.clIDLE = '1'
                self.plc.clStatus = 'NORMAL'
        elif word_bit_IDLE[0] == 0 or  has_error :
            if self.plc.clStartIDLE is not None:
                time_idle = (current_time - self.plc.clStartIDLE).total_seconds()  # Tính thời gian IDLE
                print(f"Thời gian {current_time} máy {self.plc.clNameMachine} IDLE : {time_idle:.0f}s")
                self.plc.clStartIDLE = None  # Reset thời gian IDLE
                self.plc.clIDLE = '0'
                self.conn.insert_standby_time(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, time_idle)
            else:
                self.plc.clIDLE = '0'
    
    #Xử lý trạng thái Error nhưng máy vẫn chạy
    def handle_error (self, current_time, word_bit_light):
        if word_bit_light[0] == 1 and word_bit_light[1] == 1 and word_bit_light[2] == 0 :
            if self.plc.clStartStopTime1 is None:
                self.plc.clGreen = '1'
                self.plc.clYellow = '1'
                self.plc.clStartStopTime1 = current_time
                self.plc.clStatus = 'WARNING'
                print(f'Máy bắt đầu Error {self.plc.clNameMachine} lúc {self.plc.clStartStopTime1}')
            else:
                self.plc.clGreen = '1'
                self.plc.clYellow = '1'
                self.plc.clStatus = 'WARNING'
        else:
            self.plc.clGreen = '1'
            self.plc.clYellow = '1'
            if self.plc.clStartStopTime1 is not None:
                timeerror = (current_time - self.plc.clStartStopTime1).total_seconds() 
                print(f"Thời gian {current_time} máy {self.plc.clNameMachine} Error  : {timeerror:.0f}s")
                self.plc.clStartStopTime1 = None
                self.plc.clYellow = '0'
                self.plc.clStatus = 'NORMAL'
                self.conn.insert_error_time(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, timeerror)
            else:
                self.plc.clGreen = '1'
                self.plc.clYellow = '1'
                self.plc.clStatus = 'NORMAL'
    #Xử lý trạng thái Stop
    def handle_stop_error(self, current_time, word_bit_light, wordunits_errors, word_bit_IDLE):
        has_error = any(value == 1 for value in wordunits_errors)
        if (word_bit_light[1] == 1 and word_bit_light[0] == 0 and word_bit_light[2] == 0 and word_bit_IDLE[0] == 0) or word_bit_light[2] == 1 :
                self.plc.clGreen = '0'
                self.plc.clYellow = '1'
                self.plc.clRed = '0'                      
                if self.plc.clStartStopTime is None and wordunits_errors[187] == 0:
                    self.plc.clStartStopTime = current_time
                    self.plc.clStatus = 'WARNING'
                    print(f'Máy bắt đầu Stop {self.plc.clNameMachine} lúc {self.plc.clStartStopTime}')
                else:
                    self.plc.clYellow = '0'
                    self.plc.clStatus = 'NORMAL'
        elif (word_bit_light[0] == 1 and word_bit_light[1] == 1 and word_bit_light[2] == 0) or (has_error == False and word_bit_light[1] == 0) or word_bit_IDLE[0] == 1:
            if self.plc.clStartStopTime is not None:
                time_stop = (current_time - self.plc.clStartStopTime).total_seconds()  # Tính thời gian IDLE
                print(f"Thời gian {current_time} máy {self.plc.clNameMachine} Stop : {time_stop:.0f}s")
                self.plc.clStartStopTime = None  # Reset thời gian Stop
                self.conn.insert_stop_time(self.Config['factory'], self.Config['line'], self.plc.clNameMachine, time_stop)
                Config.writeLog(f'Time Error của máy {self.plc.clNameMachine} : {time_stop}s')
            else:
                self.plc.clYellow = ''

    def handle_error_state_combined(self, current_time, listErrors, listErrorStop, listErrorCode, wordunits_errors):
        listError = []
        if not hasattr(self.plc, 'clStartErrorTime'):
            self.plc.clStartErrorTime = {}  # Khởi tạo từ điển để lưu thời gian bắt đầu lỗi
        # Kiểm tra có lỗi hay không
        has_error = any(value == 1 for value in wordunits_errors)
        if has_error:
            self.plc.clYellow = '1'
            for i, error in enumerate(wordunits_errors):
                if error == 1:
                    listError.append(i+1)
                    # Lưu thời gian bắt đầu chỉ khi chưa được lưu
                    if (i + 1) not in self.plc.clStartErrorTime:
                        self.plc.clStartErrorTime[i + 1] = current_time  # Lưu thời gian bắt đầu
                        print(f'Thời gian lỗi bắt đầu của lỗi {listErrors[i + 1]} là {current_time}')
            errorCode = ', '.join(str(error) for error in listError)
            self.plc.clError = errorCode
        # Xử lý lỗi đã kết thúc
        for i in list(self.plc.clStartErrorTime.keys()):  # Duyệt qua các lỗi hiện có
            # Kiểm tra nếu lỗi đã hết (giá trị 0 tương ứng)
            if i <= len(wordunits_errors) and wordunits_errors[i - 1] == 0:
                # Tính thời gian kết thúc
                start_time = self.plc.clStartErrorTime.pop(i)  # Lấy và xóa thời gian bắt đầu
                end_time = current_time
                error_duration = (end_time - start_time).total_seconds()  # Tính thời gian đã trôi qua
                print(f'Lỗi {listErrors[i]} mã {listErrorCode[i]} và {i} kết thúc tại {end_time}. Thời gian lỗi: {error_duration} giây')
                Config.writeLog(f'Lỗi {listErrors[i]} mã {listErrorCode[i]} và {i} kết thúc tại {end_time}. Thời gian lỗi: {error_duration} giây')
        if not self.plc.clStartErrorTime:  # Nếu không còn lỗi nào
            self.plc.clError = ''
    #Xử lý trạng thái Run
    def handle_run_state(self, current_time, wordunits_errors, word_bit_light, word_bit_IDLE):
        has_error = any(value == 1 for value in wordunits_errors)
        if (word_bit_light[0] ==1 and word_bit_light[1] ==0 and word_bit_light[2] ==0 and not has_error and word_bit_IDLE[0] == 0):
            self.plc.clGreen = '1'
            self.plc.clYellow = '0'
            self.plc.clRed = '0'
            if self.plc.clStartRunTime is None:
                self.plc.clStatus = 'NORMAL'
                self.plc.clStartRunTime = current_time
                print(f"Máy {self.plc.clNameMachine} bắt đầu chạy lúc {self.plc.clStartRunTime.strftime('%Y-%m-%d %H:%M:%S')}")
                self.plc.hasPrintedError = True
            elif self.plc.hasPrintedError and self.plc.clStartRunTime is not None:
                self.plc.clStatus = 'NORMAL'
                self.plc.clError = '' 
        elif word_bit_IDLE[0] == 1 or (word_bit_light[0] ==0 and word_bit_light[1] ==1 and word_bit_light[2] ==0) or has_error or word_bit_light[2] == 1 or (word_bit_light[0] ==1 and word_bit_light[1] ==1 and word_bit_light[2] ==0) :
            if self.plc.clStartRunTime is not None:
                timeRun = (current_time - self.plc.clStartRunTime).total_seconds()
                print(f"Máy {self.plc.clNameMachine} hết chạy, tổng thời gian chạy: {timeRun:.0f}s")
                self.plc.clStartRunTime = None  # Reset thời gian lỗi
                self.plc.clStatus = "ERROR"
                self.plc.hasPrintedError = False
                self.conn.insert_on_time(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, timeRun)
            else:
                self.plc.clStatus = "ERROR"
                self.plc.hasPrintedError = False
    # Sản lượng pass, fail
    def handle_Product_Output(self,currentTime ,word_bit_output):
        for i, output in enumerate(word_bit_output):
            if i == 0:
                if output == 1 and self.previous_output[0] == 0:
                    self.pass_count[0] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} pass làn 1 tại {currentTime}: {self.pass_count[0]}')
                    self.conn.insert_production_pass(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_output[0] = output
            if i == 1:
                if output == 1 and self.previous_output_fail[1] == 0:
                    self.fail_count[1] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} fail làn 1 tại {currentTime}: {self.fail_count[1]}')
                    self.conn.insert_production_fail(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_output_fail[1] = output
            if i == 2:
                if output == 1 and self.previous_output[2] == 0:
                    self.pass_count[2] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} pass làn 2 tại {currentTime}: {self.pass_count[2]}')
                self.previous_output[2] = output
            if i == 3:
                if output == 1 and self.previous_output_fail[3] == 0:
                    self.fail_count[3] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} fail làn 2 tại {currentTime}: {self.fail_count[3]}')
                self.previous_output_fail[3] = output
    # Sản lượng pickup, throw
    def handl_pickup_throw(self,currentTime, word_bit_pick):
        for i, pick in enumerate(word_bit_pick):
            if i == 0:
                if pick == 1 and self.previous_pickup[0] == 0:
                    self.pass_pickup_count[0] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} Pickup 1 tại {currentTime}: {self.pass_pickup_count[0]}')
                    self.conn.insert_pickup_qty1(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_pickup[0] = pick
            if i == 1:
                if pick == 1 and self.previous_throw[1] == 0:
                    self.fail_throw_count[1] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} Throw 1 tại {currentTime}: {self.fail_throw_count[1]}')
                    self.conn.insert_throw_qty1(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_throw[1] = pick
            if i == 2:
                if pick == 1 and self.previous_pickup[2] == 0:
                    self.pass_pickup_count[2] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} Pickup 2 tại {currentTime}: {self.pass_pickup_count[2]}')
                    self.conn.insert_pickup_qty2(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_pickup[2] = pick
            if i == 3:
                if pick == 1 and self.previous_throw[3] == 0:
                    self.fail_throw_count[3] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} Throw 2 tại {currentTime}: {self.fail_throw_count[3]}')
                    self.conn.insert_throw_qty2(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_throw[3] = pick
            if i == 4:
                if pick == 1 and self.previous_pickup[4] == 0:
                    self.pass_pickup_count[4] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} Pickup 3 tại {currentTime}: {self.pass_pickup_count[4]}')
                    self.conn.insert_pickup_qty3(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_pickup[4] = pick
            if i == 5:
                if pick == 1 and self.previous_throw[5] == 0:
                    self.fail_throw_count[5] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} Throw 3 tại {currentTime}: {self.fail_throw_count[5]}')
                    self.conn.insert_throw_qty3(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_throw[5] = pick
            if i == 6:
                if pick == 1 and self.previous_pickup[6] == 0:
                    self.pass_pickup_count[6] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} Pickup 4 tại {currentTime}: {self.pass_pickup_count[6]}')
                    self.conn.insert_pickup_qty4(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_pickup[6] = pick
            if i == 7:
                if pick == 1 and self.previous_throw[7] == 0:
                    self.fail_throw_count[7] += 1
                    print(f'Máy {self.plc.clNameMachine} IP: {self.Config['ipServer']} Throw 4 tại {currentTime}: {self.fail_throw_count[7]}')
                    self.conn.insert_throw_qty4(self.Config['factory'],self.Config['line'], self.plc.clNameMachine, self.Config['UPH'])
                self.previous_throw[7] = pick   
    
    def read_errors_from_txt(self, file_path):
        errors_dict = {}
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                error_name, error_code = line.strip().split(',')
                errors_dict[int(error_code.strip())] = error_name.strip()
        return errors_dict
    # Xử lý cycle time
    def handle_cycle_time(self, current_time, word_cycle_time,pymc3e):
        if word_cycle_time[0] != 0  and self.previous_cycle_time ==0:
            print(f'Cycle time máy {self.plc.clNameMachine} là {word_cycle_time[0]} lúc {current_time}')
            self.conn.insert_cycle_time(self.Config['line'], self.plc.clNameMachine, word_cycle_time[0])
            #Reset cycle time = 0
            pymc3e.batchwrite_wordunits(headdevice="D1", values=[0])
        self.previous_cycle_time = word_cycle_time[0]
    #duyệt xử lý thu thấp data all PLC
    def threadPLC(self):
        while True:
            for plc in self.plcList:
                machines = self.collect_machine_data(plc["ip"], plc["port"], plc["nameMachine"])
                self.update_signal.emit(machines)
            time.sleep(0.1)

class WorkerThread(QThread):
    def __init__(self, logic):
        super().__init__()
        self.logic = logic

    def run(self):
        self.logic.threadPLC()
app = QApplication(sys.argv)
# Khởi tạo đối tượng MainLogic và WorkerThread để chạy luồng
main = MainLogic()
worker_thread = WorkerThread(main)
worker_thread.start()
sys.exit(app.exec_())
