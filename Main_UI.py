import sys
from PyQt5.QtWidgets import QApplication, QMainWindow
from GUI import Ui_MainWindow
from PyQt5.QtCore import QThread, Qt
from PyQt5 import QtWidgets
from mainLogic import MainLogic

class WorkerThread(QThread):
    def __init__(self, logic):
        super().__init__()
        self.logic = logic

    def run(self):
        self.logic.threadPLC()

class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.setting.clicked.connect(self.ui.opensetting)

        # Khởi tạo MainLogic
        self.mainLogic = MainLogic()
        self.mainLogic.update_signal.connect(self.updateUI)
        self.ui.lbStatus.setHidden(False)
        # Sử dụng QThread cho logic
        self.logic_thread = WorkerThread(self.mainLogic)
        self.logic_thread.start()

    def updateUI(self, listMachineStatus):
        self.ui.dgvMachineStatus.setRowCount(len(listMachineStatus))
        
        for row, machine in enumerate(listMachineStatus):
            self.ui.dgvMachineStatus.setItem(row, 0, QtWidgets.QTableWidgetItem(machine.clNameMachine))
            self.ui.dgvMachineStatus.setItem(row, 1, QtWidgets.QTableWidgetItem(machine.clStatus))
            self.ui.dgvMachineStatus.setItem(row, 2, QtWidgets.QTableWidgetItem(machine.clIpaddr))
            self.ui.dgvMachineStatus.setItem(row, 3, QtWidgets.QTableWidgetItem(machine.Cltime))
            self.ui.dgvMachineStatus.setItem(row, 4, QtWidgets.QTableWidgetItem(machine.clGreen))
            self.ui.dgvMachineStatus.setItem(row, 5, QtWidgets.QTableWidgetItem(machine.clYellow))
            self.ui.dgvMachineStatus.setItem(row, 6, QtWidgets.QTableWidgetItem(machine.clRed))
            self.ui.dgvMachineStatus.setItem(row, 7, QtWidgets.QTableWidgetItem(machine.clIDLE))
            self.ui.dgvMachineStatus.setItem(row, 8, QtWidgets.QTableWidgetItem(machine.clError))
            self.ui.dgvMachineStatus.setItem(row, 9, QtWidgets.QTableWidgetItem(machine.clConnect))

        # Điều chỉnh độ rộng của các cột theo nội dung
        self.adjustTableColumns()
        # Căn giữa nội dung cho các ô
        for col in range(10):  # Giả sử bạn có 9 cột
            item = self.ui.dgvMachineStatus.item(row, col)
            item.setTextAlignment(Qt.AlignCenter)
    def adjustTableColumns(self):
        self.ui.dgvMachineStatus.resizeColumnsToContents()
        total_width = sum(self.ui.dgvMachineStatus.columnWidth(i) for i in range(self.ui.dgvMachineStatus.columnCount()))
        table_width = self.ui.dgvMachineStatus.width()
        scale_factor = table_width / total_width if total_width > 0 else 1
        for i in range(self.ui.dgvMachineStatus.columnCount()):                   
            new_width = int(self.ui.dgvMachineStatus.columnWidth(i) * scale_factor)
            self.ui.dgvMachineStatus.setColumnWidth(i, new_width)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
