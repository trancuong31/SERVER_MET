import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
from GUI import Ui_MainWindow
from PyQt5.QtCore import QThread
from PyQt5 import QtWidgets
from mainLogic import MainLogic
from connectDB import connectDB
from PyQt5.QtGui import QIcon
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
        self.db_connection = connectDB()
        self.ui.setupUi(self)
        self.setWindowIcon(QIcon('logo_met.png'))
        self.ui.setting.clicked.connect(self.ui.opensetting)
        self.ui.lbStatus.setHidden(True)
        # Khởi tạo MainLogic
        self.mainLogic = MainLogic()
        self.mainLogic.update_signal.connect(self.updateUI)
        self.logic_thread = WorkerThread(self.mainLogic)
        self.logic_thread.start()

    def updateUI(self, listMachineStatus):
        current_row_count = self.ui.dgvMachineStatus.rowCount()
        new_row_count = len(listMachineStatus)
        if new_row_count > current_row_count:
            self.ui.dgvMachineStatus.setRowCount(new_row_count)
        scroll_position = self.ui.dgvMachineStatus.verticalScrollBar().value()
        for row, machine in enumerate(listMachineStatus):
            # print(f"Machine {machine.clNameMachine} Error: {machine.clError}")
            self.ui.dgvMachineStatus.setItem(row, 0, QtWidgets.QTableWidgetItem(machine.clNameMachine))
            self.ui.dgvMachineStatus.setItem(row, 1, QtWidgets.QTableWidgetItem(machine.clStatus))
            self.ui.dgvMachineStatus.setItem(row, 2, QtWidgets.QTableWidgetItem(machine.clIpaddr))
            self.ui.dgvMachineStatus.setItem(row, 3, QtWidgets.QTableWidgetItem(machine.Cltime))
            self.ui.dgvMachineStatus.setItem(row, 4, QtWidgets.QTableWidgetItem(machine.clGreen))
            self.ui.dgvMachineStatus.setItem(row, 5, QtWidgets.QTableWidgetItem(machine.clYellow))
            self.ui.dgvMachineStatus.setItem(row, 6, QtWidgets.QTableWidgetItem(machine.clRed))
            self.ui.dgvMachineStatus.setItem(row, 7, QtWidgets.QTableWidgetItem(machine.clIDLE))
            self.ui.dgvMachineStatus.setItem(row, 8, QtWidgets.QTableWidgetItem(str(machine.clError)))
            self.ui.dgvMachineStatus.setItem(row, 9, QtWidgets.QTableWidgetItem(str(machine.clConnect)))

        self.ui.dgvMachineStatus.setColumnWidth(0, 100)
        self.ui.dgvMachineStatus.setColumnWidth(1, 100)
        self.ui.dgvMachineStatus.setColumnWidth(2, 100)
        self.ui.dgvMachineStatus.setColumnWidth(3, 150)
        self.ui.dgvMachineStatus.setColumnWidth(4, 100)
        self.ui.dgvMachineStatus.setColumnWidth(5, 100)
        self.ui.dgvMachineStatus.setColumnWidth(6, 100)
        self.ui.dgvMachineStatus.setColumnWidth(7, 100)
        self.ui.dgvMachineStatus.setColumnWidth(8, 235)
        self.ui.dgvMachineStatus.setColumnWidth(9, 100)

        self.ui.dgvMachineStatus.verticalScrollBar().setValue(scroll_position)
    def closeEvent(self, event):
        # Hiển thị hộp thoại xác nhận
        reply = QMessageBox.question(
            self,
            "Cảnh báo quan trọng!",
            "⚠️ Nếu bạn tắt chương trình này, hệ thống sẽ không thể theo dõi trạng thái máy móc. \n\nBạn có chắc chắn muốn thoát không?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            self.db_connection.close_connection()
            event.accept() 
        else:
            event.ignore() 
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.setWindowIcon(QIcon('logo_met.png'))
    window.show()
    
    sys.exit(app.exec_())
                                                                                                                                                                                