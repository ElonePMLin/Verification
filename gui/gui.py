import sys
import os
import time
from PyQt5 import QtWidgets, QtCore, QtGui
from setupUi import Ui_MainWindow
from threading import Thread
import pandas as pd
from collections import defaultdict
import openpyxl
import multiprocessing


current_path = os.path.abspath(__file__)
top_path = str(os.sep).join(current_path.split(os.sep)[:-2])
sys.path.append(top_path)

from script.data_check import DataCheck, WriteOffCheck


class MainWindow(QtWidgets.QMainWindow):

    def __init__(self):
        super(MainWindow, self).__init__()
        self.ui = Ui_MainWindow()
        self.timer = QtCore.QTimer()
        self.ui.setupUi(self)
        self.initSlot()
        self.initStatus()
        self.dataPath = defaultdict(list)
        self.mei_tuan = ["文件路径", "工作表"]
        self.tiktok = ["文件路径", "工作表"]
        self.like = ["文件路径", "工作表"]
        self.write_off = ["文件路径", "工作表"]
        self.check = None
        self.expected = None
        self.wb = None

    def initStatus(self):
        self.ui.statusbar.showMessage("选择数据源")
        self.timer.start(1000)

    def initSlot(self):
        self.ui.addExcelAction.triggered.connect(self.on_addExcel_action)
        self.ui.resetAction.triggered.connect(self.on_reset_action)
        self.ui.removeAction.triggered.connect(self.on_remove_action)
        self.ui.dataSource.itemSelectionChanged.connect(self.dataSource_select_change)
        self.ui.runAction.triggered.connect(self.on_run_action)
        self.ui.saveAction.triggered.connect(self.on_save_action)
        self.timer.timeout.connect(self.save_able)

    def save_able(self):
        if self.check and self.check.done:
            self.ui.saveAction.setEnabled(True)
        else:
            self.ui.saveAction.setDisabled(True)

    def on_save_action(self):
        self.ui.saveAction.setChecked(False)
        file_dialog = QtWidgets.QFileDialog()
        message = QtWidgets.QMessageBox
        try:
            if absPath := file_dialog.getSaveFileUrl(self, filter="*.xlsx;; *.excel;; *.csv"):
                absPath = absPath[0].path()
                if sys.platform == "win32":
                    absPath = absPath[1:]
                elif sys.platform == "darwin":
                    absPath = absPath
                self.wb.save(absPath)
                message.about(None, "提示", "保存成功！")
        except Exception as e:
            print("保存文件：", e)
            message.warning(None, "提示", "保存失败！")

    def checkData(self):
        flag = True
        font = QtGui.QFont()
        font.setPointSize(16)
        font.setBold(True)
        if self.mei_tuan[0] == "文件路径":
            self.ui.m_statusLabel.setFont(font)
            self.ui.m_statusLabel.setText("请选择文件！")
            flag = False
        if self.tiktok[0] == "文件路径":
            self.ui.t_statusLabel.setFont(font)
            self.ui.t_statusLabel.setText("请选择文件！")
            flag = False
        if self.like[0] == "文件路径":
            self.ui.l_statusLabel.setFont(font)
            self.ui.l_statusLabel.setText("请选择文件！")
            flag = False
        if self.write_off[0] == "文件路径":
            self.ui.w_statusLabel.setFont(font)
            self.ui.w_statusLabel.setText("请选择文件！")
            flag = False
        return flag

    def setError(self, expected, check):
        font = QtGui.QFont()
        font.setPointSize(16)
        font.setBold(True)
        self.ui.m_statusLabel.setFont(font)
        self.ui.t_statusLabel.setFont(font)
        self.ui.l_statusLabel.setFont(font)
        self.ui.w_statusLabel.setFont(font)
        flag = True
        if expected.mei_tuan_error:
            self.ui.m_statusLabel.setText(expected.mei_tuan_error)
            flag = False
        else:
            self.ui.m_statusLabel.setText("有效数据")
        if expected.tiktok_error:
            self.ui.t_statusLabel.setText(expected.tiktok_error)
            flag = False
        else:
            self.ui.t_statusLabel.setText("有效数据")
        if expected.like_error:
            self.ui.l_statusLabel.setText(expected.like_error)
            flag = False
        else:
            self.ui.l_statusLabel.setText("有效数据")
        if check.message:
            self.ui.w_statusLabel.setText(check.message)
            flag = False
        else:
            self.ui.w_statusLabel.setText("有效数据")
        return flag

    def on_run_action(self):
        try:
            self.ui.runAction.setChecked(False)
            if not self.checkData():
                return
            # 开启线程进行数据校对，防止阻塞
            self.expected = DataCheck()
            self.expected.mei_tuan(*self.mei_tuan)
            self.expected.tiktok(*self.tiktok)
            self.expected.like(*self.like)
            self.check = WriteOffCheck(*self.write_off)
            if not self.setError(self.expected, self.check):
                self.expected = None
                self.check = None
                return
            QtCore.QThreadPool(self)
            thread = Thread(target=self.run_action_threading)
            thread.daemon = True
            thread.start()
            t = Thread(target=self.end_action_threading, args=(thread, ))
            t.daemon = True
            t.start()
        except Exception as e:
            print(e)

    def end_action_threading(self, thread):
        start = time.time()
        try:
            self.ui.statusbar.showMessage(f"数据核对中，请等待！")
            self.ui.progressBar.setMaximum(0)
            thread.join()
            self.ui.statusbar.showMessage(f"数据核对完成")
            self.ui.progressBar.setMaximum(100)
            # datas = self.check.original_data.drop(columns=["实际金额"]).fillna("")
            datas = self.check.original_data.fillna("")
            self.wb = openpyxl.Workbook()
            sheet = self.wb.create_sheet("数据核对")
            col = [chr(ord('A') + i) for i in range(len(datas.columns))]
            for i, c in enumerate(col):
                sheet[f'{c}1'] = list(datas.columns)[i]
            for idx, data in enumerate(datas.values):
                idx += 1
                for i, c in enumerate(col):
                    sheet[f'{c}{idx + 1}'] = data[i]
        except Exception as e:
            print("线程end", e)
            # print(self.check.original_data)

    def run_action_threading(self):
        try:
            data = pd.concat([self.expected.mei_tuan_data, self.expected.tiktok_data, self.expected.like_data]).dropna().drop_duplicates()
            self.check.first_check(data)
            self.check.second_check(data)
            self.check.final_check()
        except Exception as e:
            print("线程run", e)

    def setLabel(self, top, file, sheet, abs_path):
        if top == "大众美团":
            self.ui.m_fileLabel.setText(file)
            self.ui.m_sheetLabel.setText(sheet)
            self.mei_tuan = [abs_path, sheet]
        elif top == "抖音":
            self.ui.t_fileLabel.setText(file)
            self.ui.t_sheetLabel.setText(sheet)
            self.tiktok = [abs_path, sheet]
        elif top == "有赞":
            self.ui.l_fileLabel.setText(file)
            self.ui.l_sheetLabel.setText(sheet)
            self.like = [abs_path, sheet]
        else:
            self.ui.w_fileLabel.setText(file)
            self.ui.w_sheetLabel.setText(sheet)
            self.write_off = [abs_path, sheet]

    def dataSource_select_change(self):
        state = []
        selected = self.ui.dataSource.currentItem()
        pre = selected
        while pre and pre.parent():
            state.append(pre.text(0))
            pre = pre.parent()
        state.append(pre.text(0))
        topLevel = state[-1]
        self.ui.statusbar.showMessage("->".join(state[::-1]))
        # 最低层即工作表，需要处理数据
        if selected.childCount() == 0 and selected.parent():
            file = selected.parent()
            idx = pre.indexOfChild(file)
            sheet_name = selected.text(0)
            absPath = self.dataPath[topLevel][idx]
            # print(absPath, sheet_name)
            self.setLabel(topLevel, file.text(0), sheet_name, absPath)
        else:
            self.setLabel(topLevel, "文件名", "工作表", "文件名")

    def on_remove_action(self):
        self.ui.removeAction.setChecked(False)
        cur = self.ui.dataSource.currentItem()
        if cur.parent() and cur.childCount():
            # 选择表格
            file_name = cur.text(0)
        elif cur.parent():
            # 选择工作表
            cur = cur.parent()
            file_name = cur.text(0)
        else:
            if cur.childCount() == 0:
                return
            # 选择topLevel
            file_name = ""
            for idx in range(cur.childCount()):
                file_name += cur.child(idx).text(0) + '\n'
        message = QtWidgets.QMessageBox.warning(None, "提示", f"\n是否移除表格\n{file_name}！", QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        if message != 16384:
            return
        if cur.parent():
            topLevel = cur.parent()
            idx = topLevel.indexOfChild(cur)
            topLevel.takeChild(idx)
            self.dataPath[topLevel.text(0)].pop(idx)
            self.setLabel(topLevel.text(0), "文件名", "工作表", "文件名")
        else:
            self.dataPath[cur.text(0)] = []
            cur.takeChildren()
            self.setLabel(cur.text(0), "文件名", "工作表", "文件名")

    def on_reset_action(self):
        self.ui.resetAction.setChecked(False)
        message = QtWidgets.QMessageBox.warning(None, "提示", "\n是否进行重置！", QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        if message != 16384:
            return
        for idx in range(4):
            self.ui.dataSource.topLevelItem(idx).takeChildren()

    def on_addExcel_action(self):
        # 取消选择状态
        self.ui.addExcelAction.setChecked(False)
        # 获取最顶的item
        selected = self.ui.dataSource.currentItem()
        while selected.parent():
            selected = selected.parent()
        self.ui.statusbar.showMessage(selected.text(0))
        selected.setSelected(True)

        file_dialog = QtWidgets.QFileDialog(self)
        file_dialog.setFileMode(QtWidgets.QFileDialog.AnyFile)
        file_dialog.setNameFilter("Files(*.excel *.xlsx *.csv)")
        if file_dialog.exec_():
            absPath = file_dialog.selectedFiles()[0]
            if absPath in self.dataPath[selected.text(0)]:
                QtWidgets.QMessageBox.warning(None, "提示", "\n重复上传")
                return
            self.dataPath[selected.text(0)].append(absPath)
            file = absPath.split(os.sep)[-1].split(".")
            filename, suffix = ".".join(file[:-1]), file[-1]
            excelItem = QtWidgets.QTreeWidgetItem(selected)
            excelItem.setText(0, filename)
            excelIcon = QtGui.QIcon()
            excelIcon.addPixmap(QtGui.QPixmap(":/image/image/excel-full.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
            excelItem.setIcon(0, excelIcon)

            # 获取sheets
            if suffix == "csv":
                sheet_item = QtWidgets.QTreeWidgetItem(excelItem)
                sheet_item.setText(0, "默认工作表")
                sheetIcon = QtGui.QIcon()
                sheetIcon.addPixmap(QtGui.QPixmap(":/image/image/biaoge.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
                sheet_item.setIcon(0, sheetIcon)
            else:
                xls = pd.ExcelFile(absPath)
                sheet_list = xls.sheet_names
                for sheet_text in sheet_list:
                    sheet_item = QtWidgets.QTreeWidgetItem(excelItem)
                    sheet_item.setText(0, sheet_text)
                    sheetIcon = QtGui.QIcon()
                    sheetIcon.addPixmap(QtGui.QPixmap(":/image/image/biaoge.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
                    sheet_item.setIcon(0, sheetIcon)

            # 展开
            selected.setExpanded(True)
            selected.setSelected(False)
            excelItem.setExpanded(True)
        else:
            selected.setSelected(False)
            return None


if __name__ == '__main__':
    multiprocessing.freeze_support()
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
