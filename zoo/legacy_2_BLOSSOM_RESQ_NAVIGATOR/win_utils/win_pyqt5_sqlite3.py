#coding=utf-8
import sys
import sqlite3
from PyQt5.QtWidgets import QApplication, QMainWindow, QMenu, QMenuBar, QAction
from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QFileDialog, QInputDialog
from PyQt5.QtWidgets import QMessageBox

class MainWindow(QMainWindow):
    
    def __init__(self):
        super().__init__()

        self.setWindowTitle("SQLite Browser")
        self.setGeometry(100, 100, 600, 400)

        self.table_widget = QTableWidget(self)
        self.setCentralWidget(self.table_widget)

        self.create_menu()
        self.selected_table = None
        self.selected_rows = []

    def create_menu(self):
        menu_bar = self.menuBar()

        file_menu = QMenu("File", self)
        open_action = QAction("Open", self)
        open_action.triggered.connect(self.open_file)
        file_menu.addAction(open_action)

        close_action = QAction("Close", self)
        close_action.triggered.connect(self.close_file)
        file_menu.addAction(close_action)

        delete_action = QAction("Delete", self)
        delete_action.triggered.connect(self.delete_table)
        file_menu.addAction(delete_action)

        menu_bar.addMenu(file_menu)

        browse_menu = QMenu("Browse", self)
        browse_action = QAction("Show Table", self)
        browse_action.triggered.connect(self.browse_table)
        browse_menu.addAction(browse_action)

        menu_bar.addMenu(browse_menu)

    def open_file(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(self, "Select SQLite File", "", "SQLite Files (*.db *.sqlite *.sqlite3)")
        if file_path:
            self.file_path = file_path
            self.db_connection = sqlite3.connect(file_path)

    def close_file(self):
        if hasattr(self, 'db_connection'):
            self.db_connection.close()

    def delete_table(self):
        if hasattr(self, 'db_connection'):
            if self.selected_rows:
                table_name = self.selected_table
                cursor = self.db_connection.cursor()
                for row in self.selected_rows:
                    row_id = self.table_widget.item(row, 0).text()  
                    cursor.execute("DELETE FROM {} WHERE file_path = ?".format(table_name), (row_id,))
                self.db_connection.commit()
                self.browse_table()
            else:
                QMessageBox.warning(self, "No Rows Selected", "Please select at least one row to delete.")

 
    def browse_table(self):
        if hasattr(self, 'db_connection'):
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]

            table_name, ok = QInputDialog.getItem(self, "Select Table", "Please select a table to browse:", table_names, 0, False)
            if ok and table_name:
                self.selected_table = table_name
                self.setWindowTitle(f'SQLite Browser : {table_name}@{self.file_path}')
                cursor.execute("SELECT * FROM {}".format(table_name))
                data = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]

                self.table_widget.clear()
                self.table_widget.setRowCount(len(data))
                self.table_widget.setColumnCount(len(data[0]))
                self.table_widget.setHorizontalHeaderLabels(column_names)

                self.table_widget.itemSelectionChanged.connect(self.update_selected_rows)  

                for row_num, row_data in enumerate(data):
                    for col_num, value in enumerate(row_data):
                        self.table_widget.setItem(row_num, col_num, QTableWidgetItem(str(value)))

    def update_selected_rows(self):
        self.selected_rows = [index.row() for index in self.table_widget.selectedIndexes()]



if __name__ in [ '__main__', '__console__']: 
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
