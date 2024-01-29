# -*- coding: utf-8 -*-
import requests

from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction, QMessageBox
from qgis.core import QgsSettings, QgsMessageLog, Qgis
from PyQt5.QtCore import pyqtSignal, QTimer
import boto3

from datetime import datetime
from decimal import Decimal
import sqlite3
from xml.etree import ElementTree
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction, QMessageBox
from qgis.core import QgsSettings, QgsMessageLog, Qgis, QgsProject, QgsDataSourceUri, QgsVectorLayer, QgsMapLayer
from qgis.gui import (QgisInterface, QgsOptionsWidgetFactory, QgsOptionsPageWidget)
from PyQt5.QtCore import pyqtSignal, QTimer
from PyQt5.QtWidgets import (QFileDialog, QGridLayout, QLabel, QLineEdit, QSpacerItem, 
    QSizePolicy, QPushButton, QSpinBox)

import os
import sys
import sqlite3

from .resources import *
from .globalvar import globalvar as gl
from .rosa_classes import RosaFunction, CanvasManager

class ROSA:
    """QGIS Plugin Implementation."""
    def __init__(self, iface):
        gl._init()
        gl.set_value('Rosa', self)
        # initialize CanvasManager
        self.canvas_manager = CanvasManager(self)
              
        # Save reference to the QGIS interface
        self.iface = iface
        self.actions = []
        self.menu = u'ROSA'
        # TODO: We are going to let the collobarator set this up in a future iteration
        self.toolbar = self.iface.addToolBar(u'ROSA')
        self.toolbar.setObjectName(u'ROSA')

    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""
        icon_path = ':/icon/elephant.png' 
        self.add_action(
            icon_path,
            #text=u'ROSA elephant',
            text=u'ROSA elephant',
            callback=self.elephant,
            parent=self.iface.mainWindow())            
        
        icon_path = ':/icon/whale.png'
        icon = QIcon(icon_path)
        action = QAction(icon, '自動更新', None)
        action.setEnabled(True)
        action.setCheckable(True)
        action.setChecked(False)
        action.triggered.connect(self.auto_update)
        self.auto_update_action = action
        self.iface.addPluginToMenu(u'ROSA', action)
        self.toolbar.addAction(action)
        self.actions.append(action)
            

    def auto_update(self):
        #automatic update
        if not hasattr(self, 'auto_update_timer'):
            self.auto_update_timer = QTimer()
            interval = QgsSettings().value('Rosa/AUTO_UPDATE_INTERVAL', 5, type=int)
            self.auto_update_timer.setInterval(interval * 1000)

        if self.auto_update_action.isChecked():
            self.auto_update_timer.timeout.connect(self.auto_update_)
            self.auto_update_timer.start()
            if not QgsProject.instance().mapLayersByName('rosa_disaster_report'):            
                gpkgpath  = r'C:\Users\jitin\Desktop\ROSA_WAREHOUSE\ROSA_MINERALDATA\rosa_common.gpkg'
                gpkglayer = gpkgpath + '''|layername=argu_town'''
                layer = QgsVectorLayer(gpkglayer, 'rosa_disaster_report', 'ogr')
                QgsProject.instance().addMapLayer(layer)

        else:
            self.auto_update_timer.stop()
            self.auto_update_timer.timeout.disconnect(self.auto_update_)
            # remove layer rosa_disaster_report
            QgsProject.instance().removeMapLayer(QgsProject.instance().mapLayersByName('rosa_disaster_report')[0].id())
            self.auto_update_ 
        
    def auto_update_(self):
      
        QgsMessageLog.logMessage('auto update: {}')

        for layer in QgsProject.instance().mapLayers().values():
            if layer.type() == QgsMapLayer.VectorLayer:
                layer.dataProvider().forceReload()
                layer.triggerRepaint()
                QgsMessageLog.logMessage('auto update: {}'.format(layer.name()), 'Rosa', Qgis.Info)

                       
    def initRosaFunction(self, function):
        if issubclass(function, RosaFunction):
            return function()

    #--------------------------------------------------------------------------
    def elephant(self):
        #check global variable
        #chekc if google variable has key 'Rosa'
        if gl.get_value('Rosa') == None:
            print ('Rosa is not')
        else:
            print ('Rosa is ok')

    def unload(self):
        #Removes the plugin menu item and icon from QGIS GUI."""
        #for reloading plugin

        for action in self.actions:
            self.iface.removePluginMenu(
                u'ROSA',
                action)
            self.iface.removeToolBarIcon(action)
        # remove the toolbar
        del self.toolbar
        
    def add_action(
        self,
        icon_path,
        text,
        callback,
        enabled_flag=True,
        add_to_menu=True,
        add_to_toolbar=True,
        status_tip=None,
        whats_this=None,
        parent=None):
        '''Add a toolbar icon to the toolbar.

        :param icon_path: Path to the icon for this action. Can be a resource
            path (e.g. ':/plugins/foo/bar.png') or a normal file system path.
        :type icon_path: str

        :param text: Text that should be shown in menu items for this action.
        :type text: str

        :param callback: Function to be called when the action is triggered.
        :type callback: function

        :param enabled_flag: A flag indicating if the action should be enabled
            by default. Defaults to True.
        :type enabled_flag: bool

        :param add_to_menu: Flag indicating whether the action should also
            be added to the menu. Defaults to True.
        :type add_to_menu: bool

        :param add_to_toolbar: Flag indicating whether the action should also
            be added to the toolbar. Defaults to True.
        :type add_to_toolbar: bool

        :param status_tip: Optional text to show in a popup when mouse pointer
            hovers over the action.
        :type status_tip: str

        :param parent: Parent widget for the new action. Defaults None.
        :type parent: QWidget

        :param whats_this: Optional text to show in the status bar when the
            mouse pointer hovers over the action.

        :returns: The action that was created. Note that the action is also
            added to self.actions list.
        :rtype: QAction
        '''

        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            self.toolbar.addAction(action)

        if add_to_menu:
            self.iface.addPluginToMenu(
                self.menu,
                action)

        self.actions.append(action)

        return action
