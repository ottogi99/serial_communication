'''
마이크론 옵틱스 x25 모듈과 통신하여 피크(Peak) 정보와 레벨(Level)를 취득하는 모듈
- 명령 : #GET_PEAKS_AND_LEVELS
- 구조
--------------------------------------------------------------------------------------------------
Entry                       Byte Offset                             Atomic(bytes)   Total(bytes)
--------------------------------------------------------------------------------------------------
Timestamp (sec.)            0                                       4 (U32)         4
Timestamp (msec.)           4                                       4 (U32)         4
Serial Number               8                                       4 (U32)         4
#Peaks 1 (NS1)              12                                      2 (U16)         2
(Number of Sensor가 아닐까 추측함)
#Peaks 2 (NS2)              14                                      2 (U16)         2
#Peaks 3 (NS3)              16                                      2 (U16)         2
#Peaks 4 (NS4)              18                                      2 (U16)         2
Thermal Stability Falg      20                                      2 (U16)         2
MUX State                   22                                      -
Reserved                    24                                      1               8
Ch1 data                    32                                      4 (S32)         2*NS1
Ch2 data                    32+4*(NS1)                              4 (S32)         2*NS2
Ch3 data                    32+4*(NS1+NS2)                          4 (S32)         2*NS3
Ch4 data                    32+4*(NS1+NS2+NS3)                      4 (S32)         2*NS4
Ch1 levels                  32+4*(NS1+NS2+NS3+NS4)                  2 (S16)         NS1
Ch2 levels                  32+4*(NS1+NS2+NS3+NS4)+2*(NS1)          2 (S16)         NS2
Ch3 levels                  32+4*(NS1+NS2+NS3+NS4)+2*(NS1+NS2)      2 (S16)         NS3
Ch4 levels                  32+4*(NS1+NS2+NS3+NS4)+2*(NS1+NS2+NS3)  2 (S16)         NS4
--------------------------------------------------------------------------------------------------
'''


# import asyncore
# import socket
#
#
# class EchoClient(asyncore.dispatcher):
#     def __init__(self, host, port):
#         asyncore.dispatcher.__init__(self)
#         self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
#         self.connect((host, port))
#         self.messages = ['hi', 'hello', '안녕하세요!\n안녕']
#         self.seperator = b'\r\n\r\n'
#
#     def handle_connect(self):
#         pass
#
#     def handle_expt(self):
#         self.close()    # 연결 실패, 셧다운
#
#     def writable(self):
#         print('writable')
#         pass
#         # if self.messages:
#         #     msg = self.messages.pop(0)
#         #     self.send(msg.encode())
#         #     self.send(self.seperator)
#
#     def handle_read(self):
#         s = self.recv(2048)
#         print(s)
#         # for msg in s.split(self.seperator):
#         #     if msg:
#         #         print(msg.decode())
#
#     def handle_close(self):
#         self.close()
#
#
# request = EchoClient('192.168.10.178', 7002)
# asyncore.loop()


import socket
import sys
import struct
import time
import datetime
import logging
from logging import handlers
import numpy
import binascii
import Protocol

# 로그 포맷터 설정
fbgLogFormatter = logging.Formatter('%(asctime)s,%(message)s')

# 로그 핸들러 설정
fbgLogHandler = handlers.TimedRotatingFileHandler(filename='sm225.log', when='midnight', interval=1, encoding='utf-8')
fbgLogHandler.setFormatter(fbgLogFormatter)
fbgLogHandler.suffix = "%Y%m%d"

# 로거 설정
log = logging.getLogger()
log.setLevel(logging.INFO)
log.addHandler(fbgLogHandler)


class X25:
    def __init__(self, host, port):
        self.host = '10.0.0.1' if host is None else host
        self.port = 50000 if port is None else port
        self.peaks_and_levels = []
        self.socket = None

    def getPeakAndLevel(self):
        loop = 0
        # self.socket.connect((self.host, self.port))
        while loop < 20:
            self.socket.send('#GET_PEAKS_AND_LEVELS'.encode())
            log.info('Send command: {0}'.format('#GET_PEAKS_AND_LEVELS'.encode()))

            buffer = self.socket.recv(1024)
            # log.info('Received BINARY: {0}'.format(buffer))
            # log.info('Received BINARY length:'.format(len(buffer)))

            pal = Protocol.PeaksLevels(buffer)
            log.info('Received Peaks And Level: {0}'.format(pal))
            self.peaks_and_levels.append(pal)

            loop += 1
            time.sleep(0.3)

        if len(self.peaks_and_levels) > 0:
            for pal in self.peaks_and_levels:
                log.info("{0}".format(pal))

        return self.peaks_and_levels

    def connect(self):
        if self.socket is None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.socket.connect((self.host, self.port))
        except socket.error as e:
            print('Socket connect failed')
            print("Caught exception socket.error : {0}".format(e))

        print('Socket connected! {0}({1})'.format(self.host, self.port))

    def disconnect(self):
        self.socket.close()
        self.socket = None
        print('Disconnected')

    def userCommand(self, command: str):
        self.socket.send(command.encode())
        log.info('Send command: {0}'.format(command.encode()))
        buffer = self.socket.recv(1024)
        print('Response: {0}'.format(buffer))

    def help(self):
        self.userCommand('#HELP')

    def idn(self):
        self.userCommand('#IDN?')

    def getSystemImageId(self):
        self.userCommand('#GET_SYSTEM_IMAGE_ID')

    def getProductSn(self):
        self.userCommand('#GET_PRODUCT_SN')

    def setIpAddress(self, adr: str):
        self.userCommand('#SET_IP_ADDRESS {0}'.format(adr))

    def setIpNetMask(self, adr: str):
        self.userCommand('#SET_IP_NETMASK {0}'.format(adr))

    def getIpAddress(self, adr: str):
        self.userCommand('#GET_IP_ADDRESS {0}'.format(adr))

    def getIpNetMask(self, adr: str):
        self.userCommand('#GET_IP_ADDRESS {0}'.format(adr))

    def getDutState(self, ch: str):
        self.userCommand('#GET_DUT{0}_STATE'.format(ch))

    def setDutState(self, ch: str, val):
        self.userCommand('#SET_DUT{0}_STATE {1}'.format(ch, val))

    def rebootSystem(self):
        self.userCommand('#REBOOT_SYSTEM')

    def setPeakThresholdCh(self, ch: str, val):
        self.userCommand('#SET_PEAK_THRESHOLD_CH{0} {1}'.format(ch, val))

    def getPeakThresholdCh(self, ch: str):
        self.userCommand('#GET_PEAK_THRESHOLD_CH{0}'.format(ch))

    def setRelPeakThresholdCh(self, ch: str, val):
        self.userCommand('#SET_REL_PEAK_THRESHOLD_CH{0} {1}'.format(ch, val))

    def getRelPeakThresholdCh(self, ch: str):
        self.userCommand('#GET_REL_PEAK_THRESHOLD_CH{0}'.format(ch))

    def setPeakWidthCh(self, ch: str, val):
        self.userCommand('#SET_PEAK_WIDTH_CH{0} {1}'.format(ch, val))

    def getPeakWidthCh(self, ch: str):
        self.userCommand('#GET_PEAK_WIDTH_CH{0}'.format(ch))

    def setPeakWidthLevelCh(self, ch: str, val):
        self.userCommand('#SET_PEAK_WIDTH_LEVEL_CH{0} {1}'.format(ch, val))

    def getPeakWidthLevelCh(self, ch: str):
        self.userCommand('#GET_PEAK_WIDTH_LEVEL_CH{0}'.format(ch))

    # def __str__(self):
    #     _str = ""
    #     if len(self.peaks_and_levels) > 0:
    #         for pal in self.peaks_and_levels:
    #             _str += ("\n {0}".format(pal))
    #
    #     return _str



