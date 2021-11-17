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

#log settings
fbgLogFormatter = logging.Formatter('%(asctime)s,%(message)s')

#handler settings
fbgLogHandler = handlers.TimedRotatingFileHandler(filename='sm225.log', when='midnight', interval=1, encoding='utf-8')
fbgLogHandler.setFormatter(fbgLogFormatter)
fbgLogHandler.suffix = "%Y%m%d"

#logger set
fbgLogger = logging.getLogger()
fbgLogger.setLevel(logging.INFO)
fbgLogger.addHandler(fbgLogHandler)


class DataFrame:
    CONST_MUX_STATE = {
        0: [7, 9, 7, 10],
        1: [7, 8, 7, 7],
        2: [7, 11, 7, 2],
        3: [4, 2, 4, 2],
    }

    # CONST_MUX_STATE = {
    #     0: [7, 9, 7, 10],
    #     1: [7, 1, 7, 7],
    #     2: [7, 11, 7, 2],
    #     3: [4, 2, 4, 2],
    # }

    # 채널: FBG(MIN, MAX)
    # 1.1 : 7 FBGs, 1.2 : 7 FBGs, 1.3 : 7 FBGs, 1.4 : 4 FBGs
    # 2.1 : 9 FBGs, 2.2 : 8 FBGs, 2.3 : 11 FBGs, 2.4 : 2 FBGs
    # 3.1 : 7 FBGs, 3.2 : 7 FBGs, 3.3 : 7 FBGs, 3.4 : 4 FBGs
    # 4.1 : 10 FBGs, 4.2 : 7 FBGs, 4.3 : 2 FBGs, 4.4 : 2 FBGs
    WAVELENGTH_RANGE = {
        # '1.1': [(1513.060, 1523.060), (1531.221, 1539.216), (1539.217, 1543.790), (1543.082, 1548.990), (1549.824, 1555.732), (1555.733, 1523.395), (1564.518, 1569.180)],  #A1-A7
        # '1.2': [(1516.137, 1526.137), (1532.418, 1539.360), (1538.283, 1545.225), (1545.226, 1551.211), (1551.212, 1556.098), (1556.099, 1562.241), (1562.242, 1571.438)],  #E1-E7
        # '1.3': [(1522.026, 1530.218), (1532.902, 1538.963), (1538.912, 1546.355), (1543.725, 1550.292), (1549.473, 1556.040), (1555.640, 1562.207), (1561.695, 1568.262)],  #I1-I7
        # '1.4': [(1525.827, 1535.142), (1535.103, 1542.388), (1542.439, 1550.409), (1551.743, 1561.743)], #M1-M4
        # '2.1': [(1509.003,	1519.003), (1533.359, 1539.358), (1539.604, 1547.576), (1545.216, 1551.896), (1550.972, 1557.652), (1555.633, 1562.465), (1562.466, 1569.541), (1569.542, 1575.541), (1575.542, 1583.514)], #B1-B9
        # '2.2': [(1507.150, 1514.104), (1514.105, 1521.059), (1526.922, 1535.514), (1535.515, 1540.611), (1540.612,	1546.638), (1546.639, 1552.745), (1555.603, 1561.629), (1558.471, 1564.577)], #F1-F8
        # '2.3': [(1509.076, 1519.076), (1525.377, 1533.260), (1533.261, 1537.747), (1537.748, 1540.777), (1540.778, 1545.071), (1545.072, 1551.056), (1551.057, 1555.591), (1555.592, 1561.506), (1561.507, 1568.981), (1568.982, 1573.500), (1573.501, 1580.044)], #J1-J11 (J2, J4만 씀)
        # '2.4': [(1537.307, 1547.307), (1552.209, 1562.209)], #N1-N2
        # '3.1': [(1522.006, 1530.112), (1530.113, 1537.003), (1537.004, 1544.596), (1544.597, 1551.264), (1551.265, 1557.107), (1557.108, 1561.796), (1562.898, 1567.586)], #C1-C7
        # '3.2': [(1513.076, 1523.076), (1531.236, 1537.651), (1537.223, 1543.638), (1543.578, 1549.666), (1549.691, 1555.779), (1555.749, 1561.764), (1561.765, 1568.280)], #G1-G7
        # '3.3': [(1516.084, 1526.084), (1531.600, 1537.945), (1537.190, 1543.535), (1543.937, 1551.486), (1551.487, 1557.258), (1557.259, 1563.133), (1563.134, 1571.140)], #K1-K7
        # '3.4': [(1524.786, 1534.345), (1534.346, 1542.003), (1542.004, 1550.102), (1552.459, 1562.459)], #O1-O4
        # '4.1': [(1508.976,	1515.007), (1515.008, 1521.040), (1530.930, 1537.535), (1537.055, 1543.660), (1543.579,	1549.889), (1552.367, 1558.483), (1556.007, 1563.263), (1563.264, 1569.283), (1569.284, 1575.240), (1575.241, 1583.194)], # D1 - D10
        # '4.2': [(1507.107, 1517.107), (1527.713, 1534.621), (1533.557, 1540.465), (1540.571, 1547.627), (1547.628, 1552.882), (1552.896, 1559.952), (1559.497, 1564.751)], #H1-H7
        # '4.3': [(1536.993, 1546.993), (1552.331, 1562.331)], #L1-L2
        # '4.4': [(1537.040, 1547.040), (1552.765, 1562.765)], #P1-P2
        '1.1': [(1513.060, 1523.060), (1531.221, 1539.216), (1539.217, 1543.790), (1543.791, 1549.823), (1549.824, 1555.732), (1555.733, 1523.395), (1560.396, 1568.604)],  # A1-A7
        '1.2': [(1516.137, 1526.137), (1529.384, 1538.282), (1538.283, 1545.225), (1545.226, 1551.211), (1551.212, 1556.098), (1556.099, 1562.241), (1562.242, 1571.438)],  #E1-E7
        '1.3': [(1522.026, 1530.218), (1530.219, 1536.28), (1536.281, 1543.724), (1543.725, 1550.292), (1550.293, 1554.790), (1554.791, 1560.941), (1560.942, 1569.586)],  #I1-I7
        '1.4': [(1525.827, 1535.142), (1535.103, 1542.388), (1542.389, 1550.359), (1542.439, 1550.409)],
        '2.1': [(1509.003,	1519.003), (1528.402, 1536.443), (1536.444, 1542.270), (1542.271, 1548.951), (1548.952, 1555.632), (1555.633, 1562.465), (1562.466, 1569.541), (1569.542, 1575.541), (1575.542, 1583.514)], #B1-B9
        '2.2': [(1507.150, 1514.104), (1514.105, 1521.059), (1526.922, 1535.514), (1535.515, 1540.611), (1540.612,	1546.638), (1546.639, 1552.745), (1552.746, 1557.608), (1557.609, 1565.888)], #F1-F8
        '2.3': [(1509.076, 1519.076), (1525.377, 1533.260), (1533.261, 1537.747), (1537.748, 1540.777), (1540.778, 1545.071), (1545.072, 1551.056), (1551.057, 1555.591), (1555.592, 1561.506), (1561.507, 1568.981), (1568.982, 1573.500), (1573.501, 1580.044)], #J1-J11 (J2, J4만 씀)
        '2.4': [(1537.267, 1547.267), (1552.209, 1562.209)], #N1-N2
        '3.1': [(1522.006, 1530.112), (1530.113, 1537.003), (1537.004, 1544.596), (1544.597, 1551.264), (1551.265, 1557.107), (1557.108, 1561.796), (1561.797, 1568.502)], #C1-C7
        '3.2': [(1513.076, 1523.076), (1531.236, 1537.651), (1537.652, 1543.577), (1543.578, 1549.666), (1549.667, 1555.748), (1555.749, 1561.764), (1561.765, 1568.280)], #G1-G7
        '3.3': [(1516.084, 1526.084), (1531.600, 1537.945), (1537.946, 1543.936), (1543.937, 1551.486), (1551.487, 1557.258), (1557.259, 1563.133), (1563.134, 1571.140)], #K1-K7
        '3.4': [(1524.786, 1534.345), (1534.346, 1542.003), (1542.004, 1550.102), (1552.459, 1562.459)], #O1-O4
        '4.1': [(1508.976, 1515.007), (1515.008, 1521.040), (1530.930, 1537.535), (1537.536, 1543.578), (1543.579, 1549.889), (1549.890, 1556.006), (1556.007, 1563.263), (1563.264, 1569.283), (1569.284, 1575.240), (1575.241, 1583.194)], # D1 - D10
        '4.2': [(1507.107, 1517.107), (1527.713, 1534.621), (1534.622, 1540.570), (1540.571, 1547.627), (1547.628, 1552.882), (1552.883, 1557.905), (1557.906, 1565.690)], #H1-H7
        '4.3': [(1536.993, 1546.993), (1552.331, 1562.331)], #L1-L2
        '4.4': [(1537.040, 1547.040), (1552.765, 1562.765)], #P1-P2
    }


    @staticmethod
    def int_from_bytes(bytes_data):
        return int.from_bytes(bytes_data, byteorder='little')

    def __init__(self, frame):
        self.frame = frame
        self.timestamp = ''
        self.datetime = ''
        self.serial_number = ''
        self.peaks = []
        self.thermal_stability_flag = ''
        self.mux_state = ''
        self.reserved = ''
        self.fbg_values = {
            'Ch1': [],
            'Ch2': [],
            'Ch3': [],
            'Ch4': [],
        }
        self.fbg_levels = []

    def set_fbg_values(self):
        try:
            self.timestamp = self.int_from_bytes(self.frame[0:4]) + self.int_from_bytes(self.frame[4:8]) / 1000000
            self.datetime = datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            # print('timestamp:{0}'.format(self.timestamp))
            # print('datetime:{0}'.format(self.datetime))
            self.serial_number = self.int_from_bytes(self.frame[8:12])
            self.peaks = [
                self.int_from_bytes(self.frame[12:14]),
                self.int_from_bytes(self.frame[14:16]),
                self.int_from_bytes(self.frame[16:18]),
                self.int_from_bytes(self.frame[18:20]),
            ]
            self.thermal_stability_flag = self.int_from_bytes(self.frame[20:22])
            self.mux_state = self.int_from_bytes(self.frame[22:24])
            self.reserved = self.int_from_bytes(self.frame[24:32])

            if self.peaks[0] < 1 or self.peaks[1] < 1 or self.peaks[2] < 1 or self.peaks[3] < 1:
                return -1

            if self.peaks[0] > 0:   # Peaks1 = NS1 = self.peaks[0]
                print('{0}:{1}'.format(self.peaks[0], self.CONST_MUX_STATE[self.mux_state][0]))
                if self.peaks[0] == self.CONST_MUX_STATE[self.mux_state][0]:    # 프레임에서 받은 fbg 개수와 실제 설정된 fbg 개수와 비교 (간혹 fbg 개수가 틀리게 들어오는 경우가 있음)
                    for i in range(0, self.peaks[0]):
                        value = self.int_from_bytes(self.frame[32 + (4 * i):32 + (4 * (i + 1))]) / 10000
                        self.fbg_values['Ch1'].append(value)
                else:
                    fbgLogger.warning('Different peaks1 count: Defined:{0}, Received:{1}'.format(self.CONST_MUX_STATE[self.mux_state][0], self.peaks[0]))
                    key = '1.{0}'.format(self.get_mux_state())
                    wavelength = self.WAVELENGTH_RANGE[key]

                    # 먼저 temp_values에 값을 저정한다.
                    temp_values = []
                    for i in range(0, self.peaks[0]):
                        value = self.int_from_bytes(self.frame[32 + (4 * i):32 + (4 * (i + 1))]) / 10000
                        temp_values.append(value)

                    # print('temp_values: {0}'.format(temp_values))
                    # print('wavelength: {0}'.format(wavelength))

                    for i in range(0, len(wavelength)):
                        self.fbg_values['Ch1'].append(None)
                        for value in temp_values:
                            if wavelength[i][0] < value < wavelength[i][1]:
                                # print('{0} < {1} < {2}'.format(wavelength[i][0], value, wavelength[i][1]))
                                self.fbg_values['Ch1'][i] = value
                                break

                    # print('Ch1_data: {0}'.format(self.fbg_values['Ch1']))

            if self.peaks[1] > 0:   # Peaks2 = NS2 = self.peaks[1]
                print('Ch{2}.{3} -> {0}:{1}'.format(self.peaks[1], self.CONST_MUX_STATE[self.mux_state][1], 2, self.get_mux_state()))
                if self.peaks[1] == self.CONST_MUX_STATE[self.mux_state][1]:    # 프레임에서 받은 fbg 개수와 실제 설정된 fbg 개수와 비교 (간혹 fbg 개수가 틀리게 들어오는 경우가 있음)
                    for i in range(0, self.peaks[1]):
                        value = self.int_from_bytes(self.frame[32 + (4 * self.peaks[0]) + (4 * i):32 + (4 * self.peaks[0]) + 4 * (i + 1)]) / 10000
                        self.fbg_values['Ch2'].append(value)
                else:
                    fbgLogger.warning('Different peaks2 count: Defined:{0}, Received:{1}'.format(self.CONST_MUX_STATE[self.mux_state][1], self.peaks[1]))
                    print('Different peaks2 count: Defined:{0}, Received:{1}'.format(self.CONST_MUX_STATE[self.mux_state][1], self.peaks[1]))
                    key = '2.{0}'.format(self.get_mux_state())
                    wavelength = self.WAVELENGTH_RANGE[key]
                    # print(wavelength)

                    # 먼저 temp_values에 값을 저정한다.
                    temp_values = []
                    for i in range(0, self.peaks[1]):
                        value = self.int_from_bytes(self.frame[32 + (4 * self.peaks[0]) + (4 * i):32 + (4 * self.peaks[0]) + 4 * (i + 1)]) / 10000
                        temp_values.append(value)


                    print('temp_values: {0}'.format(temp_values))
                    # print('wavelength: {0}'.format(wavelength))

                    for i in range(0, len(wavelength)):
                        self.fbg_values['Ch2'].append(None)
                        for value in temp_values:
                            # print('{0} < {1} < {2}'.format(wavelength[i][0], value, wavelength[i][1]))
                            if wavelength[i][0] < value < wavelength[i][1]:
                                self.fbg_values['Ch2'][i] = value
                                break

                    # print('Ch2_data: {0}'.format(self.fbg_values['Ch2']))

            if self.peaks[2] > 0:   # Peaks3 = NS3 = self.peaks[2]
                # print('{0}:{1}'.format(self.peaks[2], self.CONST_MUX_STATE[self.mux_state][2]))
                if self.peaks[2] == self.CONST_MUX_STATE[self.mux_state][2]:  # 프레임에서 받은 fbg 개수와 실제 설정된 fbg 개수와 비교 (간혹 fbg 개수가 틀리게 들어오는 경우가 있음)
                    for i in range(0, self.peaks[2]):
                        value = self.int_from_bytes(self.frame[32 + (4 * (self.peaks[0] + self.peaks[1])) + (4 * i):32 + (4 * (self.peaks[0] + self.peaks[1])) + (4 * (i + 1))]) / 10000
                        # print(value)
                        self.fbg_values['Ch3'].append(value)

                else:
                    fbgLogger.warning('Different peaks3 count: Defined:{0}, Received:{1}'.format(self.CONST_MUX_STATE[self.mux_state][2], self.peaks[2]))
                    key = '3.{0}'.format(self.get_mux_state())
                    wavelength = self.WAVELENGTH_RANGE[key]
                    # print(wavelength)

                    # 먼저 temp_values에 값을 저정한다.
                    temp_values = []
                    for i in range(0, self.peaks[2]):
                        value = self.int_from_bytes(self.frame[32 + (4 * (self.peaks[0] + self.peaks[1])) + (4 * i):32 + (4 * (self.peaks[0] + self.peaks[1])) + (4 * (i + 1))]) / 10000
                        # print(value)
                        temp_values.append(value)

                    # print('temp_values: {0}'.format(temp_values))
                    # print('wavelength: {0}'.format(wavelength))

                    for i in range(0, len(wavelength)):
                        self.fbg_values['Ch3'].append(None)
                        for value in temp_values:
                            if wavelength[i][0] < value < wavelength[i][1]:
                                self.fbg_values['Ch3'][i] = value
                                break

                    # print('Ch3_data: {0}'.format(self.fbg_values['Ch3']))

            if self.peaks[3] > 0:   # Peaks4 = NS4 = self.peaks[3]
                # print('{0}:{1}'.format(self.peaks[3], self.CONST_MUX_STATE[self.mux_state][3]))
                if self.peaks[3] == self.CONST_MUX_STATE[self.mux_state][3]:  # 프레임에서 받은 fbg 개수와 실제 설정된 fbg 개수와 비교 (간혹 fbg 개수가 틀리게 들어오는 경우가 있음)
                    for i in range(0, self.peaks[3]):
                        value = self.int_from_bytes(self.frame[32 + (4 * (self.peaks[0] + self.peaks[1] + self.peaks[2])) + (4 * i):32 + (4 * (self.peaks[0] + self.peaks[1] + self.peaks[2])) + (4 * (i + 1))]) / 10000
                        self.fbg_values['Ch4'].append(value)

                else:
                    fbgLogger.warning('Different peaks4 count: Defined:{0}, Received:{1}'.format(self.CONST_MUX_STATE[self.mux_state][3], self.peaks[3]))
                    key = '4.{0}'.format(self.get_mux_state())
                    wavelength = self.WAVELENGTH_RANGE[key]
                    # print(wavelength)

                    # 먼저 temp_values에 값을 저정한다.
                    temp_values = []
                    for i in range(0, self.peaks[3]):
                        value = self.int_from_bytes(self.frame[32 + (4 * (self.peaks[0] + self.peaks[1] + self.peaks[2])) + (4 * i):32 + (4 * (self.peaks[0] + self.peaks[1] + self.peaks[2])) + (4 * (i + 1))]) / 10000
                        temp_values.append(value)

                    # print('temp_values: {0}'.format(temp_values))
                    # print('wavelength: {0}'.format(wavelength))

                    for i in range(0, len(wavelength)):
                        self.fbg_values['Ch4'].append(None)
                        for value in temp_values:
                            if wavelength[i][0] < value < wavelength[i][1]:
                                self.fbg_values['Ch4'][i] = value
                                break

                    # print('Ch4_data: {0}'.format(self.fbg_values['Ch4']))

        except Exception as e:
            fbgLogger.error('Exception Error: {0}'.format(str(e)))
            return -1

    def get_mux_state(self):
        return self.mux_state + 1

    def get_peaks(self, index):
        return self.peaks[index]

    def get_fbg_values(self, ch=''):
        if ch == '':
            return self.fbg_values
        return self.fbg_values[ch]

    def __str__(self):
        str = '[FrameData] Mux: {0}  Ch1.{1}: {2}  Ch2.{1}: {3}  Ch3.{1}: {4}  Ch4.{1}: {5}  Serial: {6}'.format(
            self.mux_state,
            self.get_mux_state(),
            self.fbg_values['Ch1'],
            self.fbg_values['Ch2'],
            self.fbg_values['Ch3'],
            self.fbg_values['Ch4'],
            self.serial_number,
        )
        return str


class Peak:

    timestamp = None
    FULL_DATA = {
        'Ch1.1': [None, None, None, None, None, None, None],
        'Ch1.2': [None, None, None, None, None, None, None],
        'Ch1.3': [None, None, None, None, None, None, None],
        'Ch1.4': [None, None, None, None],
        'Ch2.1': [None, None, None, None, None, None, None, None, None],
        'Ch2.2': [None, None, None, None, None, None, None, None],
        'Ch2.3': [None, None, None, None, None, None, None, None, None, None, None],
        'Ch2.4': [None, None],
        'Ch3.1': [None, None, None, None, None, None, None],
        'Ch3.2': [None, None, None, None, None, None, None],
        'Ch3.3': [None, None, None, None, None, None, None],
        'Ch3.4': [None, None, None, None, None],
        'Ch4.1': [None, None, None, None, None, None, None, None, None, None],
        'Ch4.2': [None, None, None, None, None, None, None],
        'Ch4.3': [None, None],
        'Ch4.4': [None, None],
    }

    def __index__(self):
        # self.data_frame = new_frame
        pass

    def merge_data_frame(self, frame):
        mux = frame.get_mux_state()
        self.FULL_DATA['Ch1.{0}'.format(mux)] = frame.get_fbg_values('Ch1')
        self.FULL_DATA['Ch2.{0}'.format(mux)] = frame.get_fbg_values('Ch2')
        self.FULL_DATA['Ch3.{0}'.format(mux)] = frame.get_fbg_values('Ch3')
        self.FULL_DATA['Ch4.{0}'.format(mux)] = frame.get_fbg_values('Ch4')


    def __str__(self):
        # return '{0}: {1}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), self.FULL_DATA)
        return '[Buffer] {0}'.format(self.FULL_DATA)


class InvalidResponseException(BaseException): pass


# HOST = '127.0.0.1'
# PORT = 65432

# HOST = '172.30.15.200'
# HOST = '10.252.154.223'
HOST = '10.0.0.123'
PORT = 50000

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    peak_data = Peak()
    while True:
        s.send('#GET_PEAKS_AND_LEVELS'.encode())
        fbgLogger.info('Send command: {0}'.format('#GET_PEAKS_AND_LEVELS'.encode()))

        buffer = s.recv(1024)

        # fbgLogger.info('Received: {0}'.format(buffer.decode('ascii')))
        # print('Received BINARY: {0}'.format(buffer))
        # print('Received BINARY length: {0}'.format(len(buffer)))
        fbgLogger.info('Received BINARY: {0}'.format(buffer))
        fbgLogger.info('Received BINARY length:'.format(len(buffer)))

        data_frame = DataFrame(buffer)
        if data_frame.set_fbg_values() == -1:
            fbgLogger.error(
                'Error!! in set_fbg_values() functions: peaks1:[{0}], peaks2:[{1}], peaks3:[{2}], peaks4:[{3}]'.format(
                data_frame.get_peaks(0), data_frame.get_peaks(1), data_frame.get_peaks(2), data_frame.get_peaks(3))
            )
            continue

        fbgLogger.info('{0}'.format(data_frame))
        peak_data.merge_data_frame(data_frame)
        fbgLogger.info('{0}'.format(peak_data))
        print('{0}'.format(data_frame))
        print('{0}'.format(peak_data))
        time.sleep(0.3)
        # break


