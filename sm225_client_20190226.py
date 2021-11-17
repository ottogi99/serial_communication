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



class channel:
    serial = 0
    timestamp = 0

    RANGE = [
        # 채널: FBG(MIN, MAX)
        # 1.1 : 7 FBGs, 1.2 : 7 FBGs, 1.3 : 7 FBGs, 1.4 : 4 FBGs
        # 2.1 : 9 FBGs, 2.2 : 8 FBGs, 2.3 : 11 FBGs, 2.4 : 2 FBGs
        # 3.1 : 7 FBGs, 3.2 : 7 FBGs, 3.3 : 7 FBGs, 3.4 : 4 FBGs
        # 4.1 : 10 FBGs, 4.2 : 7 FBGs, 4.3 : 2 FBGs, 4.4 : 2 FBGs
        {'1.1': [(1513.060, 1523.060), (1531.221, 1539.216), (1539.217, 1543.790), (1543.082, 1548.990), (1549.824, 1555.732), (1555.733, 1523.395), (1564.518, 1569.180)]},
        {'1.2': [(1516.137, 1526.137), (1532.418, 1539.360), (1538.283, 1545.225), (1545.226, 1551.211), (1551.212, 1556.098), (1556.099, 1562.241), (1562.242, 1571.438)]},
        {'1.3': [(1522.026, 1530.218), (1532.902, 1538.963), (1538.912, 1546.355), (1543.725, 1550.292), (1549.473, 1556.040), (1555.640, 1562.207), (1561.695, 1568.262)]},
        {'1.4': [(1525.827, 1535.142), (1535.103, 1542.388), (1542.389, 1550.359), (1542.439, 1550.409)]},
        {'2.1': [(1509.003,	1519.003), (1533.359, 1539.358), (1539.604, 1547.576), (1545.216, 1551.896), (1550.972, 1557.652), (1555.633, 1562.465), (1562.466, 1569.541), (1569.542, 1575.541), (1575.542, 1583.514)]},
        {'2.2': [(1507.150, 1514.104), (1514.105, 1521.059), (1526.922, 1535.514), (1535.515, 1540.611), (1540.612,	1546.638), (1546.639, 1552.745), (1555.603, 1561.629), (1558.471, 1564.577)]},
        {'2.3': [(1509.076, 1519.076), (1525.377, 1533.260), (1533.261, 1537.747), (1537.748, 1540.777), (1540.778, 1545.071), (1545.072, 1551.056), (1551.057, 1555.591), (1555.592, 1561.506), (1561.507, 1568.981), (1568.982, 1573.500), (1573.501, 1580.044)]},
        {'2.4': [(1537.307, 1547.307), (1552.209, 1562.209)]},
        {'3.1': [(1522.006, 1530.112), (1530.113, 1537.003), (1537.004, 1544.596), (1544.597, 1551.264), (1551.265, 1557.107), (1557.108, 1561.796), (1562.898, 1567.586)]},
        {'3.2': [(1513.076, 1523.076), (1531.236, 1537.651), (1537.223, 1543.638), (1543.578, 1549.666), (1549.691, 1555.779), (1555.749, 1561.764), (1561.765, 1568.280)]},
        {'3.3': [(1516.084, 1526.084), (1531.600, 1537.945), (1537.190, 1543.535), (1543.937, 1551.486), (1551.487, 1557.258), (1557.259, 1563.133), (1563.134, 1571.140)]},
        {'3.4': [(1524.786, 1534.345), (1534.346, 1542.003), (1542.004, 1550.102), (1552.459, 1562.459)]},
        {'4.1': [(1508.976,	1515.007), (1515.008, 1521.040), (1530.930, 1537.535), (1537.055, 1543.660), (1543.579,	1549.889), (1552.367, 1558.483), (1556.007, 1563.263), (1563.264, 1569.283), (1569.284, 1575.240), (1575.241, 1583.194)]},
        {'4.2': [(1507.107, 1517.107), (1527.713, 1534.621), (1533.557, 1540.465), (1540.571, 1547.627), (1547.628, 1552.882), (1552.896, 1559.952), (1559.497, 1564.751)]},
        {'4.3': [(1536.993, 1546.993), (1552.331, 1562.331)]},
        {'4.4': [(1537.040, 1547.040), (1552.765, 1562.765)]},
    ]

    ch1_data = []
    ch2_data = []
    ch3_data = []


class Data:
    MUX_STATE = [
        {0: [7, 9, 7, 10]},
        {1: [7, 8, 7, 7]},
        {2: [7, 11, 7, 2]},
        {3: [4, 2, 4, 2]},
    ]

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
        self.fbg_values = []
        self.fbg_levels = []

    def set_fbg_value(self, frame):
        try :
            self.timestamp = self.int_from_bytes(self.frame[0:4]) + self.int_from_bytes(self.frame[4:8]) / 1000000
            self.datetime = datetime.datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            self. serial_number = frame[8:12]
            self.peaks = [
                self.int_from_bytes(frame[12:14]),
                self.int_from_bytes(frame[14:16]),
                self.int_from_bytes(frame[16:18]),
                self.int_from_bytes(frame[18:20]),
            ]
            self.thermal_stability_flag = frame[20:22]
            self.mux_state = frame[22:24]
            self.reserved = frame[24:32]

            self.fbg_values = [
                {'Ch1': []},
                {'Ch2': []},
                {'Ch3': []},
                {'Ch4': []},
            ]

            if self.peaks[0] > 0:   # Peaks1 = NS1 = self.peaks[0]
                for self.i in range(0, self.peaks[0]):
                    self.fbg_values['Ch1'].append(frame[32 + (4 * i):32 + (4 * (i + 1))])

            if self.peaks[1] > 0:   # Peaks2 = NS2 = self.peaks[1]
                for i in range(0, self.peaks[0]):
                    self.fbg_values['Ch2'].append(frame[32 + (4 * self.peaks[0]) + (4 * i):32 + (4 * self.peaks[0]) + 4 * (i + 1)])

            if self.peaks[2] > 0:   # Peaks3 = NS3 = self.peaks[2]
                for i in range(0, self.peaks[0]):
                    self.fbg_values['Ch3'].append(frame[32 + (4 * (self.peaks[0] + self.peaks[1])) + (4 * i):32 + (4 * (self.peaks[0] + self.peaks[1])) + (4 * (i + 1))])

            if self.peaks[3] > 0:   # Peaks4 = NS4 = self.peaks[3]
                for i in range(0, self.peaks[0]):
                    self.fbg_values['Ch4'].append(frame[32 + (4 * (self.peaks[0] + self.peaks[0] + self.peaks[2])) + (4 * i):32 + (4 * (self.peaks[0] + self.peaks[1] + self.peaks[2])) + (4 * (i + 1))])
        except BaseException:
            pass

    def get_state(self):
        return self.state

    def get_peaks(self, index):
        return self.peaks[index]

    def get_fbg_values(self, ch):
        return self.fbg_values[ch]











class InvalidResponseException(BaseException): pass


HOST = '127.0.0.1'
PORT = 65432
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    # s.connect(('192.168.10.178', 7002))
    while True:
        s.send('#GET_PEAKS_AND_LEVELS'.encode())
        data = s.recv(1024)
        print(data)

        try:
            timestamp_sec = data[0:4]
            timestamp_msec = data[4:8]

            sec = int.from_bytes(timestamp_sec, byteorder='little')
            msec = int.from_bytes(timestamp_msec, byteorder='little')
            timestamp = sec + msec / 1000000
            print('timestamp: {0}'.format(timestamp))
            st = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            print('st: {0}'.format(st))

            serial_number = data[8:12]
            peaks1 = data[12:14]
            ns1 = int.from_bytes(peaks1, byteorder='little')
            peaks2 = data[14:16]
            ns2 = int.from_bytes(peaks2, byteorder='little')
            peaks3 = data[16:18]
            ns3 = int.from_bytes(peaks3, byteorder='little')
            peaks4 = data[18:20]
            ns4 = int.from_bytes(peaks4, byteorder='little')
            thermal_stability_flag = data[20:22]
            mux_state = data[22:24]
            reserved = data[24:32]
            # ch1_data = data[32:32+4*ns1]

            print('ns1: {0}'.format(int.from_bytes(peaks1, byteorder='little'))) # 리틀 엔디안
            print('ns2: {0}'.format(int.from_bytes(peaks2, byteorder='little')))
            print('ns3: {0}'.format(int.from_bytes(peaks3, byteorder='little')))
            print('ns4: {0}'.format(int.from_bytes(peaks4, byteorder='little')))


            if ns1 < 0 or ns2 < 0 or ns3 < 0 or ns4 < 0:
                raise InvalidResponseException

            ch1_sensor_data = []
            ch2_sensor_data = []
            ch3_sensor_data = []
            ch4_sensor_data = []
            ch1_levels_data = []
            ch2_levels_data = []
            ch3_levels_data = []
            ch4_levels_data = []

            if ns1 > 0:
                for i in range(0, ns1):
                    ch1_sensor_data.append(data[32 + (4 * i):32 + (4 * (i + 1))])

            if ns2 > 0:
                for i in range(0, ns2):
                    ch2_sensor_data.append(data[32 + (4 * ns1) + (4 * i):32 + (4 * ns1) + 4 * (i + 1)])

            if ns3 > 0:
                for i in range(0, ns3):
                    ch3_sensor_data.append(data[32 + (4 * (ns1+ns2)) + (4 * i):32 + (4 * (ns1+ns2)) + (4 * (i + 1))])

            if ns4 > 0:
                for i in range(0, ns4):
                    ch4_sensor_data.append(data[32 + (4 * (ns1+ns2+ns3)) + (4 * i):32 + (4 * (ns1+ns2+ns3)) + (4 * (i + 1))])

            if ns1 > 0:
                for i in range(0, ns1):
                    ch1_levels_data.append(data[32 + (4 * (ns1+ns2+ns3+ns4)) + (2 * i):32 + (4 * (ns1+ns2+ns3+ns4)) + (2 * (i+1))])

            if ns2 > 0:
                for i in range(0, ns2):
                    ch2_levels_data.append(data[32 + (4 * (ns1+ns2+ns3+ns4)) + (2 * ns1) + (2 * i):32 + (4 * (ns1+ns2+ns3+ns4)) + (2 * ns1) + (2 * (i+1))])

            if ns3 > 0:
                for i in range(0, ns3):
                    ch3_levels_data.append(data[32 + (4 * (ns1+ns2+ns3+ns4)) + (2*(ns1+ns2)) + (2 * i):32 + (4 * (ns1+ns2+ns3+ns4)) + (2*(ns1+ns2)) + (2 * (i+1))])

            if ns4 > 0:
                for i in range(0, ns4):
                    ch4_levels_data.append(data[32 + (4 * (ns1+ns2+ns3+ns4)) + (2*(ns1+ns2+ns3)) + (2 * i):32 + (4 * (ns1+ns2+ns3+ns4)) + (2*(ns1+ns2+ns3)) + (2 * (i+1))])

            # ch1_levels = data[32+4*(peaks1+peaks2+peaks3+peaks4)+2*(peaks1):peaks1]
            # ch2_levels = data[32+4*(peaks1+peaks2+peaks3+peaks4)+2*(peaks1+peaks2):peaks2]
            # ch3_levels = data[32+4*(peaks1+peaks2+peaks3+peaks4)+2*(peaks1+peaks2+peaks3):peaks3]
            # ch4_levels = data[32+4*(peaks1+peaks2+peaks3+peaks4)+2*(peaks1+peaks2+peaks3+peaks4):peaks4]

                # sensor1_data = data[32:32 + 4 * 1]
                # sensor2_data = data[32 + 4 * 1:32 + 4 * 2]
                # sensor3_data = data[32 + 4 * 2:32 + 4 * 3]
                # sensor4_data = data[32 + 4 * 3:32 + 4 * 4]
                # sensor5_data = data[32 + 4 * 4:32 + 4 * 5]
                # sensor6_data = data[32 + 4 * 5:32 + 4 * 6]
                # sensor7_data = data[32 + 4 * 6:32 + 4 * 7]

            # ch2_data = data[32+4*(peaks1):4*peaks2]
            # ch3_data = data[32+4*(peaks1+peaks2):4*peaks3]
            # ch4_data = data[32+4*(peaks1+peaks2+peaks3):4*peaks4]

            # ch1_levels = data[32+4*(peaks1+peaks2+peaks3+peaks4)+2*(peaks1):peaks1]
            # ch2_levels = data[32+4*(peaks1+peaks2+peaks3+peaks4)+2*(peaks1+peaks2):peaks2]
            # ch3_levels = data[32+4*(peaks1+peaks2+peaks3+peaks4)+2*(peaks1+peaks2+peaks3):peaks3]
            # ch4_levels = data[32+4*(peaks1+peaks2+peaks3+peaks4)+2*(peaks1+peaks2+peaks3+peaks4):peaks4]

            # print(timestamp_sec)
            print('timestamp_sec: {0}'.format(int.from_bytes(timestamp_sec, byteorder='little')))
            # print(timestamp_msec)
            print('timestamp_msec: {0}'.format(int.from_bytes(timestamp_msec, byteorder='little')))
            # print(serial_number)
            print('serial_number: {0}'.format(int.from_bytes(serial_number, byteorder='little')))
            # print(peaks1)
            print('peaks1: {0}'.format(int.from_bytes(peaks1, byteorder='little'))) # 리틀 엔디안
            print('peaks2: {0}'.format(int.from_bytes(peaks2, byteorder='little')))
            print('peaks3: {0}'.format(int.from_bytes(peaks3, byteorder='little')))
            print('peaks4: {0}'.format(int.from_bytes(peaks4, byteorder='little')))
            print('thermal_stability_flag: {0}'.format(int.from_bytes(thermal_stability_flag, byteorder='little')))
            print('mux_state: {0}'.format(int.from_bytes(mux_state, byteorder='little')))
            print('reserved: {0}'.format(int.from_bytes(reserved, byteorder='little')))
            # print('ch1_data: {0}'.format(int.from_bytes(ch1_data, byteorder='big')))

            print('ch1_sensor_data len: {0}'.format(len(ch1_sensor_data)))
            print('ch2_sensor_data len: {0}'.format(len(ch2_sensor_data)))
            print('ch3_sensor_data len: {0}'.format(len(ch3_sensor_data)))
            print('ch4_sensor_data len: {0}'.format(len(ch4_sensor_data)))

            for sensor_data in ch1_sensor_data:
                print('ch1.{0}_sensor_data: {1:.3f}'.format(int.from_bytes(mux_state, byteorder='little'), int.from_bytes(sensor_data, byteorder='little') / 10000))

            for sensor_data in ch2_sensor_data:
                print('ch2.{0}_sensor_data: {1:.3f}'.format(int.from_bytes(mux_state, byteorder='little'), int.from_bytes(sensor_data, byteorder='little') / 10000))

            for sensor_data in ch3_sensor_data:
                print('ch3.{0}_sensor_data: {1:.3f}'.format(int.from_bytes(mux_state, byteorder='little'), int.from_bytes(sensor_data, byteorder='little') / 10000))

            for sensor_data in ch4_sensor_data:
                print('ch4.{0}_sensor_data: {1:.3f}'.format(int.from_bytes(mux_state, byteorder='little'), int.from_bytes(sensor_data, byteorder='little') / 10000))


            for levels in ch1_levels_data:
                print('ch1.{0}_levels_data: {1:.3f}'.format(int.from_bytes(mux_state, byteorder='little'), int.from_bytes(levels, byteorder='little', signed=True) / 100))

            for levels in ch2_levels_data:
                print('ch2.{0}_levels_data: {1:.3f}'.format(int.from_bytes(mux_state, byteorder='little'), int.from_bytes(levels, byteorder='little', signed=True) / 100))

            for levels in ch3_levels_data:
                print('ch3.{0}_levels_data: {1:.3f}'.format(int.from_bytes(mux_state, byteorder='little'), int.from_bytes(levels, byteorder='little', signed=True) / 100))

            for levels in ch4_levels_data:
                print('ch4.{0}_levels_data: {1:.3f}'.format(int.from_bytes(mux_state, byteorder='little'), int.from_bytes(levels, byteorder='little', signed=True) / 100))

    # except BaseException:
    #     pass
            time.sleep(1)

        except InvalidResponseException:
            pass

