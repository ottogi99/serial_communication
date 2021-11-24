'''
--------------------------------------------------------------------------------------------------
Entry                       Byte Offset                             Atomic(bytes)   Total(bytes)
--------------------------------------------------------------------------------------------------
Timestamp (sec.)            0                                       4 (U32)         4
Timestamp (msec.)           4                                       4 (U32)         4
Serial Number               8                                       4 (U32)         4
#Peaks 1 (NS1)              12                                      2 (U16)         2
(Number of Sensor)
#Peaks 2 (NS2)              14                                      2 (U16)         2
#Peaks 3 (NS3)              16                                      2 (U16)         2
#Peaks 4 (NS4)              18                                      2 (U16)         2
Thermal Stability Falg      20                                      2 (U16)         2
MUX State                   22                                      -
Reserved                    24                                      1               8
Ch1 data                    32                                      4 (S32)         4*NS1
Ch2 data                    32+4*(NS1)                              4 (S32)         4*NS2
Ch3 data                    32+4*(NS1+NS2)                          4 (S32)         4*NS3
Ch4 data                    32+4*(NS1+NS2+NS3)                      4 (S32)         4*NS4
Ch1 levels                  32+4*(NS1+NS2+NS3+NS4)                  2 (S16)         NS1
Ch2 levels                  32+4*(NS1+NS2+NS3+NS4)+2*(NS1)          2 (S16)         NS2
Ch3 levels                  32+4*(NS1+NS2+NS3+NS4)+2*(NS1+NS2)      2 (S16)         NS3
Ch4 levels                  32+4*(NS1+NS2+NS3+NS4)+2*(NS1+NS2+NS3)  2 (S16)         NS4
--------------------------------------------------------------------------------------------------
'''

import datetime


class PeaksLevels:
    def __init__(self, frame=None):
        self.seconds = None
        self.microseconds = None
        self.serial_number = None
        self.number_of_sensor = {
            'ch1': 0,
            'ch2': 0,
            'ch3': 0,
            'ch4': 0,
        }
        self.thermal_stability_flag = None
        self.mux_state = None           # 0,8 for 8 channels, 0,1,2,3 for 16 channel
        self.data = {                   # Ch1 ~ Ch4 data
            'ch1': [],
            'ch2': [],
            'ch3': [],
            'ch4': [],
        }
        self.levels = {                 # Ch1 ~ Ch4 levels
            'ch1': [],
            'ch2': [],
            'ch3': [],
            'ch4': [],
        }

        self.parse(frame)

    def parse(self, frame):
        self.seconds = self.bytesToInt(frame[0:4])
        self.microseconds = self.bytesToInt(frame[4:8])
        self.serial_number = self.bytesToInt(frame[8:12])
        self.number_of_sensor['ch1'] = self.bytesToInt(frame[12:14])
        self.number_of_sensor['ch2'] = self.bytesToInt(frame[14:16])
        self.number_of_sensor['ch3'] = self.bytesToInt(frame[16:18])
        self.number_of_sensor['ch4'] = self.bytesToInt(frame[18:20])
        self.thermal_stability_flag = self.bytesToInt(frame[20:22])
        self.mux_state = self.bytesToInt(frame[22:24])
        # self.reserved = self.bytesToInt(frame[24:32])

        ns_sum = 0
        for key in self.number_of_sensor.keys():
            # print('key:', key)
            byte_offset = 32 + 4 * ns_sum
            ns = self.number_of_sensor[key]
            if ns > 0:
                for i in range(ns):
                    print('i===', i)
                    # data = self.bytesToInt(frame[byte_offset:byte_offset + (4 * ns)]) / 10000
                    # data = self.bytesToInt(frame[byte_offset + (4 * i):byte_offset + (4 * (i + 1))]) / 10000
                    # data = self.bytesToInt(frame[byte_offset + (ns * 4) + (4 * i):byte_offset + (ns * 4) + (4 * (i + 1))]) / 10000
                    data = self.bytesToInt(frame[byte_offset + (i * 4):byte_offset + ((i+1) * 4)]) / 10000
                    print('data:', data)
                    self.data[key].append(data)

            ns_sum += ns

        ns_sum_level = 0
        for key in self.number_of_sensor.keys():
            byte_offset = 32 + (4 * ns_sum) + 2 * ns_sum_level
            ns = self.number_of_sensor[key]
            if ns > 0:
                for i in range(ns):
                    # levles = self.bytesToInt(frame[byte_offset:byte_offset + (4 * ns)]) / 10000
                    levels = self.bytesToInt(frame[byte_offset + (i * 2):byte_offset + ((i+1) * 2)]) / 100
                    print('level:', levels)
                    self.levels[key].append(levels)

            ns_sum_level = + ns

    def bytesToInt(self, bytes_data):
        # print('bytes_data:', bytes_data)
        return int.from_bytes(bytes_data, byteorder='little', signed=True)

    def getTimestamp(self):
        return self.seconds + self.microseconds / 1000000

    def getDateTime(self):
        return datetime.datetime.fromtimestamp(self.getTimestamp()).strftime('%Y-%m-%d %H:%M:%S')

    def getSerialNumber(self):
        return self.serial_number

    def getNumberOfSensors(self):
        return self.number_of_sensor

    def getNumberOfSensor(self, ch):
        if ch in self.number_of_sensor.keys():
            return self.number_of_sensor[ch]

    def getThermalStabilityFlag(self) -> int:
        return self.thermal_stability_flag

    def getMuxState(self):
        return self.mux_state

    def getData(self):
        return self.data

    def getDataCh(self, ch):
        if ch in self.data.keys():
            return self.data[ch]

    def getLevel(self):
        return self.levels

    def getLevelCh(self, ch):
        if ch in self.levels.keys():
            return self.levels[ch]

    def __str__(self):
        return "- Datetime: {0}" \
                "- Serial Number: {1}" \
                "- Peaks1 (NS1): {2}" \
                "- Peaks2 (NS2): {3}" \
                "- Peaks3 (NS3): {4}" \
                "- Peaks4 (NS4): {5}" \
                "- Thermal Stability Flag: {6}" \
                "- MUX State: {7}" \
                "- Ch1 Data: {8}" \
                "- Ch2 Data: {9}" \
                "- Ch3 Data: {10}" \
                "- Ch4 Data: {11}" \
                "- Ch1 Levels: {12}" \
                "- Ch2 Levels: {13}" \
                "- Ch3 Levels: {14}" \
                "- Ch4 Levels: {15}".format(
                    self.getDateTime(),
                    self.getSerialNumber(),
                    self.getNumberOfSensor('ch1'),
                    self.getNumberOfSensor('ch2'),
                    self.getNumberOfSensor('ch3'),
                    self.getNumberOfSensor('ch4'),
                    self.getThermalStabilityFlag(),
                    self.getMuxState(),
                    self.getDataCh('ch1'),
                    self.getDataCh('ch2'),
                    self.getDataCh('ch3'),
                    self.getDataCh('ch4'),
                    self.getLevelCh('ch1'),
                    self.getLevelCh('ch2'),
                    self.getLevelCh('ch3'),
                    self.getLevelCh('ch4')
                )
