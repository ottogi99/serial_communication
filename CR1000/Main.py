'''
보현산댐 데이터로거 수동 복구 프로그램
2022.01.11.

Pandas 패키지 사용
'''
'''
DB 입력쿼리 
- PL01-X (상하)
    INSERT INTO DULLNDGGDT (OBSDT,DAMCD,SENID,MEAVAL1,CALVAL1,CALVAL2,CALVAL3,CALVAL4) VALUES( '20220101130000','2012101','PL0101',{5},{6},{7},{8},{9}) -> 측정값과 계산삾
- PL01-Y (좌우안)
    INSERT INTO DULLNDGGDT (OBSDT,DAMCD,SENID,MEAVAL1,CALVAL1,CALVAL2,CALVAL3,CALVAL4) VALUES( '{0}{1}{2}{3}{4}00','2012101','PL0102',{5},{6},{7},{8},{9})
    BODPU_DLG_YRI_001;
    BODPU_DLG_MTI_001;
    BODPU_DLG_DAI_001;
    BODPU_DLG_HRI_001;
    BODPU_DLG_MII_001;
    
    BODPU_PLD_PLM_102;
    BODPU_PLD_XVC_102;
    BODPU_PLD_TEB_102;
    BODPU_PLD_TEC_102;
    BODPU_PLD_PLC_102
- PL01-Z (침하)
    INSERT INTO DULLNDGGDT (OBSDT,DAMCD,SENID,MEAVAL1,CALVAL1,CALVAL2,CALVAL3,CALVAL4) VALUES( '{0}{1}{2}{3}{4}00','2012101','PL0103',{5},{6},{7},{8},{9})
    ;BODPU_DLG_YRI_001
    ;BODPU_DLG_MTI_001
    ;BODPU_DLG_DAI_001
    ;BODPU_DLG_HRI_001
    ;BODPU_DLG_MII_001
    
    ;BODPU_PLD_PLM_103  - 측정값
    ;BODPU_PLD_XVC_103  - 기본계산식 (C01)
    ;BODPU_PLD_TEB_103  - 온도보정 (C02)
    ;BODPU_PLD_TEC_103  - 온도보정결과 (C03)
    ;BODPU_PLD_PLC_103  - 단위보정결과 (C04)
    
- WL1
    INSERT INTO DULLNDGGDT (OBSDT,DAMCD,SENID,MEAVAL1,MEAVAL2,CALVAL1,CALVAL2,CALVAL3,CALVAL4,CALVAL5) VALUES( '{0}{1}{2}{3}{4}00','2012101','OP0001',{5},{6},{7},{8},{9},{10},{11})
    ;BODPU_DLG_YRI_002
    ;BODPU_DLG_MTI_002
    ;BODPU_DLG_DAI_002
    ;BODPU_DLG_HRI_002
    ;BODPU_DLG_MII_002
    
    ;BODPU_OPM_OPM_001  - 측정값 (M01)
    ;BODPU_OPM_TEM_001  - 측정 온도값 (M02)
    ;BODPU_OPM_XVB_001  - 기본계산식 (C01)
    ;BODPU_OPM_TEB_001  - 온도보정 (C02)
    ;BODPU_OPM_TEC_001  - 온도보정결과 (C03)
    ;BODPU_OPM_XVC_001  - 단위보정결과 (C04)
    ;BODPU_OPM_OPC_001  - EL.m (C05)

'''
import os
import configparser
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

import math
from types import MethodType


def calculate_method(self):
    if self['TYPE'] == 'PL':
        pass

    elif self['TYPE'] == 'WL':
        # 기본계산식
        self['C01'] = (self['M01'] - self['초기RAW']) * self['GF1'] + \
                             (math.pow((self['M01'] - self['초기RAW']), 2)) * self['GF2'] + \
                             self['OFFSET']
        # 온도보정
        self['C02'] = 0
        # 온도보정결과
        self['C03'] = self['C01'] + self['C02']
        # 단위보정결과
        self['C04'] = (self['C03'] * self['단위보정'] - self['초기데이터']) * 10
        # EL.m
        self['C05'] = self['EL'] - self['C04']

    else:
        pass


class WLSensor:
    def __init__(self):
        return


class Formula:
    def __init__(self, sensor):
        self.sensor = sensor

    def calculate(self):
        if self.sensor['TYPE'] == 'PL':
            pass

        elif self.sensor['TYPE'] == 'WL':
            # 기본계산식
            self.sensor['C01'] = (self.sensor['M01'] - self.sensor['초기RAW']) * self.sensor['GF1'] + \
                                 (math.pow((self.sensor['M01'] - self.sensor['초기RAW']), 2)) * self.sensor['GF2'] + \
                                 self.sensor['OFFSET']
            # 온도보정
            self.sensor['C02'] = 0
            # 온도보정결과
            self.sensor['C03'] = self.sensor['C01'] + self.sensor['C02']
            # 단위보정결과
            self.sensor['C04'] = (self.sensor['C03'] * self.sensor['단위보정'] - self.sensor['초기데이터']) * 10
            # EL.m
            self.sensor['C05'] = self.sensor['EL'] - self.sensor['C04']

        else:
            pass

    #
    # class OdbcDao:
    #     def __init__(self, dsn, uid, pwd):
    #         self.dsn = dsn
    #         self.uid = uid
    #         self.pwd = pwd
    #
    #     # def make_query(self, damcd, name, obsdt, value):
    #     #     code = '수온/전기전도도/탁도/누수량/월류수심 (TB0001/TB0002/TB0003/LWC001/LWC002)'
    #     #     # query = "INSERT INTO DULFDDT (DAMCD, DAMORGNO, DAMLYGGB, MEASURDT, LGMSVALU, CONNCTTP, CALCULATE) " \
    #     #     #         "VALUES ('{0}', '1001', '{1}', TO_DATE('{2}', 'yyyymmddhh24mi'), {3}, '1', 'N')"\
    #     #     #         .format(damcd, name, obsdt, value)
    #     #     query = "INSERT INTO DULPCLTWTDT (OBSDT, DAMCD, SENID, DIVNO, MEACYCLE, MEAVAL) " \
    #     #             "VALUES (TO_DATE('{0}', 'yyyymmddhh24mi'), '{0}', {1}, '1001', '{1}', , {3}, '1', 'N')"\
    #     #             .format(obsdt, damcd, senid, divno, 5, value)
    #     #     return query
    #
    #     def make_query(self, obsdt, damcd, senid, divno, value):
    #         query = "INSERT INTO DULPCLTWTDT (OBSDT, DAMCD, SENID, DIVNO, MEACYCLE, MEAVAL) " \
    #                 "VALUES (TO_DATE('{0}', 'yyyymmddhh24mi'), '{1}', '{2}', {3}, {4}, {5})"\
    #                 .format(obsdt, damcd, senid, divno, 5, value)
    #         return query
    #
    def execute_query(self, query):

        # direct ODBC using IP PORT 티베로 에서는 Tibero ODBC Driver로 하라고 하였으나, 접속이 안되어 odbca ad32 에서 표시되는 이름을 사용하니 접속이 됩니다.
        provider1 = 'DRIVER={Tibero 5 ODBC Driver};SERVER=127.0.0.1;PORT=8639;UID=layme;PWD=thdwlwls_2020;DATABASE=DTB6'
        provider2 = 'DRIVER={Tibero 5 ODBC Driver};SERVER=127.0.0.1;PORT=8629;UID=layme;PWD=thdwlwls_2020;DATABASE=DTB5'

        # using ODBC Configuration DSN 구성시
        # provider1 = 'DSN=T_LOCAL_64;UID=layme;PWD=thdwlwls_2020'
        # provider2 = 'DSN=T_HQ_64;UID=layme;PWD=thdwlwls_2020'

        cnxn = pyodbc.connect(provider1)
        cnxn = pyodbc.connect(provider2)

        try:
            cnxn = pyodbc.connect('DSN={0};UID={1};PWD={2}'.format(self.dsn, self.uid, self.pwd))
            cursor = cnxn.cursor()
            cursor.execute(query)
            cnxn.commit()
            cnxn.close()
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print('[pyodbc Exception] sqlstate:{0}'.format(sqlstate))


#
#
# fn = 'failed.txt'
# with open(fn, 'w') as f:
#     dao_list = []
#     output_list = []
#
#     dao_list.append(OdbcDao('T_LOCAL_64', 'layme', 'thdwlwls_2020'))
#     dao_list.append(OdbcDao('T_HQ_64', 'layme', 'thdwlwls_2020'))
#
#     for dao in dao_list:
#         try:
#             cnxn = pyodbc.connect('DSN={0};UID={1};PWD={2}'.format(dao.dsn, dao.uid, dao.pwd))
#             cursor = cnxn.cursor()
#
#             for line in lines:
#                 line = line.strip('\n')
#                 recv_items = line.split(',')
#
#                 senid, divno = get_senid_n_divno(recv_items[1])
#                 insert_query = dao.make_query(recv_items[0], senid, divno, recv_items[2])
#                 print(insert_query)
#
#                 cursor.execute(cnxn, insert_query)
#                 cnxn.commit()
#
#             cnxn.close()
#         except pyodbc.Error as ex:
#             sqlstate = ex.args[0]
#             print('[pyodbc Exception] sqlstate:{0}'.format(sqlstate))
#             output_list.append(line)
#
#         for item in output_list:
#             f.writelines(item+'\n')


# fn = 'inserted.txt'
# with open(fn, 'a') as f:
#     for item in item_inserted:
#         f.writelines(item+'\n')


# def wl_formula(rawdata):


# print(df.values[0])     # 0행 데이터
# print(df['X_Reading'])  # 칼럼 데이터
# print(df['Y_Reading'])  # 칼럼 데이터
# print(df['Z_Reading'])  # 칼럼 데이터

# print(datetime.strptime(df.loc[i, 'TIMESTAMP'], '%Y-%m-%d %H:%M:%S'))
# print((df.loc[i, 'TIMESTAMP'], df.loc[i, 'RECORD'], df.loc[i, 'AVW_Avg(1,1)'], df.loc[i, 'AVW_Avg(2,1)']))


def load_config(file):
    """
    ini 파일로 부터 환경설정 정보를 읽어들인다.
    :param file: 환경파일(.ini) 경로
    :return: 환경파일 DICTIONARY 객체 (_config_dict)
    """

    _config = configparser.ConfigParser()
    _config.read(file)
    _sections = _config.sections()

    _config_dict = {'DAM': {}, 'WL': {}, 'PL': {}}

    for section in _sections:
        _sections_info = _config[section]

        if section == 'DAM':
            _config_dict['DAM'] = {'CODE': '', 'NAME': ''}
            _config_dict['DAM']['NAME'] = _sections_info['NAME']
            _config_dict['DAM']['CODE'] = _sections_info['CODE']
        elif section == 'DB1':
            _config_dict['DB1'] = {'IP': '', 'PORT': '', 'UID': '', 'PWD': '', 'DATABASE': ''}
            _config_dict['DB1']['IP'] = _sections_info['SERVER']
            _config_dict['DB1']['PORT'] = _sections_info['PORT']
            _config_dict['DB1']['UID'] = _sections_info['UID']
            _config_dict['DB1']['PWD'] = _sections_info['PWD']
            _config_dict['DB1']['DATABASE'] = _sections_info['DATABASE']

        elif section == 'DB2':
            _config_dict['DB2'] = {'IP': '', 'PORT': '', 'UID': '', 'PWD': '', 'DATABASE': ''}
            _config_dict['DB2']['IP'] = _sections_info['SERVER']
            _config_dict['DB2']['PORT'] = _sections_info['PORT']
            _config_dict['DB2']['UID'] = _sections_info['UID']
            _config_dict['DB2']['PWD'] = _sections_info['PWD']
            _config_dict['DB2']['DATABASE'] = _sections_info['DATABASE']

        elif section == 'WL':
            _config_dict['WL'] = {'DATA_FILE': '', 'SENSOR_FILE': ''}
            _config_dict['WL']['DATA_FILE'] = _sections_info['DATA_FILE']
            _config_dict['WL']['SENSOR_FILE'] = _sections_info['SENSOR_FILE']

        elif section == 'PL':
            _config_dict['PL'] = {'DATA_FILE': '', 'SENSOR_FILE': ''}
            _config_dict['PL']['DATA_FILE'] = _sections_info['DATA_FILE']
            _config_dict['PL']['SENSOR_FILE'] = _sections_info['SENSOR_FILE']

        else:
            pass

    return _config_dict


def get_raw_data(path):
    """
    원천 데이터를 읽어들인다. (지하수위계)
    :param path: 원천 데이터 파일 경로
    :return: 원천데이터 데이터프레임 (_df_raw_data)
    """

    _df_raw_data = None
    if os.path.exists(path):
        logging.info('Read raw data file: {0}'.format(path))
        # "TIMESTAMP","RECORD","AVW_Avg(1,1)","AVW_Avg~2","AVW_Avg(2,1)","AVW_Avg~4","batt_volt","p_temp"
        _df_raw_column = pd.read_csv(path, header=None, skiprows=1, nrows=1)
        _raw_columns = _df_raw_column.values[0]
        _df_raw_data = pd.read_csv(path, header=None, names=_raw_columns, skiprows=4)
        _df_raw_data = _df_raw_data.loc[_df_raw_data['RECORD'] > 60460]

    else:
        logging.error('데이터 파일 경로가 올바르지 않습니다. {0}'.format(path))

    return _df_raw_data


def get_sensor_info(path):
    """
    센서 정보를 읽은 후 센서객체를 생성하고 리스트에 추가한다.
    :param path: 센서정보 파일 경로
    :return: 센서 객체 정보 리스트 반환 (_wl_sensors)
    """
    # 센서 정보 가져오기
    _wl_sensors = []

    if os.path.exists(path):
        logging.info('Read info file: {0}'.format(path))
        _df_sensor_info = pd.read_csv(path)
        _df_sensor_info = _df_sensor_info.astype({
            '초기RAW': 'float',
            '초기데이터': 'float',
            'GF1': 'float',
            'GF2': 'float',
            'OFFSET': 'float',
            '단위보정': 'float',
            '온도보정': 'float',
            'EL': 'float',
        })

        for idx in range(len(_df_sensor_info)):
            sensor = _df_sensor_info.loc[idx].copy()  # 깊은 복사
            sensor['TYPE'] = 'WL'
            _wl_sensors.append(sensor)

    else:
        logging.error('데이터 파일 경로가 올바르지 않습니다. {0}'.format(path))

    return _wl_sensors


def insert_data(sensor):
    """
    DB 저장 프로세스
    :param sensor:
    :return:
    """
    # logging.info('DB 저장 프로세스')

    item_inserted.append(sensor)  # DB 입력에 성공한 측정정보를 누적

    pass
    for item in item_inserted:
        logging.info(item)

    for item in item_failed:
        logging.error(item)

    def execute_query(self, query):

        # direct ODBC using IP PORT 티베로 에서는 Tibero ODBC Driver로 하라고 하였으나, 접속이 안되어 odbca ad32 에서 표시되는 이름을 사용하니 접속이 됩니다.
        provider1 = 'DRIVER={Tibero 5 ODBC Driver};SERVER=127.0.0.1;PORT=8639;UID=layme;PWD=thdwlwls_2020;DATABASE=DTB6'
        provider2 = 'DRIVER={Tibero 5 ODBC Driver};SERVER=127.0.0.1;PORT=8629;UID=layme;PWD=thdwlwls_2020;DATABASE=DTB5'

        # using ODBC Configuration DSN 구성시
        # provider1 = 'DSN=T_LOCAL_64;UID=layme;PWD=thdwlwls_2020'
        # provider2 = 'DSN=T_HQ_64;UID=layme;PWD=thdwlwls_2020'

        cnxn = pyodbc.connect(provider1)
        cnxn = pyodbc.connect(provider2)

        try:
            cnxn = pyodbc.connect('DSN={0};UID={1};PWD={2}'.format(self.dsn, self.uid, self.pwd))
            cursor = cnxn.cursor()
            cursor.execute(query)
            cnxn.commit()
            cnxn.close()
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print('[pyodbc Exception] sqlstate:{0}'.format(sqlstate))


def validate_value(in_value):
    try:
        result = float(in_value)
    except ValueError as ex:
        err_message = ex.args[0]
        result = 0
        logging.error('[Validate Exception]:{0}, replaced with {1}'.format(err_message, result))

    return result if not math.isnan(result) else 0


def process_data(df_raw_data, sensors, config):
    """
    원천데이터에서 값을 추출하고 센서별 팩터등을 적용해서 산출값을 계산. 마지막으로, DB에 센서 원천값 및 산출값을 저장
    :param raw_data: 원천데이터
    :param sensors: 센서정보 리스트
    :return:
    """
    item_inserted = []  # DB 입력에 성공한 측정정보
    item_failed = []  # DB 입력에 실패한 측정정보
    result = []

    provider1 = 'DRIVER={{Tibero 5 ODBC Driver}};SERVER={0};PORT={1};UID={2};PWD={3};DATABASE={4}'.format(
        config['DB1']['IP'],
        config['DB1']['PORT'],
        config['DB1']['UID'],
        config['DB1']['PWD'],
        config['DB1']['DATABASE'],
    )

    provider2 = 'DRIVER={{Tibero 5 ODBC Driver}};SERVER={0};PORT={1};UID={2};PWD={3};DATABASE={4}'.format(
        config['DB2']['IP'],
        config['DB2']['PORT'],
        config['DB2']['UID'],
        config['DB2']['PWD'],
        config['DB2']['DATABASE'],
    )

    # cnxn1 = pyodbc.connect(provider1)
    # cnxn2 = pyodbc.connect(provider2)

    # .raw data 행만큼 반복하고 DB 저장, sensor[0]은 WL1, sensor[1]은 WL2 정보
    # logging.debug('1: {}'.format(len(raw_data)))
    # for idx, row in df_raw_data.iterrows():
    # # for i in range(len(raw_data)):
    #     # string을 datetime으로 변경
    #     timestamp = datetime.strptime(raw_data.loc[i, 'TIMESTAMP'], '%Y-%m-%d %H:%M:%S')
    #     sensors[0]['OBSDT'] = sensors[1]['OBSDT'] = timestamp.strftime("%Y%m%d%H%M%S")
    #     sensors[0]['DAMCD'] = sensors[1]['DAMCD'] = config['DAM']['CODE']
    #     sensors[0]['M01'] = validate_value(raw_data.loc[i, 'AVW_Avg(1,1)'])
    #     sensors[0]['M02'] = validate_value(raw_data.loc[i, 'AVW_Avg~2'])
    #     sensors[1]['M01'] = validate_value(raw_data.loc[i, 'AVW_Avg(2,1)'])
    #     sensors[1]['M02'] = validate_value(raw_data.loc[i, 'AVW_Avg~4'])

    for idx, row in df_raw_data.iterrows():
        # string을 datetime으로 변경
        timestamp = datetime.strptime(row['TIMESTAMP'], '%Y-%m-%d %H:%M:%S')
        sensors[0]['OBSDT'] = sensors[1]['OBSDT'] = timestamp.strftime("%Y%m%d%H%M%S")
        sensors[0]['DAMCD'] = sensors[1]['DAMCD'] = config['DAM']['CODE']
        sensors[0]['M01'] = validate_value(row['AVW_Avg(1,1)'])
        sensors[0]['M02'] = validate_value(row['AVW_Avg~2'])
        sensors[1]['M01'] = validate_value(row['AVW_Avg(2,1)'])
        sensors[1]['M02'] = validate_value(row['AVW_Avg~4'])

        # WL 계산식을 각각의 센서객체(WL1, WL2)에 동적으로 추가
        for sensor in sensors:
            sensor.calculate = MethodType(calculate_method, sensor)
            sensor.calculate()

            query = "INSERT INTO DULLNDGGDT(OBSDT, DAMCD, SENID, MEAVAL1, MEAVAL2, CALVAL1, CALVAL2, CALVAL3, " \
                    "CALVAL4, CALVAL5) VALUES('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {})".format(
                sensor['OBSDT'],
                sensor['DAMCD'],
                sensor['SENID'],
                sensor['M01'],
                sensor['M02'],
                sensor['C01'],
                sensor['C02'],
                sensor['C03'],
                sensor['C04'],
                sensor['C05']
            )

            # print(query)
            # exit()
            logging.debug(query)

    #         query = ''
    #         try:
    #             cursor1 = cnxn1.cursor()
    #             cursor1.execute(query)
    #             cnxn1.commit()
    #
    #             cursor2 = cnxn1.cursor()
    #             cursor2.execute(query)
    #             cnxn2.commit()
    #
    #         except pyodbc.Error as ex:
    #             sqlstate = ex.args[0]
    #             print('[pyodbc Exception] sqlstate:{0}'.format(sqlstate))
    #             continue
    #
    # cnxn1.close()
    # cnxn2.close()


if __name__ == '__main__':
    import logging

    logging.basicConfig(
        filename='app.log',
        level=logging.DEBUG,
    )

    config = load_config('./config.ini')
    logging.info('Config file load completed!')

    # 지하수위계 원천데이터를 읽음
    data_filepath = os.path.join('./data', config['WL']['DATA_FILE'])
    df_raw_data = get_raw_data(data_filepath)
    # 지하수위계 센서정보를 읽음
    info_filepath = os.path.abspath(config['WL']['SENSOR_FILE'])
    wl_sensors = get_sensor_info(info_filepath)

    # raw 파일과 info 파일의 데이터프레임의 생성이 정상적이라면 raw 파일을 한 줄씩 읽어서 연산한 다음 DB에 저장한다.
    process_data(df_raw_data, wl_sensors, config)

    # '''
    # 2. 데이터를 읽어들인다.
    # '''
    # filepath = os.path.join('./data', _config['WL']['DATA_FILE'])
    # if os.path.exists(filepath):
    #     logging.INFO('Read data file: {0}'.format(filepath))
    #     df = pd.read_csv(filepath)
    #
    # # 칼럼 추출
    # df = pd.read_csv(filepath, header=None, skiprows=1, nrows=1)
    # columns = df.values[0]
    # # ['TIMESTAMP' 'RECORD' 'X_Reading' 'Y_Reading' 'Z_Reading' 'batt_volt_Min', 'PTemp' 'vw_Pour' 'vw_temp']
    # #   일자, 레코드수, 플럼라인-X(상하), 플럼라인-Y(좌.우안), 플럼라인-Z(침하), 배터리 볼트, 플럼라인온도?, ??, 로거전압???
    # # print(columns)
    #
    # # 데이터 취득
    # df = pd.read_csv(filepath, header=None, names=columns, skiprows=4)
    # # print(df.values[0])     # 0행 데이터
    # # print(df['X_Reading'])  # 칼럼 데이터
    # # print(df['Y_Reading'])  # 칼럼 데이터
    # # print(df['Z_Reading'])  # 칼럼 데이터
    #
    # '''
    # 3. 식을 계산한다.
    # '''
