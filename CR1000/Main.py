import os
import configparser
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import math
from types import MethodType

'''
보현산댐 데이터로거 수동 복구 프로그램
2022.01.11.

Pandas 패키지 사용
'''


def calculate_method(self):
    """
    센서별 산출식을 통해 중간값 및 최종값을 센서객체에 대입한다.
    :param self:
    :param sensor_type: 플럼라인(PL), 지하수위계(WL)
    :return:
    """
    if self['TYPE'] == 'PL':
        # 기본계산식
        self['C01'] = (self['M01'] - self['초기RAW']) * self['GF1'] + self['OFFSET']
        # 온도보정
        self['C02'] = 0
        # 온도보정결과
        self['C03'] = self['C01'] + self['C02']
        # 단위보정결과
        self['C04'] = self['C03'] * self['단위보정'] - self['초기데이터']

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
        logging.error('산출식에 사용될 센서타입이 올바르지 않습니다. {}'.format(self['TYPE']))
        pass


def load_config(path):
    """
    ini 파일로 부터 환경설정 정보를 읽어들인다.
    :param path: 환경파일(.ini) 경로
    :return: 환경파일 DICTIONARY 객체 (_config_dict)
    """
    if not os.path.exists(path):
        logging.error('{} 파일이 존재하지 않습니다.')

    _config = configparser.ConfigParser()
    _config.read(path)
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
    원천 데이터를 읽어들인다. (플럼라인, 지하수위계)
    :param path: 원천 데이터 파일 경로
    :return: 원천데이터 데이터프레임 (_df_raw_data)
    """

    _df_raw_data = None
    if os.path.exists(path):
        logging.info('{} 파일로 부터 원천 데이터를 읽어들입니다.'.format(path))
        # 지하수위 : ["TIMESTAMP","RECORD","AVW_Avg(1,1)","AVW_Avg~2","AVW_Avg(2,1)","AVW_Avg~4","batt_volt","p_temp"]
        # 플럼라인 : ['TIMESTAMP' 'RECORD' 'X_Reading' 'Y_Reading' 'Z_Reading' 'batt_volt_Min', 'PTemp' 'vw_Pour' 'vw_temp']
        _df_raw_column = pd.read_csv(path, header=None, skiprows=1, nrows=1)
        _raw_columns = _df_raw_column.values[0]
        _df_raw_data = pd.read_csv(path, header=None, names=_raw_columns, skiprows=4)

        # 테스트 코드
        _basename = os.path.basename(path)
        if _basename == 'RXTX_RXTX1.dat':
            _df_raw_data = _df_raw_data.loc[_df_raw_data['RECORD'] > 93741]
        elif _basename == 'WL_BH_1.dat':
            _df_raw_data = _df_raw_data.loc[_df_raw_data['RECORD'] > 60460]

    else:
        logging.error('데이터 파일 경로가 올바르지 않습니다. {0}'.format(path))

    return _df_raw_data


def get_sensor_info(path, sensor_type):
    """
    센서 정보를 읽은 후 센서객체를 생성하고 리스트에 추가한다.
    :param sensor_type:
    :param path: 센서정보 파일 경로
    :return: 센서 객체 정보 리스트 반환 (_wl_sensors)
    """
    # 센서 정보 가져오기
    _sensors = []
    if os.path.exists(path):
        logging.info('{} 로부터 센서 정보를 가져옵니다.'.format(path))
        _df_sensor_info = pd.read_csv(path)

        if sensor_type == 'PL':
            _df_sensor_info = _df_sensor_info.astype({
                '초기RAW': 'float',
                '초기데이터': 'float',
                'GF1': 'float',
                'OFFSET': 'float',
                '단위보정': 'float',
            })

            for idx in range(len(_df_sensor_info)):
                sensor = _df_sensor_info.loc[idx].copy()  # 깊은 복사
                sensor['TYPE'] = 'PL'
                _sensors.append(sensor)

        elif sensor_type == 'WL':
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
                _sensors.append(sensor)

        else:
            logging.error('센서 타입이 올바르지 않습니다. : {}'.format(sensor_type))
            exit()

    else:
        logging.error('데이터 파일 경로가 올바르지 않습니다. {0}'.format(path))

    return _sensors


def validate_value(in_value):
    """
    실수형 값인지 확인하는 함수 실수가 아닌경우 0으로 
    :param in_value: 입력값
    :return: 실수값으로 변환된 값. 실수가 아닌 경우 0으로 반환
    """
    try:
        result = float(in_value)
    except ValueError as ex:
        err_message = ex.args[0]
        result = 0
        logging.error('[Validate Exception]:{0}, replaced with {1}'.format(err_message, result))

    return result if not math.isnan(result) else 0


def process_pl_data(df_raw_data, sensors, cfg, cnxns):
    """
    원천데이터에서 값을 추출하고 센서별 팩터등을 적용해서 산출값을 계산. 마지막으로, DB에 센서 원천값 및 산출값을 저장
    :param cnxns: DB 컨넥션
    :param cfg: 환경설정값
    :param df_raw_data: 원천데이터
    :param sensors: 센서정보 리스트
    :return:
    """
    for idx, row in df_raw_data.iterrows():
        # string을 datetime으로 변경
        timestamp = datetime.strptime(row['TIMESTAMP'], '%Y-%m-%d %H:%M:%S')
        sensors[0]['M01'] = validate_value(row['X_Reading'])
        sensors[1]['M01'] = validate_value(row['Y_Reading'])
        sensors[2]['M01'] = validate_value(row['Z_Reading'])

        # PL 계산식을 각각의 센서객체(PL01-X, PL01-Y, PL01-Z)에 동적으로 추가
        for sensor in sensors:
            sensor['OBSDT'] = timestamp.strftime("%Y%m%d%H%M%S")
            sensor['DAMCD'] = cfg['DAM']['CODE']
            sensor.calculate = MethodType(calculate_method, sensor)
            sensor.calculate()

            # 플럼라인
            query = "INSERT INTO DULLNDGGDT(OBSDT, DAMCD, SENID, MEAVAL1, CALVAL1, CALVAL2, CALVAL3, CALVAL4) " \
                    "VALUES('{}', '2012101', 'PL0103', {}, {}, {}, {}, {})".format(
                sensor['OBSDT'],
                sensor['DAMCD'],
                sensor['SENID'],
                sensor['M01'],
                sensor['C01'],
                sensor['C02'],
                sensor['C03'],
                sensor['C04'],
            )

            try:
                for _connection in cnxns:
                    logging.info('[QUERY] {}'.format(query))
                    if _connection is not None:
                        cursor = _connection.cursor()
                        cursor.execute(query)
                        _connection.commit()

            except pyodbc.Error as ex:
                sqlstate = ex.args[0]
                logging.error('[pyodbc Exception] sqlstate:{0}'.format(sqlstate))
                continue


def process_wl_data(df_raw_data, sensors, cfg, cnxns):
    """
    원천데이터에서 값을 추출하고 센서별 팩터등을 적용해서 산출값을 계산. 마지막으로, DB에 센서 원천값 및 산출값을 저장
    :param cnxns: DB 컨넥션
    :param df_raw_data: 원천데이터
    :param sensors: 센서정보 리스트
    :param cfg: 환경설정값
    :return:
    """
    for idx, row in df_raw_data.iterrows():
        # string을 datetime으로 변경
        timestamp = datetime.strptime(row['TIMESTAMP'], '%Y-%m-%d %H:%M:%S')
        sensors[0]['OBSDT'] = sensors[1]['OBSDT'] = timestamp.strftime("%Y%m%d%H%M%S")
        sensors[0]['DAMCD'] = sensors[1]['DAMCD'] = cfg['DAM']['CODE']
        sensors[0]['M01'] = validate_value(row['AVW_Avg(1,1)'])
        sensors[0]['M02'] = validate_value(row['AVW_Avg~2'])
        sensors[1]['M01'] = validate_value(row['AVW_Avg(2,1)'])
        sensors[1]['M02'] = validate_value(row['AVW_Avg~4'])

        # WL 계산식을 각각의 센서객체(WL1, WL2)에 동적으로 추가
        for sensor in sensors:
            sensor.calculate = MethodType(calculate_method, sensor)
            sensor.calculate()

            # 지하수위계 쿼리
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

            try:
                for _connection in cnxns:
                    logging.info('[QUERY]: {}'.format(query))
                    if _connection is not None:
                        cursor = _connection.cursor()
                        cursor.execute(query)
                        _connection.commit()

            except pyodbc.Error as ex:
                sqlstate = ex.args[0]
                logging.error('[pyodbc Exception] sqlstate:{0}'.format(sqlstate))
                continue


def get_db_connection(cfg):
    provider1 = 'DRIVER={{Tibero 5 ODBC Driver}};SERVER={0};PORT={1};UID={2};PWD={3};DATABASE={4}'.format(
        cfg['DB1']['IP'],
        cfg['DB1']['PORT'],
        cfg['DB1']['UID'],
        cfg['DB1']['PWD'],
        cfg['DB1']['DATABASE'],
    )

    provider2 = 'DRIVER={{Tibero 5 ODBC Driver}};SERVER={0};PORT={1};UID={2};PWD={3};DATABASE={4}'.format(
        cfg['DB2']['IP'],
        cfg['DB2']['PORT'],
        cfg['DB2']['UID'],
        cfg['DB2']['PWD'],
        cfg['DB2']['DATABASE'],
    )

    _cnxn1 = None
    _cnxn2 = None

    try:
        _cnxn1 = pyodbc.connect(provider1)
        _cnxn2 = pyodbc.connect(provider2)
    except pyodbc.InterfaceError as ex:
        # error_state = ex.args[0]
        logging.error('[DB 연결 오류] {0}'.format(ex))

    return [_cnxn1, _cnxn2]


def close_connection(cnxns):
    for cnxn in cnxns:
        if cnxn is not None:
            cnxn.close()


if __name__ == '__main__':
    import logging

    logging.basicConfig(
        filename='app.log',
        level=logging.DEBUG,
    )

    config = load_config('./config.ini')
    logging.info('> 환경설정 파일 로딩 완료')

    logging.info('> 플럼라인 데이터 처리 시작')
    # 플럼라인 원천데이터를 읽음
    data_filepath = os.path.join('./data', config['PL']['DATA_FILE'])
    df_pl_raw_data = get_raw_data(data_filepath)
    # 플럼라인 센서정보를 읽음
    info_filepath = os.path.abspath(config['PL']['SENSOR_FILE'])
    pl_sensors = get_sensor_info(info_filepath, 'PL')
    # DB 컨넥션을 획득
    connections = get_db_connection(config)
    # raw 파일과 info 파일의 데이터프레임의 생성이 정상적이라면 raw 파일을 한 줄씩 읽어서 연산한 다음 DB에 저장한다.
    process_pl_data(df_pl_raw_data, pl_sensors, config, connections)
    # DB 컨넥션을 반환
    # close_connection(connections)
    logging.info('> 플럼라인 데이터 처리 완료')

    logging.info('> 지하수위계 데이터 처리 시작')
    # 지하수위계 원천데이터를 읽음
    data_filepath = os.path.join('./data', config['WL']['DATA_FILE'])
    df_wl_raw_data = get_raw_data(data_filepath)
    # 지하수위계 센서정보를 읽음
    info_filepath = os.path.abspath(config['WL']['SENSOR_FILE'])
    wl_sensors = get_sensor_info(info_filepath, 'WL')
    # DB 컨넥션을 획득
    # connections = get_db_connection(config)
    # raw 파일과 info 파일의 데이터프레임의 생성이 정상적이라면 raw 파일을 한 줄씩 읽어서 연산한 다음 DB에 저장한다.
    process_wl_data(df_wl_raw_data, wl_sensors, config, connections)
    # DB 컨넥션을 반환
    close_connection(connections)
    logging.info('> 지하수위계 데이터 처리 완료')
