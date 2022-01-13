import socket
import re
from datetime import datetime
import pyodbc
import time


class RtuItems:
    req_datetime = ''
    items = []

    def set_datetime(self, req_datetime):
        self.req_datetime = req_datetime

    def __init__(self):
        pass


class RtuItem:
    def __init__(self, name, value, status):
        self.name = name
        self.value = value
        self.status = status

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value

    def get_status(self):
        return self.status

    def set_name(self, name):
        self.name = name

    def set_value(self, value):
        self.value = value

    def set_status(self, status):
        self.status = status

    def __str__(self):
        return '{0},{1},{2}'.format(self.name, self.value, self.status)


class RtuClient:
    _commands = ['DATA', 'TSET', 'RSET']
    stx = bytes([0x02])
    etx = bytes([0x03])
    cr = bytes([0x0D])
    rtu_Items = RtuItems()

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.dam_code = '4007110'
        self.timeout = 10
        self.request_frame = b''
        self.cmd_code = ''
        self.buffer = ''
        self.rtu_Items.items = []

    def crc16_ccitt(self, crc, data):
        msb = crc >> 8
        lsb = crc & 255
        for c in data:
            x = ord(c) ^ msb
            x ^= (x >> 4)
            msb = (lsb ^ (x >> 3) ^ (x << 4)) & 255
            lsb = (x ^ (x << 5)) & 255
        return (msb << 8) + lsb

    def make_crc(self, body):
        icrc = self.crc16_ccitt(0xFFFF, body)
        xcrc = '{0:04X}'.format(icrc)
        # print('xcrc:{0}'.format(xcrc))

        crc = xcrc[0].encode()
        crc += xcrc[1].encode()
        crc += xcrc[2].encode()
        crc += xcrc[3].encode()

        # print('CRC:{0}'.format(crc))
        return crc

    def set_dam_code(self, code):
        self.dam_code = code

    def set_socket_timeout(self, sec):
        self.timeout = sec

    def get_buffer(self):
        return self.buffer

    def _create_tset_message(self, req_datetime):
        message = 'TSET000{dam_code}00{datetime}'.format(dam_code=self.dam_code, datetime=req_datetime)
        return message

    def _create_rset_message(self):
        message = 'REST000{dam_code}00'.format(dam_code=self.dam_code)
        return message

    def _create_data_message(self, req_datetime):
        message = 'DATA000{dam_code}00{datetime}'.format(dam_code=self.dam_code, datetime=req_datetime)
        return message

    def make_packet(self, cmd, req_datetime=''):
        if cmd != 'DATA' and cmd != 'TSET' and cmd != 'REST':
            return

        self.cmd_code = cmd
        print('make_req_datetime:{0}'.format(req_datetime))
        self.rtu_Items.req_datetime = req_datetime
        self.request_frame = self.stx

        if cmd == 'DATA':
            message = self._create_data_message(req_datetime)
            crc = self.make_crc(message)
            self.request_frame += message.encode()
            self.request_frame += self.etx
            self.request_frame += crc

        elif cmd == 'TSET':
            now = datetime.now()
            req_datetime = '{0}{1:02d}{2:02d}{3:02d}{4:02d}'.format(now.year, now.month, now.day, now.hour, now.minute)
            message = self._create_tset_message(req_datetime)
            crc = self.make_crc(message)
            self.request_frame += message.encode()
            self.request_frame += self.etx
            self.request_frame += crc

        elif cmd == 'REST':
            message = self._create_rset_message()
            crc = self.make_crc(message)
            self.request_frame += message.encode()
            self.request_frame += self.etx
            self.request_frame += crc

        self.request_frame += self.cr

    def send_packet(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(self.timeout)
            s.connect((self.host, self.port))
            s.send(self.request_frame)
            print('보낸패킷:{0}'.format(self.request_frame))
            self.parse_packet(s.recv(512))

    def parse_packet(self, buffer, req_datetime):
        # 체크섬
        start_index = -1
        message = ''
        cmd_code = ''
        dam_code = ''
        find_stx = False
        i = 0
        while i < len(buffer):
            if hex(buffer[i]) == '0x2':
                print('STX 발견')
                cmd_code = buffer[i+1:i+5].decode()
                dam_code = re.sub('[^0-9]', '', buffer[i + 5:i + 15].decode())
                buf_req_datetime = buffer[i + 29:i + 41].decode()  # 저장시간(요청시간)(12)
                data_length = buffer[i + 41:i + 44].decode()  # 데이터총길이(3)

                if self.cmd_code != cmd_code or self.dam_code != dam_code or req_datetime != buf_req_datetime:
                    if self.cmd_code != cmd_code:
                        print('명령코드 불일치')
                    if self.dam_code != dam_code:
                        print('댐코드 불일치:{0}'.format(dam_code))
                    if req_datetime != buf_req_datetime:
                        print('요청시간 불일치:{0}/{1}'.format(req_datetime, buf_req_datetime))

                    i += int(data_length)
                    continue

                start_index = i

            if hex(buffer[i]) == '0x3' and start_index > -1:
                print('ETX 발견')
                print('start_index:{0}'.format(start_index))
                message = buffer[start_index+1:i]
                break

            i += 1

        # 프레임을 찾은 경우 데이터 파싱
        if message != '':
            print(message)
            self.rtu_Items.set_datetime(req_datetime)

            item_cnt = buffer[start_index+44:start_index+46].decode()  # 항목수(2)
            print('item_cnt:{0}'.format(int(item_cnt)))

            loop_cnt = 0
            i = start_index+46
            while loop_cnt < int(item_cnt):
                name = buffer[i:i+6].decode()       # 측정항목
                i += 6
                value = buffer[i:i+10].decode()     # 측정값
                i += 10
                status = buffer[i:i+2].decode()     # 상태
                i += 2
                self.rtu_Items.items.append(RtuItem(name, value, status))
                loop_cnt += 1

            etx = buffer[i:i+1]
            i += 1
            r_crc = buffer[i:i+1].decode()
            i += 1
            r_crc += buffer[i:i+1].decode()
            i += 1
            r_crc += buffer[i:i+1].decode()
            i += 1
            r_crc += buffer[i:i+1].decode()
            i += 1

            crc = self.crc16_ccitt(0xFFFF, message.decode())
            crc = '{0:04X}'.format(crc)
            # print('CRC:{0}'.format(crc))

            if r_crc != crc:
                print('r_crc:{0}'.format(r_crc))
                print('crc:{0}'.format(crc))
                print('CRC 에러 발생')

            cr = buffer[i:i+1]

        # for buf in buffer:
        #     i = i + 1
        #     if hex(buf) == '0x2':
        #         continue
        #     if hex(buf) == '0x3':
        #         message = buffer[1:i-1]
        #         break
        #
        # if cmd_code == 'DATA':
        #     rtu_datetime = buffer[17:29].decode()       # 현재시간(12)
        #     req_datetime = buffer[29:41].decode()       # 저장시간(요청시간)(12)
        #     self.rtu_Items.set_datetime(req_datetime)
        #
        #     data_length = buffer[41:44].decode()        # 데이터총길이(3)
        #     item_cnt = buffer[44:46].decode()           # 항목수(2)
        #     print('item_cnt:{0}'.format(int(item_cnt)))
        #
        #     loop_cnt = 0
        #     i = 46
        #
        #     while loop_cnt < int(item_cnt):
        #         name = buffer[i:i+6].decode()
        #         i = i + 6
        #         value = buffer[i:i+10].decode()
        #         i = i + 10
        #         status = buffer[i:i+2].decode()
        #         i = i + 2
        #         self.rtu_Items.items.append(RtuItem(name, value, status))
        #         loop_cnt += 1
        #
        #     etx = buffer[i:i + 1]
        #     i += 1
        #     r_crc = buffer[i:i+1].decode()
        #     i += 1
        #     r_crc += buffer[i:i+1].decode()
        #     i += 1
        #     r_crc += buffer[i:i+1].decode()
        #     i += 1
        #     r_crc += buffer[i:i+1].decode()
        #     i += 1
        #
        #     cr = buffer[i:i + 1]
        #
        #     crc = self.crc16_ccitt(0xFFFF, message.decode())
        #     crc = '{0:04X}'.format(crc)
        #     # print('CRC:{0}'.format(crc))
        #
        #     if r_crc != crc:
        #         print('r_crc:{0}'.format(r_crc))
        #         print('crc:{0}'.format(crc))
        #         print('CRC 에러 발생')
        #
        # else:   # TSET, REST
        #     print('수신버퍼:{0}'.format(buffer))
        #     if len(buffer) != 24:
        #         print('수신 프레임 길이 오류')
        #
        #     message = buffer[1:18]
        #     r_crc = buffer[19:20].decode()
        #     r_crc += buffer[20:21].decode()
        #     r_crc += buffer[21:22].decode()
        #     r_crc += buffer[22:23].decode()
        #
        #     crc = self.crc16_ccitt(0xFFFF, message.decode())
        #     crc = '{0:X}'.format(crc)
        #
        #     if r_crc != crc:
        #         print('r_crc:{0}'.format(r_crc))
        #         print('crc:{0}'.format(crc))
        #         print('CRC 에러 발생')
        #
        #     status_code = buffer[17:18]
        #     print(status_code)
        #     if status_code == bytes([0x10]):
        #         print('상태코드: 데이터가 존재하지 않는다')
        #     elif status_code == bytes([0x15]):
        #         print('상태코드: 실패')
        #     elif status_code == bytes([0x06]):
        #         print('상태코드: 성공')
        #         pass

    def create_output(self, filename=''):
        if filename == '':
            now = datetime.now()
            # filename = 'rtu_{0}{1:02d}{2:02d}{3:02d}{4:02d}.txt'.format(now.year, now.month, now.day, now.hour, now.minute)
            filename = 'received.txt'
            f = open(filename, 'a')

        output_list = []

        for item in self.rtu_Items.items:
            output_list.append(self.rtu_Items.req_datetime)
            output_list.append(item.name)
            output_list.append(re.sub('[^0-9\.]', '', item.value))

            f.write(','.join(output_list))
            f.write('\n')
            output_list.clear()

        f.close()

    def __str__(self):
        pass


class OdbcDao:
    def __init__(self, dsn, uid, pwd):
        self.dsn = dsn
        self.uid = uid
        self.pwd = pwd

    # def make_query(self, damcd, name, obsdt, value):
    #     code = '수온/전기전도도/탁도/누수량/월류수심 (TB0001/TB0002/TB0003/LWC001/LWC002)'
    #     # query = "INSERT INTO DULFDDT (DAMCD, DAMORGNO, DAMLYGGB, MEASURDT, LGMSVALU, CONNCTTP, CALCULATE) " \
    #     #         "VALUES ('{0}', '1001', '{1}', TO_DATE('{2}', 'yyyymmddhh24mi'), {3}, '1', 'N')"\
    #     #         .format(damcd, name, obsdt, value)
    #     query = "INSERT INTO DULPCLTWTDT (OBSDT, DAMCD, SENID, DIVNO, MEACYCLE, MEAVAL) " \
    #             "VALUES (TO_DATE('{0}', 'yyyymmddhh24mi'), '{0}', {1}, '1001', '{1}', , {3}, '1', 'N')"\
    #             .format(obsdt, damcd, senid, divno, 5, value)
    #     return query

    def make_query(self, obsdt, damcd, senid, divno, value):
        query = "INSERT INTO DULPCLTWTDT (OBSDT, DAMCD, SENID, DIVNO, MEACYCLE, MEAVAL) " \
                "VALUES (TO_DATE('{0}', 'yyyymmddhh24mi'), '{1}', '{2}', {3}, {4}, {5})"\
                .format(obsdt, damcd, senid, divno, 5, value)
        return query

    def execute_query(self, query):
        try:
            cnxn = pyodbc.connect('DSN={0};UID={1};PWD={2}'.format(self.dsn, self.uid, self.pwd))
            cursor = cnxn.cursor()
            cursor.execute(query)
            cnxn.commit()
            cnxn.close()
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print('[pyodbc Exception] sqlstate:{0}'.format(sqlstate))


# TB0001, 1, 온도
# TB0001, 2, 전기전도도
# TB0001, 3, 탁도
# LWC001, 1, 월류수심
# LWC001, 2, 누수량
def get_senid_n_divno(self, name):
    divno = name[-1:]
    senid = "{0}{1:0>2}".format(name[0:5], divno)

    return senid, divno

# 파일 읽어오는 부분
dam_code = ''
try:
    f = open('request.txt', 'r')
    lines = f.readlines()
    for line in lines:
        req_items = line.split(',')
        dam_code = req_items[0]
        rtu = RtuClient('192.168.10.181', 5500)
        # request.set_socket_timeout(5)
        rtu.set_dam_code(dam_code)
        # request.make_packet('TSET')
        # request.make_packet('REST')
        rtu.make_packet('DATA', req_items[1])

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # s.settimeout(request.timeout)
            s.connect((request.host, request.port))
            s.send(rtu.request_frame)
            print('보낸패킷:{0}'.format(rtu.request_frame))
            time.sleep(3)
            buf = s.recv(512)
            print('받은패킷:{0}'.format(buf))
            print('받은패킷길이:{0}'.format(len(buf)))
            rtu.parse_packet(buf, req_items[1].strip('\r').strip('\n'))

        # request.send_packet()
        rtu.create_output()
except IOError as e:
    print('I/O error:({0}): {1}'.format(e.errno, e.strerror))
    exit()

# names = ['LWC001', 'LWC002', 'TB0001', 'TB0002', 'TB0003']

fn = 'received.txt'
with open(fn, 'r') as f:
    lines = f.readlines()
    print('lines:{0}'.format(lines))

fn = 'failed.txt'
with open(fn, 'w') as f:
    dao_list = []
    output_list = []

    dao_list.append(OdbcDao('T_LOCAL_64', 'layme', 'thdwlwls_2020'))
    dao_list.append(OdbcDao('T_HQ_64', 'layme', 'thdwlwls_2020'))

    for dao in dao_list:
        try:
            cnxn = pyodbc.connect('DSN={0};UID={1};PWD={2}'.format(dao.dsn, dao.uid, dao.pwd))
            cursor = cnxn.cursor()

            for line in lines:
                line = line.strip('\n')
                recv_items = line.split(',')

                senid, divno = get_senid_n_divno(recv_items[1])
                insert_query = dao.make_query(recv_items[0], senid, divno, recv_items[2])
                print(insert_query)

                cursor.execute(cnxn, insert_query)
                cnxn.commit()

            cnxn.close()
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print('[pyodbc Exception] sqlstate:{0}'.format(sqlstate))
            output_list.append(line)

        for item in output_list:
            f.writelines(item+'\n')

