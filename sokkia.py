import socket
import sys
import logging
import time
from logging import handlers

#log settings
logFormatter = logging.Formatter('%(asctime)s,%(message)s')

#handler settings
logHandler = handlers.TimedRotatingFileHandler(filename='sm225.log', when='midnight', interval=1, encoding='utf-8')
logHandler.setFormatter(logFormatter)
logHandler.suffix = "%Y%m%d"

#logger set
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logHandler)

HOST = '192.168.10.178'
# HOST = '172.30.15.30'
#PORT = 7000
# PORT = 7001
PORT = 7008

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    # while True:
    print('connected')
    cmd = '{0}'.format('\r')
    s.send(cmd.encode())
    time.sleep(1)
    cmd = '*PON{0}'.format('\r')
    s.send(cmd.encode())
    buffer = s.recv(1024)
    print(buffer)
    time.sleep(30)
    cmd = '*POFF{0}'.format('\r')
    s.send(cmd.encode())
    buffer = s.recv(1024)
    print(buffer)
    s.close()

        # cmd = input("보낼 명령어를 입력하세요: ")
        # if cmd == 'Q':
        #     sys.exit()
        # else:
        #     cmd = '{0}{1}'.format(cmd, '\r')
        #     s.send(cmd.encode())
        #     time.sleep(0.5)
        #     cmd = '*PON{0}'.format(cmd, '\r')
        #     s.send(cmd.encode())
        #
        # # print(cmd.encode())
        # #
        # buffer = s.recv(1024)
        # # print(buffer)
