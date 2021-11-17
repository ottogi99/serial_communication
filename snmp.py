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

# 하류 한강기전 UPS
# HOST = '172.30.15.30'
# PORT = 7002

# 상류 한강기전 UPS
HOST = '172.30.15.55'
PORT = 7006

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    print('connected')
    s.connect((HOST, PORT))
    # while True:
    cmd = 'Q1{0}'.format('\r')
    print('send command: {0}'.format(cmd.encode()))
    s.send(cmd.encode())
    # time.sleep(1)
    buffer = s.recv(1024)
    print(buffer)
    cmd = 'F{0}'.format('\r')
    print('send command: {0}'.format(cmd.encode()))
    s.send(cmd.encode())
    buffer = s.recv(1024)
    print(buffer)
    s.close()
    print('disconnected')

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
