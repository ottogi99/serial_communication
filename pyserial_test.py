import socket
import serial
import time

# 콘솔 입력: input()
# 콘솔 출력: print()

# python 에는 switch-case 문이 없다.
# 그래서 딕셔너리를 이용해 마치 switch-case 문 처럼 사용할 수 있다.

# x가 'a'라면 1
def f(x):
    return {'a': 1, 'b': 2}[x]


# 디폴트 값 설정
def f(x):
    return {'a':1, 'b':2}.get(x, '3')


# SM225 명령어 셋
commands = {
    '0': '#GET_DATA',
    '1': '#GET_PEAKS_AND_LEVELS',
    '2': '#HELP',
    '3': '#IDN?',
    '4': '#GET_SYSTEM_IMAGE_ID',
    '5': '#GET_PRODUCT_SN',
    '6': '#SET_IP_ADDRESS',
    '7': '#SET_IP_NETMASK',
    '8': '#GET_IP_ADDRESS',
    '9': '#GET_IP_NETMASK',
    '10': '#GET_DUT[Ch]_STATE',
    '11': '#SET_DUT[Ch]_STATE',
    '12': '#REBOOT_SYSTEM',
    '13': '#SET_PEAK_THRESHOLD_CH[Ch]',
    '14': '#GET_PEAK_THRESHOLD_CH[Ch]',
    '15': '#SET_REL_PEAK_THRESOLD_CH[Ch]',
    '16': '#GET_REL_PEAK_THRESOLD_CH[Ch]',
    '17': '#SET_PEAK_WIDTH_CH[Ch]',
    '18': '#GET_PEAK_WIDTH_CH[Ch]',
    '19': '#SET_PEAK_WIDTH_LEVEL_CH[Ch]',
    '20': '#GET_PEAK_WIDTH_LEVEL_CH[Ch]',
    'Q': 'QUIT',
}


def get_commands(x):
    return commands.get(x, '3')


port = input("Input COM Port: ")
ser = serial.Serial(port, 115200)
try:
    ser.open()
except serial.SerialException as e:
    print(str(e))

while True:
    for (key, value) in commands.items():
        print('[{0}] {1}'.format(key, value))

    cmd = get_commands(input("Input Command number: "))

    if cmd == 'Q':
        break
    else:
        ser.write(cmd.encode())
        time.sleep(10)
        response = ser.read()
        print(response)

ser.close()
# 출력부
# f = open('out.txt', 'w')
# print(1, 2, 3, 4, 5, file = f)
# f.close()
#
# open('out.txt').read()