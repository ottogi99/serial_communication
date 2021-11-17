a = 0x00
b = 0xE9
c = 0xEE
d = 0x36
e = b'\x00\xE9\xEE\x36'

print(e[0:2])
print(e[2:4])
print('test1: {0}'.format(int.from_bytes(b'\xEE\x36', byteorder='big')))
print('test2: {0}'.format(int.from_bytes(b'\x00\xE9', byteorder='big')))


value = b'\x00\x01\x00\x02\x00\x03'
print(value[:2])
print(value[2:4])
print(value[-2:])
