from Client import X25

# 본댐
# HOST = '172.20.83.84'
# 보조여수로
HOST = '172.20.83.96'
PORT = 50000
client = X25(HOST, PORT)
client.connect()
client.getPeakAndLevel()
client.disconnect()

