import socket
import sys
import time
import random

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((str(sys.argv[1]), int(sys.argv[2])))
s.listen(1)
conn, addr = s.accept()

records = []

def generate():
	global records
	records = []
	chipid=0;
	hyd_lat=17.3700 
	hyd_long=78.4800
	for i in range(1,1000):
		record = ""
		chipid += 1
		#lat=hyd_lat+random.uniform(-3,2)
		#lon=hyd_long+random.uniform(-4.5,2)
		lat=hyd_lat+random.uniform(-0.15,0.15)
		lon=hyd_long+random.uniform(-0.15,0.15)
		speed = random.randint(1,10)*7.0
		flag = random.randint(1,100)
		if flag <= 99:
			accident = 0
		else:
			accident = 1
		record += "chipid:" + str(chipid) + " lat:" + str(lat) + " long:" + str(lon) + " speed:" + str(speed) + " accident:" + str(accident)
		record += '\n'
		records.append(record)

while 1:
	generate()
	for record in records:
		conn.send(record)
	print "data sent...!!"
	time.sleep(15)

conn.close()