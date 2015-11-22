from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from math import radians, cos, sin, asin, sqrt
import folium
import os
import time
import sys

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 15)

os.system("firefox &")

inp_lines = ssc.socketTextStream(str(sys.argv[1]), int(sys.argv[2]))
batch = inp_lines.map(lambda line: line.split(" "))
processed_records = []
counter = 1

def process(record):
	curr = {}
	for field in record:
		field = str(field)
		idx = field.split(':')[0]
		val = field.split(':')[1]
		curr[idx] = val
	temp_curr = {}
	temp_curr["chipid"] = curr["chipid"]
	temp_curr["lat"] = curr["lat"]
	temp_curr["long"] = curr["long"]
	temp_curr["speed"] = curr["speed"]
	if(curr["accident"] == str(1)):
		temp_curr["alert"] = "red"	
	else:
		if(float(curr['speed']) >= 70.0):
			temp_curr["alert"] = "orange"
		else:
			temp_curr["alert"] = "green"
	processed_records.append(temp_curr)


def output_batch():
	global  processed_records
	if len(processed_records)==0:
		print ("%s" % "empty batch")
		return

	for record in processed_records:
		print ("%s " % record['alert'])


def get_distance(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km

def find_traffic():
	global processed_records
	mini=10**10
	for record in processed_records:
		vehicle_count = 0
		clat = record['lat']
		clon = record['long']
		curr_sum = 0
		for r in processed_records:
			d = get_distance(clon,clat,r['long'],r['lat'])
			#print("d=%s" %str(d))
			if d<5.0 and float(r['speed'])<50:
				curr_sum+=d
				vehicle_count+=1
	#	print ("%s" % str(vehicle_count))
		if curr_sum<mini and vehicle_count>4:
			mini=curr_sum
			plat = record['lat']
			plon = record['long']
	if mini!=(10**10):
		#print ("here")
		return plat,plon
		#curr_map.circle_marker(location=[float(plat),float(plon)], radius=100,popup='Traffic', line_color='red',fill_color='red')		

def plot_map():
	global processed_records
	global counter

	cnt = avg_lat = avg_long = 0
	to_map = []
	for record in processed_records:
		if record["alert"] != "green":
			to_map.append(record)
			cnt += 1
			avg_lat += float(record["lat"])
			avg_long += float(record["long"])
	
	if cnt == 0:
		return

	avg_lat /= cnt
	avg_long /= cnt
	plat,plon = find_traffic()
	plat,plon = float(plat),float(plon)
	plat = (avg_lat+plat)/2
	plon = (avg_long+plon)/2
	map_1 = folium.Map(location=[avg_lat, avg_long], zoom_start=12)
	for record in to_map:
		description = str(record["chipid"]) + ":  " + str(record["lat"]) + ", " + str(record["long"])
		map_1.simple_marker([record["lat"], record["long"]], popup=description, marker_color=record["alert"])
	#os.remove("plot1.html")
	#plat,plon = find_traffic()
	#print ("%s" % str(plat))
	map_1.circle_marker(location=[float(plat),float(plon)], radius=1500,popup='Traffic', line_color='#3186cc',fill_color='#3186cc')
	path = "plot" + str(counter) + ".html"
	map_1.create_map(path=path)

		
def f(block):
	global processed_records
	global counter

	processed_records = []

	block = block.collect()
	for records in block:
		process(records)
	
	plot_map()
	command = "firefox plot" + str(counter) + ".html"
	os.system(command)
	counter += 1
	print("%s" % "New Batch...!!!")
	#output_batch()	
 
batch.foreachRDD(f)

ssc.start()
ssc.awaitTermination()
