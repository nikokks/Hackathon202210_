import json
from pymongo import database
from pymongo.mongo_client import MongoClient

###
# This Sink service transform incoming WiFi data from one station (PC,Smartphone, IoT Device, Sound device ...) 
# into aggregate data and detect anomalies to improve WiFi services
# Example message:
# {
#  "info": {
#        "identifier": "GTW02",
#       "uploadTime" : 1665057512,
#        "manufacturerName" : "Sagemcom"
#    },
#    "wifiData": [
#        {
#            "eventTime" : 1665057512,
#            "deviceType": "PC",
#            "bytesSent": 782140,
#            "bytesReceived" : 15860542,
#            "connection": "2_4GHZ",
#            "rssi": -56
#        }
#     ]
# }
# 
###
# The expected data is an aggregation of:
# - WiFi metrics aggregation (min/max/avg)
# - Number of connection changes during the data collection
# - Monitoring RSSI value during the collection
#   - RSSI (Received Signal Strength Indication) --> Indication of WiFi Link quality --> A low value indicate a poor WiFi link quality

# Threshold used for RSSI
RSSITHRESHOLD = -70

#MongoDB ENV
MONGO_DB_HOST= "db1"
MONGO_DB_PORT = 27017
MONGO_DB_USERNAME = "mongodb"
MONGO_DB_PASSWORD = "mongodb"

MONGO_DB_DATABASE_NAME = "dataLake"
MONGO_DB_COLLECTION_NAME = "wifi"

# Main function
def sink_aggregation(json_data):
    startTime, endTime = find_min_max(json_data["wifiData"],"eventTime")
    minRSSI, maxRSSI, avgRSSI = find_min_max_avg(json_data["wifiData"],"rssi")

    aggregate_data = {
        "identifier": json_data["info"]["identifier"],
        "manufacturerName": json_data["info"]["manufacturerName"],
        "startTime": startTime,
        "endTime": endTime,
        "wifiAggregate": {
            "deviceType" : json_data["wifiData"][0]["deviceType"],
            "minRSSI": minRSSI,
            "avgRSSI": avgRSSI,
            "maxRSSI": maxRSSI,
            "countBandChange" : count_value_change(json_data["wifiData"],"connection")
        },
        "anomalies_report" : detect_anomaly_min(json_data["wifiData"],"rssi",RSSITHRESHOLD)
    }

    identifier_data = json_data["info"]["identifier"]

    # Mongo Insert data into mongoDB
    try:
        mongo_server = MongoClient(f"mongodb://{MONGO_DB_USERNAME}:{MONGO_DB_PASSWORD}@{MONGO_DB_HOST}:{MONGO_DB_PORT}/")
        insert_data_mongo(mongo_server,aggregate_data)        

        # Find the data into mongoDB
        result_aggregate_data = find_data_mongo(mongo_server,identifier_data)
    except:
        result_aggregate_data = {}
    return result_aggregate_data

def find_min_max(array,key):
    if (length :=len(array)) <= 0:
        return None, None
    if length % 2 == 1:
        min = max = array[0][key]
        begin = 1
    else:
        min, max = array[0][key], array[1][key]
        if min > max:
            min, max = max, min
        begin = 2
    for i in range(begin, length, 2):
        v1, v2 = array[i][key], array[i+1][key]
        if v1 > v2:
            v1, v2 = v2, v1 # guarantee v1 <= v2
        if v1 < min:
            min = v1
        if v2 > max:
            max = v2
    return min, max

def find_min_max_avg(array,key):
    if (length :=len(array)) <= 0:
        return None, None, None
    if length % 2 == 1:
        culmu = min = max = array[0][key]
        begin = 1
    else:
        min, max = array[0][key], array[1][key]
        culmu = min + max
        if min > max:
            min, max = max, min
        begin = 2
    for i in range(begin, length, 2):
        v1, v2 = array[i][key], array[i+1][key]
        culmu += v1
        culmu += v2
        if v1 > v2:
            v1, v2 = v2, v1 # guarantee v1 <= v2
        if v1 < min:
            min = v1
        if v2 > max:
            max = v2
    return min, max, culmu//length

def count_value_change(array,key):
    if len(array) > 0:
        counter = 0
        ref_value = array[0][key]
        for i in range(1, len(array)):
            if ref_value != (current := array[i][key]):
                counter += 1
                ref_value = current
        return counter
    return None

def detect_anomaly_min(array,key,threshold):
    array_anomaly = []
    for entry in array:
        if entry[key] < threshold:
            anomaly_report = {
                "eventTime": entry["eventTime"],
                "deviceType": entry["deviceType"],
                "rssi": entry["rssi"],
                "connection": entry["connection"]
            }
            array_anomaly.append(anomaly_report)
    return array_anomaly


# Connect on mongodb and post one aggregated WiFi Data
def insert_data_mongo(mongo_server: MongoClient,json_insert):
    try:
        collection = mongo_server[MONGO_DB_DATABASE_NAME][MONGO_DB_COLLECTION_NAME]
        collection.insert_one(json_insert)
        return True
    except OverflowError:
        print("Cannot put data into MongoDB")
        return False

def find_data_mongo(mongo_server: MongoClient, identifier:str):
    result = {}
    try:
        result = mongo_server[MONGO_DB_DATABASE_NAME][MONGO_DB_COLLECTION_NAME]\
            .find_one({"identifier": identifier},{"_id": 0})
    except:
        print("Cannot find data into MongoDB")
    return result