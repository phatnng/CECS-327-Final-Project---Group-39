from pymongo import MongoClient
from datetime import datetime

client = MongoClient("my db")
db = client["test"]
collection = db["327IoT_virtual"]

pipeline = [
    {
        '$lookup': {
            'from': '327IoT_metadata', 
            'localField': 'payload.parent_asset_uid', 
            'foreignField': 'assetUid', 
            'as': 'metadata'
        }
    }, {
        '$unwind': {
            'path': '$metadata'
        }
    }, {
        '$project': {
            'Name': '$metadata.customAttributes.name', 
            '_id': 1, 
            'time': 1,  
            'Amps': {
                '$ifNull': [
                    '$payload.Ammeter for SF1', {
                        '$ifNull': [
                            '$payload.Ammeter for SF2', '$payload.Ammeter for DW'
                        ]
                    }
                ]
            }, 
            'Fahrenheit': {
                '$ifNull': [
                    '$payload.Thermistor for SF1', '$payload.Thermistor for SF2'
                ]
            }, 
            'Humidity': {
                '$ifNull': [
                    '$payload.Thermistor for SF1', '$payload.Thermistor for SF2'
                ]
            }, 
            'Gallons': '$payload.Water Sensor for DW'
        }
    }
]

cursor = collection.aggregate(pipeline)

data_table = {}
for document in cursor:
    key = str(document["_id"])
    data_table[key] = document

client.close()
for data in data_table.items():
    print(f"{data}")