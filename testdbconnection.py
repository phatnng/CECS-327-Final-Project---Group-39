from pymongo import MongoClient


client = MongoClient("message me for DB info")
db = client["test"]
collection = db["327IoT_virtual"]

pipeline = [
    {
        "$project": {
            "_id": 1,
            "Gallons": "$payload.Water Sensor for DW",
            "Amps": {
                "$ifNull": [
                    "$payload.Ammeter for SF1", 
                    {
                        "$ifNull": [
                            "$payload.Ammeter for SF2", 
                            "$payload.Ammeter for DW"
                        ]
                    }
                ]
            },
            "Fahrenheit": {
                "$ifNull": [
                    "$payload.Thermistor for SF1", 
                    "$payload.Thermistor for SF2"
                ]
            },
            "Humidity": {
                "$ifNull": [
                    "$payload.Thermistor for SF1", 
                    "$payload.Thermistor for SF2"
                ]
            },
            "asset_uid": "$payload.asset_uid"
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