from pymongo import MongoClient
from datetime import datetime, timedelta


def query_database():
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
    return data_table


def calc_avg_moisture(data_table):
    sum = 0
    counter = 0
    current_time = datetime.now()
    cutoff_time = current_time - timedelta(hours = 3)
    try:
        for key, data in data_table.items():
            if data.get('time') > cutoff_time and data.get('Name') == 'Smart Refrigerator' and data.get('Humidity') is not None:
                sum += float(data.get('Humidity'))
                print (sum)
                counter += 1
        return f'Average moisture of the Kitchen fidge is: {sum/counter:.2f}'
    except:
        print("Failed")

def calc_max_electricity(data_table):
    electricity_usage = {'Fridge 1': 0, 'Fridge 2': 0, 'Dishwasher': 0}
    for key, data in data_table.items():
        if data.get('Name') == 'Smart Refrigerator' and data.get('Amps') is not None:
            electricity_usage['Fridge 1'] += float(data.get('Amps'))
        if data.get('Name') == 'Smart Refrigerator 2' and data.get('Amps') is not None:
            electricity_usage['Fridge 2'] += float(data.get('Amps'))
        if data.get('Name') == 'Smart Dishwasher' and data.get('Amps') is not None:
            electricity_usage['Dishwasher'] += float(data.get('Amps'))
    print(electricity_usage)
    max_device = max(electricity_usage, key=electricity_usage.get)
    max_value = electricity_usage[max_device]
    return f'{max_device} used the most electricity with {max_value} Amps'

#def calc_avg_cycle(data_table):
    

def main():
    data_table = query_database()
    print(calc_max_electricity(data_table))
    #calc_avg_moisture(data_table)
    #for key, data in data_table.items():
    #    print(data)

main()

