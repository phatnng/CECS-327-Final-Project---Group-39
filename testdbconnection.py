from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import pytz


def query_collection(filter_pipeline, key_field="_id"):
    """
    Helper function to query the database with a specific pipeline and return results as a hashmap.
    - `filter_pipeline`: The MongoDB aggregation pipeline to filter and process the data.
    - `key_field`: The field to use as the key for the resulting hashmap.
    """
    # Connect to MongoDB using the provided URI
    client = MongoClient("mongodb+srv://frankieandrade1127:AEHx7Srq0Wg1EhWg@327cluster.kkgy3.mongodb.net/?retryWrites=true&w=majority&appName=327Cluster")
    db = client["test"]  # Access the "test" database
    collection = db["327IoT_virtual"]  # Access the "327IoT_virtual" collection

    # Execute the aggregation pipeline and create a dictionary of results
    cursor = collection.aggregate(filter_pipeline)
    data = {str(doc[key_field]): doc for doc in cursor if key_field in doc}  # Build the hashmap

    client.close()  # Close the database connection
    return data


def calc_avg_moisture():
    """
    Calculates the average humidity inside the Smart Refrigerator for the last 3 hours.
    """
    # Define the aggregation pipeline
    pipeline = [
        {
            '$lookup': {
                'from': '327IoT_metadata',
                'localField': 'payload.parent_asset_uid',
                'foreignField': 'assetUid',
                'as': 'metadata'
            }
        },
        {'$unwind': '$metadata'},
        {
            '$match': {
                'metadata.customAttributes.name': 'Smart Refrigerator',
                'payload.Humidity for SF1': {'$exists': True}
            }
        },
        {
            '$project': {
                '_id': 1,
                'time': '$payload.timestamp',
                'Humidity': '$payload.Humidity for SF1',
            }
        }
    ]

    documents = query_collection(pipeline)

    # Determine the cutoff time for the last 3 hours
    current_time = datetime.now()
    cutoff_time = current_time - timedelta(hours=3)
    pst_timezone = pytz.timezone('US/Pacific')
    formatted_time = cutoff_time.astimezone(pst_timezone).strftime('%I:%M %p %Z')    

    # Filter documents within the last 3 hours
    filtered_docs = {
        key: doc for key, doc in documents.items()
        if datetime.fromtimestamp(int(doc['time'])) > cutoff_time
    }

    # Calculate the average humidity
    total_humidity = sum(float(doc['Humidity']) for doc in filtered_docs.values())
    count = len(filtered_docs)

    if count == 0:
        return "No humidity data available for the Smart Refrigerator in the last 3 hours."

    avg_humidity = total_humidity / count
    return f'Average moisture of the Kitchen fridge is {avg_humidity:.2f}% RH since {formatted_time}'


def calc_max_electricity():
    """
    Identifies which IoT device consumed the most electricity and calculates its usage.
    """
    pipeline = [
        {
            '$lookup': {
                'from': '327IoT_metadata',
                'localField': 'payload.parent_asset_uid',
                'foreignField': 'assetUid',
                'as': 'metadata'
            }
        },
        {'$unwind': '$metadata'},
        {
            '$match': {
                'metadata.customAttributes.name': {
                    '$in': ['Smart Refrigerator', 'Smart Refrigerator 2', 'Smart Dishwasher']
                }
            }
        },
        {
            '$project': {
                '_id': 1,
                'Name': '$metadata.customAttributes.name',
                'Amps': {
                    '$ifNull': [
                        '$payload.Ammeter for SF1',
                        {
                            '$ifNull': [
                                '$payload.Ammeter for SF2',
                                {
                                    '$ifNull': ['$payload.Ammeter for DW', None]
                                }
                            ]
                        }
                    ]
                }
            }
        }
    ]

    documents = query_collection(pipeline)

    # Accumulate electricity usage per device
    electricity_usage = {}
    for doc in documents.values():
        device_name = doc.get('Name')
        amps = doc.get('Amps')
        if device_name and amps is not None:
            if device_name not in electricity_usage:
                electricity_usage[device_name] = 0
            electricity_usage[device_name] += float(amps)

    # Determine the device with maximum electricity usage
    max_device = max(electricity_usage, key=electricity_usage.get)
    max_value = (((electricity_usage[max_device] / 30) * 120) / 1000)  # Convert to kilowatt-hours
    return f'{max_device} used the most electricity with {max_value:.2f} kWh'


def calc_avg_cycle():
    """
    Calculates the average water consumption per cycle for the Smart Dishwasher.
    """
    pipeline = [
        {
            '$lookup': {
                'from': '327IoT_metadata',
                'localField': 'payload.parent_asset_uid',
                'foreignField': 'assetUid',
                'as': 'metadata'
            }
        },
        {'$unwind': '$metadata'},
        {
            '$match': {
                'metadata.customAttributes.name': 'Smart Dishwasher',
                'payload.Water Sensor for DW': {'$exists': True}
            }
        },
        {
            '$project': {
                '_id': 1,
                'Gallons': '$payload.Water Sensor for DW'
            }
        }
    ]

    documents = query_collection(pipeline)

    # Calculate the average water usage per cycle
    total_water = sum(float(doc['Gallons']) for doc in documents.values())
    count = len(documents)

    if count == 0:
        return "No data available for water consumption cycles."

    avg_water = total_water / count
    return f"Average water consumption per cycle for the Smart Dishwasher: {avg_water:.2f} gallons."


def main():
    """
    Main function to execute the calculations and print the results.
    """
    print(calc_max_electricity())
    print(calc_avg_moisture())
    print(calc_avg_cycle())


# Run the main function if this script is executed
if __name__ == "__main__":
    main()
