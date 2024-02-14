import os 
import time
from time import sleep
from spicepy import Client


API_KEY=os.environ.get("API_KEY")

client = Client(API_KEY)

startTime = time.time()

data = client.query('SELECT * FROM eth.recent_blocks ORDER BY number DESC')
endTime = time.time()
pd = data.read_pandas()

print(pd.to_string() + "\n")
print("Query Time: " + str(endTime - startTime) + " seconds\n")

exit()


###########################
#   Spice AI Datasource   #
###########################

client = Client(API_KEY, 'grpc://127.0.0.1:50051')

while True:
    startTime = time.time()
    data = client.query('SELECT * FROM eth_blocks ORDER BY number ASC')
    endTime = time.time()
    pd = data.read_pandas()

    print(pd.to_string() + "\n")
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    startTime = time.time()
    data = client.query('SELECT number FROM eth_blocks ORDER BY number ASC')
    endTime = time.time()
    pd = data.read_pandas()

    print(pd.to_string() + "\n")
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    sleep(5)

###########################
#    Dremio Datasource    #
###########################

while True:
    startTime = time.time()
    data = client.query('SELECT * FROM taxi_trips ORDER BY pickup_datetime ASC')
    endTime = time.time()
    pd = data.read_pandas()

    print(pd.to_string() + "\n")
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    startTime = time.time()
    data = client.query('SELECT count(*) FROM taxi_trips')
    endTime = time.time()
    pd = data.read_pandas()

    print(pd.to_string() + "\n")
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    sleep(5)

###########################
# Spice/Dremio Datasource #
###########################

while True:
    startTime = time.time()
    data = client.query("""
        SELECT DISTINCT
            eth_blocks.number as block_number, 
            taxi_trips.trip_distance_mi
        FROM eth_blocks 
        LEFT JOIN taxi_trips 
        ON eth_blocks.number%100 = taxi_trips.trip_distance_mi*10
        ORDER BY eth_blocks.number ASC                
        """)
    endTime = time.time()
    pd = data.read_pandas()

    print(pd.to_string() + "\n")
    print("Query Time: " + str(endTime - startTime) + " seconds\n")


    sleep(5)