import time
from time import sleep
from spicepy import Client

###########################
#   Spice AI Platform     #
###########################

# use API key from SpiceAI app to instantiate a client
client = Client('353231|e9dd434b7ba54fc99b027a7c6d326ecd')

startTime = time.time()
data = client.query('SELECT * FROM eth.recent_traces trace JOIN eth.recent_transactions trans ON trace.transaction_hash = trans.hash ORDER BY trans.block_number DESC;')
endTime = time.time()
pd = data.read_all()

print(pd)
print("Query Time: " + str(endTime - startTime) + " seconds\n")

exit()

client = Client('353231|e9dd434b7ba54fc99b027a7c6d326ecd', 'grpc://127.0.0.1:50051')

###########################
#   Spice AI Datasource   #
###########################

while True:
    startTime = time.time()
    data = client.query('SELECT * FROM eth_recent_traces trace JOIN eth_recent_transactions trans ON trace.transaction_hash = trans.hash ORDER BY trans.block_number DESC;')
    endTime = time.time()
    pd = data.read_all()

    print(pd)
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    # startTime = time.time()
    # data = client.query('SELECT * FROM eth.recent_traces trace JOIN eth.recent_transactions trans ON trace.transaction_hash = trans.hash ORDER BY trans.block_number DESC;')
    # endTime = time.time()
    # pd = data.read_all()

    # print(pd)
    # print("Query Time: " + str(endTime - startTime) + " seconds\n")

    sleep(5)

###########################
#    Dremio Datasource    #
###########################

while True:
    startTime = time.time()
    data = client.query('SELECT * FROM taxi_trips ORDER BY pickup_datetime DESC LIMIT 100')
    endTime = time.time()
    pd = data.read_all()

    print(pd)
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    startTime = time.time()
    data = client.query('SELECT count(*) FROM taxi_trips')
    endTime = time.time()
    pd = data.read_all()

    print(pd)
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    sleep(5)

###########################
# Spice/Dremio Datasource #
###########################

while True:
    startTime = time.time()
    data = client.query("""
        SELECT DISTINCT
            eth_recent_blocks.number as block_number, 
            taxi_trips.trip_distance_mi
        FROM eth_recent_blocks 
        LEFT JOIN taxi_trips 
        ON eth_recent_blocks.number%100 = taxi_trips.trip_distance_mi*10
        ORDER BY eth_recent_blocks.number DESC                
        LIMIT 10
        """)
    endTime = time.time()
    pd = data.read_all()

    print(pd)
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    sleep(5)