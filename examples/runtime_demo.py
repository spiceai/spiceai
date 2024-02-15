import time
from time import sleep
import threading
from spicepy import Client

###########################
#   Spice AI Platform     #
###########################

# use API key from SpiceAI app to instantiate a client
client = Client('YOUR_API_KEY',)

startTime = time.time()
data = client.query('SELECT * FROM eth.recent_traces trace JOIN eth.recent_transactions trans ON trace.transaction_hash = trans.hash ORDER BY trans.block_number DESC;')
pd = data.read_chunk()
endTime = time.time()

print("Query Time: " + str(endTime - startTime) + " seconds\n")

exit()

########################################
#   DO NOT COMMENT OUT THE LINE BELOW  #
########################################
client = Client('YOUR_API_KEY', 'grpc://127.0.0.1:50051')

###########################
#   Spice AI Datasource   #
###########################

while True:
    startTime = time.time()
    data = client.query('SELECT * FROM eth_recent_traces trace JOIN eth_recent_transactions trans ON trace.transaction_hash = trans.hash ORDER BY trans.block_number DESC;')
    endTime = time.time()
    pd = data.read_all()

    print(pd.to_string() + "\n")
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    # startTime = time.time()
    # data = client.query('SELECT * FROM eth_recent_traces trace JOIN eth_recent_transactions trans ON trace.transaction_hash = trans.hash ORDER BY trans.block_number DESC LIMIT 10;')
    # endTime = time.time()
    # pd = data.read_all()

    # print(pd.to_string() + "\n")
    # print("Query Time: " + str(endTime - startTime) + " seconds\n")

    sleep(5)

#####################################
#    High-RPS Queries Simulation    #
#####################################
def simulate_spice_user(user_id):
    print("User " + str(user_id) + " started\n")
    # make a new client for each user
    client = Client('YOUR_API_KEY', 'grpc://127.0.0.1:50051')
    start = time.time()
    # make a query
    data = client.query('SELECT * FROM eth_recent_traces trace JOIN eth_recent_transactions trans ON trace.transaction_hash = trans.hash ORDER BY trans.block_number DESC;')
    pd = data.read_all()
    end = time.time()
    total = end - start
    
    print("User" + str(user_id) + " took " + str(total) + " seconds\n")

# simulate the number of users
num_users = 10
threads = []

start_time = time.time()

for user_id in range(num_users):
    t = threading.Thread(target=simulate_spice_user, args=(user_id,))
    threads.append(t)
    t.start()

for thread in threads:
    thread.join()

end_time = time.time()

print("Total Time: " + str(end_time - start_time) + " seconds\n")

exit()


###########################
#    Dremio Datasource    #
###########################

while True:
    startTime = time.time()
    data = client.query('SELECT * FROM taxi_trips ORDER BY pickup_datetime DESC LIMIT 100')
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
            eth_recent_blocks.number as block_number, 
            taxi_trips.trip_distance_mi
        FROM eth_recent_blocks 
        LEFT JOIN taxi_trips 
        ON eth_recent_blocks.number%100 = taxi_trips.trip_distance_mi*10
        ORDER BY eth_recent_blocks.number DESC                
        LIMIT 10
        """)
    endTime = time.time()
    pd = data.read_pandas()

    print(pd.to_string() + "\n")
    print("Query Time: " + str(endTime - startTime) + " seconds\n")

    sleep(5)