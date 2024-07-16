import time
import json
from datetime import datetime
import os
import logging
import redis
from elasticsearch import Elasticsearch, helpers

# init clients from outside list and remote registers define
file_path='ModbusServers.txt'
WATER_LEVEL_ADDR = 0        # input register address for tank's water level
HIGH_MARK_ADDR = 0          # discrete input and holding register addresses for HIGH mark state and threshold value 
LOW_MARK_ADDR = 1           # discrete input and holding register addresses for LOW mark state and threshold value
WATER_PUMP_ADDR = 0         # Coils address for water tank's pump status

# define log file
logging.basicConfig(level=logging.INFO, filename="/var/log/OT/redis2es.log", filemode="w", format='%(asctime)s - %(levelname)s - %(message)s')

# define remote redis cluster / container
redis_host = 'eesgi10.ee.bgu.ac.il'
redis_port=6379
redis_index= ['modbusclientsreports', 'databank'] # list of the indexes stored on redis - must be lowercases only!

# define elasticsearch remote container
es_host = 'https://eesgi10.ee.bgu.ac.il:9200'
es_username = 'elastic'
es_pass = os.getenv('ELASTIC_PASSWORD')

# Establish connections with redis and elasticsearch containerized services
try:
    r = redis.Redis(host=redis_host,port=redis_port,decode_responses=True)
    es = Elasticsearch([es_host], basic_auth=(es_username,es_pass), verify_certs=False)
    logging.info(f"Info: Connection to redis container {redis_host}:{redis_port} has established")
except redis.ConnectionError as e:
    logging.error(f"Error: Redis connection has failed: {e}")
    exit(1)

def main():
    while True:
        try:
            for index in redis_index:
                json_items = r.lrange(index,0,-1)
                if not json_items:
                    print(f"{index} in redis is empty")
                    logging.info(f"Info: {index} has no new docs in redis")
                else:
                    data = [json.loads(item) for item in json_items]
                    actions = [
                        {
                            "_index": index,
                            "_source": d
                        }
                        for d in data
                    ]
                    helpers.bulk(es, actions)
                    r.delete(index)
                    print(f"Send bulk of index: {index} to ES node")
                    logging.info(f"Info: sent bulk of index {index} to ES node")
            time.sleep(10)
        except KeyboardInterrupt as e:
            logging.error(f"Error: {e}")
            break


if __name__ == "__main__":
   main()





