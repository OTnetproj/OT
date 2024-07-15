import os
import psutil
import json
import time
import datetime
import requests
import logging
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning

elk_pass = os.getenv('ELASTIC_PASSWORD')
url = "https://eesgi10.ee.bgu.ac.il:9200/servers/_doc?pipeline=add_date"
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# define info and error logs for post requests
logging.basicConfig(level=logging.INFO, filename="/var/log/OT/servers_handler.log", filemode="w", format='%(asctime)s - %(levelname)s - %(message)s')

def monitor(prev):
	current = set()
	for conn in psutil.net_connections():
		if conn.status=='ESTABLISHED' and conn.raddr.port==502:
			current.add((conn.raddr.ip, conn.laddr.port))
	new_servers = current - prev
	to_remove_servers = prev - current
	current_details=[{"IP": ip, "Port": port} for ip, port in current]
	current_servers_json = {
		"message_type": "repetetive_check",
		"servers_count": len(current),
		"servers_details": current_details,
		"timestamp": datetime.datetime.now().isoformat()
	}
	print(json.dumps(current_servers_json)) # debug
	if(len(new_servers)):
		new_servers_add(new_servers)
	if(len(to_remove_servers)):
		old_servers_removal(to_remove_servers)
	post_to_elastic(current_servers_json)
	return list(current)

def new_servers_add(new):
	new_details=[{"IP": ip, "Port": port} for ip, port in new]
	new_servers_json = {
		"message_type": "new_servers_added",
		"servers_count_diff": len(new),
		"servers_details_diff": new_details,
		"timestamp": datetime.datetime.now().isoformat()
	}
	print(new_servers_json) # debug
	post_to_elastic(new_servers_json)


def old_servers_removal(old):
	old_details=[{"IP": ip, "Port": port} for ip, port in old]
	old_servers_json = {
		"message_type": "server_disconnected",
		"servers_count_diff": len(old),
		"servers_details_diff": old_details,
		"timestamp": datetime.datetime.now().isoformat()
	}
	post_to_elastic(old_servers_json)

def post_to_elastic(payload):
	response = requests.post(
		url,
		auth=HTTPBasicAuth('elastic', elk_pass),
		headers={'Content-Type': 'application/json'},
		json=payload,
		verify=False
	)
	if response.status_code not in [200,201]:
		logging.error(f"Error: Received status code {response.status_code}")
	else:
		logging.info(f"Info: Received status code {response.status_code}")
	print(f"response status is: {response.status_code}")


def main():
	current_servers = set()
	for conn in psutil.net_connections():
		if conn.status == 'ESTABLISHED' and conn.raddr.port==502:
			current_servers.add((conn.raddr.ip, conn.laddr.port))
	while True:
		time.sleep(10)
		update = monitor(current_servers)
		current_servers.clear()
		current_servers.update(update)


if __name__ == "__main__":
	main()
