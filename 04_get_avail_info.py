import requests
import urllib
import json
import pandas as pd
from tqdm import tqdm
from tqdm.notebook import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from fake_useragent import UserAgent
import os
from requests.adapters import HTTPAdapter
#from requests.packages.urllib3.util.retry import Retry
from fake_useragent import UserAgent
from tqdm import tqdm
import time
import asyncio
import aiohttp
from tqdm.asyncio import tqdm_asyncio
import datetime

df_port = pd.read_csv('/home/ec2-user/get_charger_avail/all_station_port_ids.csv')
df_port2 = df_port
timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# Async function to fetch data from URL
async def fetch_data(session, url, headers):
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError:
        return None

# Async function to process each device ID
async def process_device(session, device_id, user_agent):
    url = df_port2[df_port2['device_id'] == device_id].iloc[0]['url_price']
    if pd.notna(url):
        headers = {'User-Agent': user_agent.random}
        json_data = await fetch_data(session, url, headers)
        return device_id, json_data
    else:
        return device_id, None

# Async main function to run the program
async def main():
    unique_device_ids = df_port2['device_id'].unique()
    device_responses = {}
    user_agent = UserAgent()

    async with aiohttp.ClientSession() as session:
        tasks = [process_device(session, device_id, user_agent) for device_id in unique_device_ids]

        for future in tqdm_asyncio(asyncio.as_completed(tasks), total=len(unique_device_ids), desc="Fetching data"):
            device_id, json_data = await future
            device_responses[device_id] = json_data

    for index, row in tqdm(df_port2.iterrows(), total=df_port2.shape[0], desc="Updating availability"):
      device_id = row['device_id']
      port_id = row['port_id']
      json_data = device_responses[device_id]

      if json_data and 'ports' in json_data.get('portsInfo', {}):
          status_list = [port['status'] for port in json_data['portsInfo']['ports']]
          port_number = int(port_id.split('_')[1]) - 1
          #df_port2.at[index, 'availability'] = status_list[port_number]
          if 0 <= port_number < len(status_list):
              df_port2.at[index, 'availability'] = status_list[port_number]
          else:
              print(f"Invalid port_number {port_number} for device_id {device_id}")
      else:
          df_port2.at[index, 'availability'] = 'Missing'

# Function to handle the async execution
def run_async_main():
    loop = asyncio.get_event_loop()
    if loop.is_running():
        task = loop.create_task(main())
    else:
        loop.run_until_complete(main())

run_async_main()
df_port2.to_csv('df_port2_' + timestamp + '.csv', index=False)