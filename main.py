import time

from coinbase.websocket import WSClient, WebsocketResponse
import json, csv, os, gzip, datetime
from datetime import datetime
import subprocess

last_price = 0
buffered_data = []


def get_time():
	return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


def log(data):
	global last_price
	global buffered_data
	if data.type != 'ticker':
		return
	if last_price == data.price:
		return
	last_price = data.price
	buffered_data.append([data.price, data.volume_24_h, data.low_24_h, data.high_24_h, data.low_52_w, data.high_52_w,
	                      data.price_percent_chg_24_h, data.best_bid, data.best_ask, data.best_bid_quantity,
	                      data.best_ask_quantity])
	if len(buffered_data) >= 50:
		with open('output.csv', 'a+') as f:
			writer = csv.writer(f)
			writer.writerows(buffered_data)
		buffered_data.clear()
		if os.stat('output.csv').st_size > 1 * 1024 * 1024 * 1024:
			new_name = get_time() + ".csv"
			print('compressing', new_name)
			os.rename('output.csv', new_name)
			subprocess.Popen(["gzip", new_name])


def on_message(msg):
	ws_object = WebsocketResponse(json.loads(msg))
	if ws_object.channel == 'ticker':
		for event in ws_object.events:
			if event.type == 'update':
				for ticker in event.tickers:
					log(ticker)


if __name__ == '__main__':
	while 1:
		try:
			print("running")
			client = WSClient(key_file='./coinbase_cloud_api_key.json', on_message=on_message)
			client.open()
			client.subscribe(product_ids=["BTC-USD"], channels=["ticker", "heartbeats"])
		except Exception as e:
			print("client crashed", e)
		time.sleep(5)
