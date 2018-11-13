from bs4 import BeautifulSoup

from rq import Queue, Connection
from rq import get_current_job

import redis
import requests
import numpy as np
import json

# path_style = 'stroke-width:3;stroke:rgb(1,1,0);opacity: 0.5;fill:'
path_style = 'opacity: 0.5;font-size:12px;fill-rule:nonzero;stroke:#FFFFFF;st roke-opacity:1;stroke-width:0.1;stroke-miterlimit:4;stroke-dasharray:none;stroke-linecap:butt;marker-start:none;stroke-linejoin:bevel;fill:'
text_style = 'transform:translate(-50px, 10px)'
colors = ['#ffffb2','#fed976','#feb24c','#fd8d3c','#f03b20','#bd0026']

def data_getter(influx_ip, time_start, time_end, bt_address, feature):
    data = "mean(" + feature + ") as \"mean_noise\""
    address = "\'" + bt_address + "\'"
    group_time = "time(" + str(1) + "h)"

    query = "SELECT " + data + " from \"autogen\".\"IFoT-GW1-Meas\" WHERE time > " + \
              str(int(time_start)) + " and time < " + \
              str(int(time_end)) + \
              " and \"bt_address\"=" + address + \
              " group by " + group_time + \
              " fill(none)"

    query_all_NUC_sensors = "SELECT " + data + " from \"autogen\".\"meas_1\" WHERE time > " + \
                          str(int(time_start)) + " and time < " + \
                          str(int(time_end)) + \
                          " and \"bt_address\"=" + address + \
                          " group by " + group_time + \
                          " fill(none)"

    #Need to fix this hard coded part!
    db = ''
    if influx_ip == '163.221.68.206':
      db = 'bl01_db'
      query = query_all_NUC_sensors
    elif influx_ip == '163.221.68.191':
      db = 'IFoT-GW1'
      query = query

    payload = {
    "db": db,
    "pretty": True,
    "epoch": 'ms',
    "q": query
    }

    r = requests.get('http://' + influx_ip + ':8086/query?', params=payload)

    
    j_r = json.loads(r.text)
    return j_r

def get_data_array(influx_ip, start_time, end_time, feature):
  # with open('sensor_location.txt') as f:
  #   data = json.load(f)
  
  # bts2 = []
  # [bts2.append(sensor['id']) for sensor in data]

  # bts2_dict = {}
  # for sensor in data:
  #   name = 'sensor' + sensor['number']
  #   bts2_dict[name] = sensor['id']

  with open('sensor_location.txt') as f:
    data = json.load(f)
  #     print(data)
  bts2 = []
  [bts2.append(sensor['id']) for sensor in data]


  bts2_dict = {}
  for i, sensor in enumerate(data):
    name = 'sensor' + sensor['number']
    bts2_dict[sensor['id']] = name

  print(bts2_dict)

  results = []
  for i, bt in enumerate(bts2):
    results.append(data_getter(influx_ip, start_time, end_time, bt, feature))

  print('Length:', len(results))

  temp_np_arr = {}
  for i, result in enumerate(results):
    if 'series' in result['results'][0]:
      values = result['results'][0]['series'][0]
      temp_np = np.asarray(values['values'])
      print(temp_np.shape)
      temp_np_arr[bts2[i]] = (temp_np)

  for key in temp_np_arr:
    tt = np.delete(temp_np_arr[key], 0, 1)

    # temp_np_arr[key] = np.mean(tt)

  # min_key = min(temp_np_arr, key = lambda x: temp_np_arr.get(x))
  # max_key = max(temp_np_arr, key = lambda x: temp_np_arr.get(x))
  
  return (bts2_dict, temp_np_arr)

def generate_json(bts2_dict, temp_np_arr):
  new_dict = {}
  first_key = next(iter(temp_np_arr))
  # print(first_key)
  timestamps = temp_np_arr[first_key][:, 0]
  new_dict["timestamps"] = timestamps.tolist()

  data = []
  for i, (k, v) in enumerate(temp_np_arr.items()):
  # for key in temp_np_arr:
      sensor_dict = {}
      b = np.delete(temp_np_arr[k], 0, axis=1)
      value_list = b.ravel().tolist()
      sensor_dict["bt_address"] = k
      sensor_dict["id"] = bts2_dict[k]
      sensor_dict["values"] = value_list
      data.append(sensor_dict)
  new_dict["data"] = data

  r = json.dumps(new_dict)
  return r

def readSVG(influx_ip, start_time, end_time, feature):
  svg = open('updated-test.svg', 'r').read()
  soup = BeautifulSoup(svg, 'xml', selfClosingTags=['defs','sodipodi:namedview'])
  bts2_dict, temp_np_arr = get_data_array(influx_ip, start_time, end_time, feature)

  json = generate_json(bts2_dict, temp_np_arr)
  return { 'svg_data': json, 'influx_ip': influx_ip }

  paths = soup.findAll('g', {'class': 'layer'})

  titles = soup.findAll('text', {'id': 'title'})
  for title in titles:
      title.string = feature
      
  temp_key = []
  for path in paths:
    circles = path.findAll('circle', {'class': 'sensor'})
    texts = path.findAll('text')
#     print(texts)
        
    rate_arr = []
    for circle, text in zip(circles, texts):
      key = bts2_dict[circle['id']]
      print(key)
      temp_key.append(key)
      if key in temp_np_arr:
        rate = temp_np_arr[bts2_dict[circle['id']]]
        rate_arr.append(rate)
        if rate > 37:
          color_class = 5
        elif rate > 36:
          color_class = 4
        elif rate > 35:
          color_class = 3
        elif rate > 34:
          color_class = 2
        elif rate > 33:
          color_class = 1
        else:
          color_class = 0

        color = colors[color_class]
        if rate < 0:
          rate *= -1
        text.string = str("{:.2f}".format(rate))
        text['style'] = text_style
        circle['r'] = rate * 5
        circle['style'] = path_style + color

  # return 'TEST1'
  return { 'svg_data': str(soup), 'temp' : str(temp_key), 'influx_ip': influx_ip}