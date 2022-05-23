# -*- coding: utf-8 -*-
import datetime
import time
from flask import Flask, Response
from prometheus_client import generate_latest, Gauge
import requests
import base64

plugin_exporter = Flask(name)
CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')

url_wiki = 'http://wiki.confluence.io/rest/plugins/1.0/'
user_ = base64.b64decode('user64encode').decode('utf-8')
pass_ = base64.b64decode('pass64encode').decode('utf-8')
content_type = 'application/vnd.atl.plugins+json'

def sorting_plugin_key(plugin_key):
    url =  url_wiki+plugin_key+'/license'
    req = requests.get(url=url,auth=(user_,pass_))
    resp = req.json()
    if 'expiryDate' in resp:
        try:
            exp_unix = resp['expiryDate']
            return exp_unix
        except:
            return 0
    else:
        return 0

def getTimestamp_to_unix():
    d = datetime.datetime.now()
    unixtime = time.mktime(d.timetuple())
    return int(unixtime)

def get_exp_days(exp_time_plugin: int):
    date_time_now = getTimestamp_to_unix()
    exp_time_unix = str(exp_time_plugin)[:-3].strip()
    exp_days = int(exp_time_unix) - int(date_time_now)
    exp_days = round(exp_days/60/60/24) 
    return int(exp_days)


metric = Gauge('plugin_expiration_days','Срок истечения плагина в днях',labelnames=['name','plugin_key'])

#
def plugin_metrics():
    req = requests.get(url=url_wiki,auth=(user_,pass_))
    resp = req.json()
    for x in range((len(resp['plugins']))):
        if resp['plugins'][x]['usesLicensing'] == True:
            key_ = resp['plugins'][x]['key']
            name_ = resp['plugins'][x]['name']
            key = key_+'-key'
            expiryDate = sorting_plugin_key(plugin_key=key)
            if expiryDate == 0:
                continue
            else:
                exp_days = get_exp_days(expiryDate)
                metric.labels(name_,key).set(exp_days)
        else:
            continue

@plugin_exporter.route('/metrics', methods=['GET'])
def get_metrics():
    plugin_metrics()
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

if name == 'main':
    plugin_exporter.run(debug=True,host='0.0.0.0',port=9091)