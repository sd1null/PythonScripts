# -*- coding: utf-8 -*-
import json
from flask import Flask, request, Response
from flask import cli
from kafka import KafkaProducer
import datetime
import time
import yaml
import click
import sys
import uuid

CONFIG = []

def init_kafka(bootstrap_servers):
    global producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer = lambda v: json.dumps (v) .encode ('utf-8'))

cli.show_server_banner = lambda *_: None
alermnanager_service = Flask(__name__)
metrics_and_values = []

def getTimestamp_to_unix():
    d = datetime.datetime.now()
    unixtime = time.mktime(d.timetuple())
    return int(unixtime)

def parse_prom_alert(jdata):
    for x in range(len(jdata["alerts"])):
        metrics_and_values.append(jdata["alerts"][x]["labels"])
        title = jdata["alerts"][x]["annotations"]['title']
        message = jdata["alerts"][x]["annotations"]['description']
        url_title = jdata["alerts"][x]["generatorURL"]
        alert_id = jdata["alerts"][x]["fingerprint"]
        status = jdata["alerts"][x]["status"]
    metrics = str(metrics_and_values).strip('[]').replace("{'","").replace("'}","").replace("'","")
    data = {"title": title,
            "message": message,
            "url_title": url_title,
            "alert_id": alert_id,
            "status": status,
            "img_url": "",
            "metrics": str('Labels: ') + metrics,
            "datetime": str(getTimestamp_to_unix()),
            "uuid": str(uuid.uuid4()),
            "source": "Alertmanager"}
    jalert = json.dumps(data,ensure_ascii=False)
    producer.send(CONFIG[3],jalert)
    metrics_and_values.clear()

@alermnanager_service.route('/alert_router/alertmanager_parser', methods=['POST'])
def respond():
    jdata = request.json
    print(jdata)
    parse_prom_alert(jdata)
    return Response()

@alermnanager_service.route('/alert_router/metrics', methods=['GET'])
def get_healt_metrics():
    return 'alert_router_hc{service="alertman_alert_parser"} 1'

@click.command()
@click.option('--config', default=str, help='The path to the config yaml file')
def run_app(config):
    """Usage: --config [PATH]"""
    try:
        with open(config, "r") as ymlfile: 
            cfg = yaml.load(ymlfile,Loader=yaml.FullLoader)
            try:
                app_host = cfg['app_host']
                app_port = cfg['app_port']
                bootstrap_servers = cfg['kafka_config']['bootstrap_servers']
                topic = cfg['kafka_config']['topic']
            except:
                sys.exit('Error reading sections! Exit.')
        CONFIG.append(app_host)
        CONFIG.append(app_port)
        CONFIG.append(bootstrap_servers)
        CONFIG.append(topic)
        init_kafka(bootstrap_servers)
        start_server(host=CONFIG[0],port=CONFIG[1])
    except FileNotFoundError:
        sys.exit('Config yaml file not found! Exit.')

def start_server(host,port):
    alermnanager_service.run(debug=False,host=host,port=port)

if __name__ == '__main__':
    run_app()
