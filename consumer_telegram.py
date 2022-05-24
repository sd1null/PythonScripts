# -*- coding: utf-8 -*-

import json
import time
import re
from kafka import KafkaConsumer
import json
import urllib3
import yaml
import sys
import telebot
from telebot import apihelper
import datetime
import logging
import psycopg2
from contextlib import closing
import sql_requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 


with open(r'./sender_telegram.yaml', "r") as ymlfile:
    cfg = yaml.load(ymlfile,Loader=yaml.FullLoader)
    try:
        proxy = cfg['telegram_config']['proxy_config']
        token = cfg['telegram_config']['token']
        bootstrap_servers = cfg['kafka_config']['bootstrap_servers']
        group_id = cfg['kafka_config']['group_id']
        client_id = cfg['kafka_config']['client_id']
        topic = cfg['kafka_config']['topic']
        default_route = cfg['telegram_config']['default_route']
        duty_channel = cfg['telegram_config']['duty_channel']
        duty_tags = cfg['duty_tags']
        routing = cfg['routing']
        log_level = cfg['log_level']
        db_name = cfg['database_config']['db_name']
        db_user = cfg['database_config']['db_user']
        db_pass = cfg['database_config']['db_pass']
        db_host = cfg['database_config']['db_host']
    except Exception as e:
        logging.error('Error reading sections! - {}'.format(str(e)))
        sys.exit('Error reading sections! Exit.')

def get_log_path(log_path: str):
    if str(log_path[-1:]) != '/':
        return str(log_path) + '/'
    else:
        return log_path

def get_log_levels(level: str):
    levels = {'DEBUG':logging.DEBUG,'INFO':logging.INFO,'WARNING':logging.WARNING,'ERROR':logging.ERROR,'CRITICAL':logging.CRITICAL}
    return levels.get(level)

logging.basicConfig(level=get_log_levels(log_level),filename=r'/opt/ar/logs/sender_telegram.log', format='{"timestamp":"%(asctime)s","severity":"%(levelname)s","message":"%(message)s","func_name":"%(funcName)s"}')
status_alert = {'alerting':'\U0001F534','ok':'\U0001F7E2','firing':'\U0001F534','resolved':'\U0001F7E2','no_data':'\U0001F7E3'}


def search_tag(message: str):
    str_message = message
    f_message = str_message.lower()
    pattern_tag = r'#\w{1,}'
    tags = re.search(pattern_tag,str(f_message))
    if tags == None:
        logging.warning('Tag not found')
        return 0
    else:
        tags = str(tags.group()).replace('#','')
        logging.info('Tag = %s',tags)
        return tags

def search_tags_perf(message: str):
    str_message = message
    f_message = str_message.lower()
    tfs_ = re.search(r'#!add_perf',str(f_message))
    if tfs_ == None:
        None
    else:
        return tfs_.group()

def check_button(message: str):
    buttons = telebot.types.InlineKeyboardMarkup(row_width=3)
    verify = telebot.types.InlineKeyboardButton('✅ Verify',callback_data='verify')
    bpm_btn = telebot.types.InlineKeyboardButton('▶️ In BPM',callback_data='bpm') 
    tfs_btn = telebot.types.InlineKeyboardButton('◀️ In TFS',callback_data='tfs')
    perfomance = telebot.types.InlineKeyboardButton('🛠 add performance',callback_data='perf') 
    tag = search_tags_perf(message=message)
    if tag == None:
        buttons.add(tfs_btn,verify,bpm_btn)
        return buttons
    if tag == '#!add_perf':
        buttons.add(bpm_btn,verify,tfs_btn,perfomance)
        return buttons
  

def send_message_duty(token,proxy,uuid,alert_id,title,url_title,message,status,source,metrics=None):
    bot = telebot.TeleBot(token)
    apihelper.proxy = proxy
    if str(status) == 'ok' or str(status) == 'resolved':
        try:
            mess_id = sql_requests.get_duty_alert(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=str(alert_id))
            if mess_id != None: # Если есть message_id
                bot.delete_message(chat_id=str(duty_channel),message_id=str(mess_id)) # Удалить из duty_channel
                time.sleep(1)
                log = sql_requests.delete_duty_alert(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=str(alert_id)) # Удалить из бд
            else:
                logging.info('Не найден алерт для резолва в таблице активных алертов!')
        except Exception as e:
            logging.error('Ошибка резолва алерта! - {}'.format(str(e)))
            return
    else:
        alert_message = str(status_alert.get(status)) + '#'+str(alert_id) +','+str(title)+ ' from '+ str(source) + '\n\n' +'Source: '+str(source)+'\n\n'+ str(url_title) + '\n\n'+ str(message) + '\n\n' + str(metrics) + '\n\n' +str(uuid)
        ids = sql_requests.get_duty_alert(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=str(alert_id))
        if ids != None: 
            try:
                bot.edit_message_text(text=alert_message + '\nUpd: \U0001F53B [Alerting!] ' + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),chat_id=duty_channel,message_id=str(ids))
            except Exception as e:
                logging.error('Ошибка создания треда алерта! - {}'.format(str(e)))
        else:
            try:
                msg = bot.send_message(chat_id=str(duty_channel),text=alert_message)
                time.sleep(1)
                message_id = msg.message_id # Получаем message_id
                sql_requests.create_duty_alerts(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=alert_id,message_id=message_id,alert_message=alert_message)
            except Exception as e:
                logging.error('Ошибка отправки алерта в дежурный канал,ID канала:'+str(duty_channel)+' - {}'.format(str(e)))
                time.sleep(2)

def send_message(token,proxy,datetime_,uuid,alert_id,title,url_title,url_img,message,status,source,chat_id,metrics=None):
    bot = telebot.TeleBot(token)
    apihelper.proxy = proxy
    timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    tags = search_tag(message)
    if str(status) == 'ok' or str(status) == 'resolved':
        try:
            mmu = sql_requests.get_active_alert(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=str(alert_id))
            if mmu != None: # Если есть что резолвить
                if int(mmu[3]) == 0: 
                    alert_message = str(status_alert.get(status)) + '#'+str(alert_id) +','+str(title)+ ' from '+ str(source) + '\n\n' +'Source: '+str(source)+'\n\n'+ str(url_title) + '\n\n' + str(message) + '\n\n'+ str(mmu[1])+'\n\n' + str(mmu[2])
                else: # Если был звонок по алерту добавляем иконку звонка
                    alert_message = str('\U0001F4F2 ') + str(status_alert.get(status)) + '#'+str(alert_id) +','+str(title)+ ' from '+ str(source) + '\n\n' +'Source: '+str(source)+'\n\n'+ str(url_title) + '\n\n' + str(message) + '\n\n'+ str(mmu[1])+'\n\n' + str(mmu[2])
                bot.edit_message_text(text=alert_message,chat_id=chat_id,message_id=str(mmu[0]))
                sql_requests.resolve_alert(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=str(alert_id),resolve_date=timestamp)
                time.sleep(1)
                log = sql_requests.delete_active_alert(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=str(alert_id))
            else: # Если нечего резолвить пришёл
                logging.info('Не найден алерт для резолва в таблице активных алертов!')
        except Exception as e:
            logging.error('Ошибка резолва алерта! - {}'.format(str(e)))
            return
    else:
        alert_message = str(status_alert.get(status)) + '#'+str(alert_id) +','+str(title)+ ' from '+ str(source) + '\n\n' +'Source: '+str(source)+'\n\n'+ str(url_title) + '\n\n'+ str(message) + '\n\n' + str(metrics) + '\n\n' +str(uuid)
        ids = sql_requests.get_active_alert(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=str(alert_id))
        if ids != None: # Если есть активный алерт, добавляем тред
            bot.edit_message_text(text=alert_message + '\nUpd: \U0001F53B [Alerting!] ' + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),chat_id=chat_id,message_id=str(ids[0]),reply_markup=check_button(alert_message))
        else:
            try:
                msg = bot.send_message(chat_id=chat_id,text=alert_message,reply_markup=check_button(alert_message))
                time.sleep(1)
                message_id = msg.message_id
                sql_requests.create_new_alert(db_name=db_name,db_user=db_user,
                                          db_pass=db_pass,db_host=db_host,
                                          alert_id=alert_id,title=title,
                                          create_date=timestamp,source=source,
                                          tag=tags,metrics=metrics,uuid=uuid,
                                          alert_message=alert_message)
                sql_requests.create_active_alerts(db_name=db_name,db_user=db_user,
                                              db_pass=db_pass,db_host=db_host,
                                              alert_id=alert_id,message_id=message_id,
                                              alert_message=alert_message,metrics=metrics,
                                              uuid=uuid,create_datetime=timestamp,tag=tags,tg_chat_id=chat_id)
            except Exception as e:
                logging.error('Ошибка отправки и записи алерта в бд! Тег алерта:'+str(tags)+',ID канала:'+str(chat_id)+' - {}'.format(str(e)))
                time.sleep(2)

consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,client_id=client_id,group_id=group_id,value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(topic)

def get_message_kafka():
    for msg in consumer:
        jdata = json.loads(msg.value)
        title=jdata['title']
        message_=jdata['message']
        url_title=jdata['url_title']
        alert_id=jdata['alert_id']
        status=jdata['status']
        img_url=jdata['img_url']
        metrics=jdata['metrics']
        source=jdata['source']
        datetime_ = jdata['datetime']
        uuid = jdata['uuid']
        tags = search_tag(message_)
        logging.info('Message received from Kafka - '+str(title)+'Tag:'+str(tags))
        if tags in duty_tags: 
            if status == 'no_data':
                None
            else: 
                send_message_duty(token=token,uuid=uuid,proxy=proxy,alert_id=alert_id,title=title,url_title=url_title,message=message_,status=status,source=source,metrics=metrics)
                time.sleep(3)
        else:
            None
        try:
            if status == 'no_data':
                sql_requests.create_no_data_alerts(db_name=db_name,db_user=db_user,db_pass=db_pass,db_host=db_host,alert_id=alert_id,title=title,create_date=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            else:
                for id in routing[str(tags)]:
                    send_message(token=token,datetime_=datetime_,uuid=uuid,proxy=proxy,alert_id=alert_id,title=title,url_title=url_title,url_img=img_url,message=message_,status=status,source=source,chat_id=id,metrics=metrics)
                    time.sleep(3)  
        except KeyError as e:
            logging.warning('Тег не найден, алерт отправлен в канал по умолчанию!')
            id = default_route
            send_message(token=token,datetime_=datetime_,uuid=uuid,proxy=proxy,alert_id=alert_id,title=title,url_title=url_title,url_img=img_url,message=message_,status=status,source=source,chat_id=id,metrics=metrics)
            time.sleep(3)

if __name__ == '__main__':
  get_message_kafka()
