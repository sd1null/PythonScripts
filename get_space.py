# -*- coding: utf-8 -*-
#Что учитывать?
#Два типа ссылок с id и name страницы
#Имя страницы может быть с пробелами
#В англоязычных названиях указывается имя(title) страницы вместо id номера
#Короткие ссылки через кнопку "поделиться" содержат /x/
#Примеры:
# https://wiki.confluence.io/pages/viewpage.action?pageId=24922401
# https://wiki.confluence.io/display/WEB/Retro
# https://wiki.confluence.io/display/WEB/MNP+WebForm
# https://wiki.confluence.io/x/eaY1Ag

import base64
import requests
import json
import click

auth=(base64.b64decode(user).decode('utf-8'), base64.b64decode(passwd).decode('utf-8'))

def get_page_id(url):
    url_list = []
    url_list.append(url)
    for i in range(len(url_list)):
        if 'display' in url_list[i]:
            format_url = url_list[i]
            format_url = format_url.split('/')
            format_url = format_url[-1]
            context = format_url.replace('+','%20')
            try:
                url_api = 'https://wiki.confluence.io/rest/api/content?title=' + context
                resp = requests.get(auth=auth,url=url_api)
                jdata = resp.json()
                page_id = jdata['results'][0]['id']
                return page_id
            except:
                print('Could not determine page id')
        if 'pageId' in url_list[i]:
            format_url = url_list[i]
            page_id = format_url.split('=')[1]
            return page_id
        if '/x/' in url_list[i]:
            resp = requests.get(url,auth=auth)
            god_url = resp.url
            return get_page_id(god_url)
        else:
            print('Could not determine link')

@click.command()
@click.option('--url', default=None, help='Link to define space Confluence')
def get_name_space(url):
    """The application returns the name of the space to the Confluence

Example usage: --url https://wiki.confluence.io/display/WEB/Retro

Copyright (c) 2021 sd1null in /home"""
    try:
        page_id = get_page_id(url)
        url = 'https://wiki.tele2.ru/rest/api/content/' + page_id
        resp = requests.get(auth=auth,url=url)
        key = resp.json()
        namespace = key['space']['name']
        print(namespace)
    except:
        print('Could not determine space name! Use --help')

if name == 'main':
    get_name_space()