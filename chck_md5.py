# -*- coding: utf-8 -*-

import subprocess
import time

md5sum = {}
docker_config = {'recording.yaml':'ar_recording',
                 'sender_telegram.yaml':'sender_telegram',
                 'sender_slack.yaml':'sender_slack',
                 'parser_alertman.yaml':'parser_alertman',
                 'parser_grafana.yaml':'parser_grafana',
                 'catcher.yaml':'AR_ar_catcher'}

file_list = ['catcher.yaml','recording.yaml','parser_alertman.yaml','parser_grafana.yaml','sender_slack.yaml','sender_telegram.yaml']


def get_md5sum():
    for file_ in file_list:
        cmd = [ 'md5sum', '/ar_config/'+str(file_)]
        run_cmd = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        chcksum = run_cmd.stdout.read().decode('utf-8').split(' ')[0]
        md5sum[file_] = chcksum


def restart_docker(container):
    cmd = ['docker','restart',str(container)]
    run_cmd = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    result = run_cmd.stdout.read().decode('utf-8')
    print(result)


def chck_md5sum():
    while True:
        for file_ in file_list:
            cmd = [ 'md5sum', '/ar_config/'+str(file_)]
            run_cmd = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            chcksum = run_cmd.stdout.read().decode('utf-8').split(' ')[0]
            if str(chcksum) == str(md5sum.get(file_)):
                None
            else:
                md5sum[file_] = chcksum
                restart_docker(container=docker_config.get(file_))
            time.sleep(5)

if __name__ == '__main__':
    get_md5sum()
    chck_md5sum()
