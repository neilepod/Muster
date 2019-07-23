#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Author:-Nilay Trivedi
#dATE:-23/7/2019
import pandas as pd
import json
import requests
import os
import MySQLdb
#change db according to database name and host
seed = 238610
n = 200
#Change file name and destination acccording to output file
with open('../secrets.json') as data_file:
    CLIENT_SECRETS = json.load(data_file)
USERNAME = CLIENT_SECRETS['username']
PASSWORD = CLIENT_SECRETS['password']
db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                     user=USERNAME,  # your username
                     passwd=PASSWORD,  # your password
                     db="musterid",  #Change database name
                     charset='utf8',
                     port=3306)  # name of the data base
df_panchayats = pd.read_sql('''select panchayat_code from panchayats where state_code in (17,34);''', con=db)
db.close()
sample = df_panchayats.sample(n=n,random_state=seed)
panchayat_list = sample.to_dict('records')
url = 'http://nregarep2.nic.in/netnrega/nregapost/API_API3.asmx/API_API3_Workers'
#Url def __str__musterid
try:
    os.remove('output/workers.json')
except:
    'No file found'
for i, item in enumerate(panchayat_list):
    try:

        print 'Getting data for muster', str(i + 1), 'of', n

        post_data = {
            'panchayat_code': item['panchayat_code'],
            'mustrolid': item['msr_no'],
            'Workid': ''
        }
        r = requests.post(url, data=post_data)
        r_dict = r.json()
        r_data = json.loads(r_dict['reponse_data'])
        # Need to update this part
        # r_data = [{'panchayat_code':item['panchayat_code'],'payment_date': muster['payment_date'], 'total_dues': muster['total_dues'], 'tot_persondays': muster['tot_persondays'], 'msr_no': muster['msr_no'], 'muster_roll_period_from': muster['muster_roll_period_from'], 'muster_roll_period_to': muster['muster_roll_period_to']} for muster in r_data]
        with open('output/workers.json', 'a') as outfile:
            json.dump(r_data, outfile, ensure_ascii=False)
    except Exception as e:
        print 'Got an error:'
        print str(e)
