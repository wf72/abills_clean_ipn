#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb as db
import ConfigParser
import datetime
import os


def config(section='Main'):
    """ConfigParser"""
    try:
        work_dir = os.path.dirname(os.path.realpath(__file__)) + os.sep
    except:
        work_dir = "./"
    defaultconfig = {'dbHost': '127.0.0.1',
                     'dbUser': 'abills',
                     'dbPassword': '',
                     'dbName': 'abills',
                     'keep_days': '120',
                     }
    Config = ConfigParser.SafeConfigParser(defaultconfig)
    Config.read(work_dir+"/settings.cfg")
    if section == 'DBConfig':
        return {'host': Config.get('DBConfig', 'dbHost'),
                'user': Config.get('DBConfig', 'dbUser'),
                'passwd': Config.get('DBConfig', 'dbPassword'),
                'db': Config.get('DBConfig', 'dbName'), }
    else:
        dict1 = {}
        options = Config.options(section)
        for option in options:
            try:
                dict1[option] = Config.get(section, option)
            except:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1


def DBConnect():
    try:
        con = db.connect(**config('DBConfig'))
        cur = con.cursor()
        cur.execute('SET NAMES `utf8`')
        return con, cur
    except db.Error:
        print(db.error())


def CleanDB():
    """Clean db tables ipn_traf_detail_YYYY_mm_dd"""
    con, cur = DBConnect()
    cur.execute('SELECT table_name FROM information_schema.tables where table_name like "ipn_traf_detail_%" ORDER BY table_name;')
    data = cur.fetchall()
    for table_name in data:
        if abs(datetime.datetime.now() - datetime.datetime.strptime(table_name[0][-10:],"%Y_%m_%d")).days > int(config('Main')['keep_days']):
            query = 'DELETE FROM %s' % table_name[0]
            cur.execute(query)
            query = 'OPTIMIZE TABLE %s' % table_name[0]
            cur.execute(query)


if __name__ == '__main__':
    CleanDB()
