import re
import sys
import json
import time
import random
import traceback
from subprocess import call
import datetime

def yesterday():
    '''
    返回昨天的日期，返回格式为'%Y%m%d'
    '''
    return (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

def today():
    '''
    返回今天的日期，返回格式为'%Y%m%d'
    '''
    return datetime.date.today().strftime('%Y%m%d')

def date_range(start_dt, end_dt):
    '''
    返回start_dt到end_dt的所有日期，返回格式为list
    输入格式为'%Y%m%d'
    date_range('20190504', '20190506')=['20190504', '20190505', '20190506']
    '''
    try:
        start_dt = datetime.date(int(start_dt[:4]), int(start_dt[4:6]), int(start_dt[6:]))
        end_dt = datetime.date(int(end_dt[:4]), int(end_dt[4:6]), int(end_dt[6:]))
        if end_dt < start_dt:
            return []
        dt_range = range((end_dt - start_dt).days + 1)
        if len(dt_range) > 10000:
            return []
        return [str(start_dt + datetime.timedelta(days=i)).replace('-', '') for i in dt_range]
    except Exception as e:
        traceback.print_exc()
        return []

def day(n, today=''):
    '''
    返回today之前的第n天的日期
    today输入格式为'%Y%m%d'，返回格式也为'%Y%m%d'
    day(1,'20210102')='20210101'
    '''
    if today == '':
        return (datetime.date.today() + datetime.timedelta(days=n)).strftime('%Y%m%d')
    today = str(today)
    today = datetime.date(int(today[:4]), int(today[4:6]), int(today[6:]))
    return (today + datetime.timedelta(days=n)).strftime('%Y%m%d')

def dayOfWeek(today):
    '''
    返回today日期属于该周的第几天
    输入格式为'%Y%m%d'
    '''
    today = str(today)
    today = datetime.date(int(today[:4]), int(today[4:6]), int(today[6:]))
    return today.strftime('%w')

def week(n, today=''):
    '''
    返回today日期之后的第n周的所有日期，today输入格式为'%Y%m%d'
    week(0)=['20210308', '20210309', '20210310', '20210311', '20210312', '20210313', '20210314']
    n可以取负数
    '''
    n = n * -1
    if today == '':
        today = datetime.datetime.now()
    else:
        today = str(today)
        today = datetime.date(int(today[:4]), int(today[4:6]), int(today[6:]))
    start_dt = (today - datetime.timedelta(days=today.weekday()+7*n)).strftime('%Y%m%d')
    end_dt = (today - datetime.timedelta(days=today.weekday()+1+(n-1)*7)).strftime('%Y%m%d')
    return date_range(start_dt, end_dt)

def month(n, today=''):
    '''
    返回today日期之后的第n月的所有日期，today输入格式为'%Y%m%d'
    month(0)=['20210201', '20210202'...'20210228']
    n可以取负数
    '''
    n = n * -1
    if today == '':
        today = datetime.datetime.now()
    else:
        today = str(today)
        today = datetime.date(int(today[:4]), int(today[4:6]), int(today[6:]))
    start_dt = datetime.date(today.year, today.month-n,1).strftime('%Y%m%d')
    end_dt = (datetime.date(today.year, today.month-n+1,1)-datetime.timedelta(1)).strftime('%Y%m%d')
    return date_range(start_dt, end_dt)

def delta(start_dt, end_dt):
    '''
    返回两日期之间相差的天数
    输入格式为'%Y%m%d'
    '''
    try:
        start_dt = datetime.datetime.strptime(start_dt, '%Y%m%d')
        end_dt = datetime.datetime.strptime(end_dt, '%Y%m%d')
        return (end_dt - start_dt).days
    except:
        return -1

def formatTime(format, t):
    '''
    将格式为'%Y%m%d'的日期t转换为指定的format，如'%Y-%m-%d'
    '''
    return time.strftime(format, time.strptime(t, '%Y%m%d'))

def parseToTime(date):
    '''
    将格式为'%Y-%m-%d'的日期转换为'%Y%m%d'格式
    '''
    return datetime.datetime.strptime(date, '%Y%m%d').date()

if __name__ == '__main__':
    # print(day(-1, '20190801'))
    # print(month(0, today='20150201'))
    # print(week(0, '20170915').index('20170915') + 1)
    # print(yesterday())
    # print(date_range('20190504', '20190510'))
    # print(formatTime('%Y-%m-%d', '20210101'))
    print(parseToTime('20210101'))
    # print(delta('20210102','20201201'))
