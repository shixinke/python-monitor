#! /usr/bin/python
# encoding:UTF-8
import urllib
import urllib2
import ConfigParser
import socket
import simplejson as json

cf = ConfigParser.ConfigParser()
cf.read('monitor.conf')


def curl_post(url, post={}, headers={}):
    result = {}
    data = urllib.urlencode(post)
    req = urllib2.Request(url, data, headers)
    try:
        response = urllib2.urlopen(req)
        if response.code == 200:
            result = json.loads(response.read())
    except urllib2.URLError, e:
        if hasattr(e, "reason"):
            print "Failed to reach the server"
            print "The reason:", e.reason
        elif hasattr(e, "code"):
            print "The server couldn't fulfill the request"
            print "Error code:", e.code
            print "Return content:", e.read()
    else:
        pass  # 其他异常的处理

    return result


def get_api_config(keys=''):
    url = get_local_config('api', 'config_url')
    data = curl_post(url, {'keys': keys})
    if data and data['data']:
        return data['data']
    else:
        return False

def report_mysql(status):
    url = get_local_config('api', 'mysql_report_url')
    for k in status:
        status[k] = json.dumps(status[k])
        pass
    data = curl_post(url, status)
    if data and data['code'] and data['code'] == 200:
        return True
    else:
        return False


def report_os(status):
    url = get_local_config('api', 'os_report_url')
    for k in status:
        status[k] = json.dumps(status[k])
    data = curl_post(url, status)
    if data and data['code'] and data['code'] == 200:
        return True
    else:
        return False


def get_local_config(sec, item):
    items = item.split(',')
    data = {}
    for i in items:
        data[i] = cf.get(sec, i)
    if len(data) == 1:
        return data[item]
    else:
        return data

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 0))
        ip = s.getsockname()[0]
    except:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

def get_item(vars, var):
    try:
       item_value = vars[var]
       return item_value
    except:
       return ''



