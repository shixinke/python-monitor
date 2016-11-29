#! /usr/bin/python
# encoding:UTF-8

import os


def vmstat():
    fd = os.popen('vmstat')
    tmp = []
    data = {}
    num = 0
    for line in fd.readlines():
        num += 1
        if num == 3:
            tmp = line.split()
    if len(tmp) >= 17:
        #data['process_running'] = tmp[0]
        #data['process_blocked'] = tmp[1]
        data['mem_free'] = tmp[3]
        data['mem_buffered'] = tmp[4]
        data['mem_cached'] = tmp[5]
        data['cpu_user_time'] = tmp[12]
        data['cpu_system_time'] = tmp[13]
        data['cpu_idle_time'] = tmp[14]

    fd.close()
    del tmp
    return data


def memory():
    fd = os.popen('free')
    num = 0
    data = {}
    tmp = []
    for line in fd.readlines():
        num += 1
        if num == 2:
            tmp = line.split()
            data['mem_total'] = tmp[1]
            data['mem_used'] = tmp[2]
            data['mem_shared'] = tmp[4]
            data['mem_available'] = tmp[6]
            data['mem_usage_rate'] = round(float(tmp[2]) / float(tmp[1]), 4)
        elif num == 3:
            tmp = line.split()
            data['swap_total'] = tmp[1]
            #data['swap_used'] = tmp[2]
            data['swap_avail'] = tmp[3]

    del tmp
    del num
    fd.close()
    return data


def uptime():
    fd = open('/proc/uptime')
    data = {}
    tmp = fd.read().split()
    data['system_uptime'] = tmp[0]
    fd.close()
    fd = open('/proc/loadavg')
    tmp = fd.read().split()
    data['load_1'] = tmp[0]
    data['load_5'] = tmp[1]
    data['load_15'] = tmp[2]
    arr = tmp[3].split('/')
    data['process'] = arr[1]
    fd.close()
    return data


def iostat():
    data = {}
    status = {}
    fd = os.popen('iostat -m')
    num = 0
    status['disk_io_reads_total'] = 0
    status['disk_io_writes_total'] = 0
    stats = []
    for line in fd.readlines():
        num += 1
        if num == 1:
            tmp = line.split()
            status['kernel'] = tmp[0] + " " + tmp[1]
            status['hostname'] = tmp[2].strip('(').strip(')')
            status['system_date'] = tmp[3].replace('年', '-').replace('月', '-').replace('日', '')
        elif num >= 7 and line.strip():
            tmp = line.split()
            status['disk_io_reads_total'] += int(tmp[4])
            status['disk_io_writes_total'] += int(tmp[5])
            obj = {}
            obj['fdisk'] = tmp[0]
            obj['disk_io_reads'] = tmp[4]
            obj['disk_io_writes'] = tmp[5]
            stats.append(obj)

    data['status'] = status
    data['stats'] = stats
    return data


def disk():
    fd = os.popen('df')
    data = []
    num = 0
    for line in fd.readlines():
        num += 1
        if num > 1:
            obj = {}
            tmp = line.split()
            obj['mounted'] = tmp[5]
            obj['total_size'] = tmp[1]
            obj['used_size'] = tmp[2]
            obj['avail_size'] = tmp[3]
            obj['used_rate'] = int(tmp[4].replace('%', ''))
            data.append(obj)

    return data


def network():
    data = {}
    status = {'net_in_bytes_total': 0, 'net_out_bytes_total': 0}
    stats = []
    num = 0
    fd = open('/proc/net/dev')
    for line in fd.readlines():
        num += 1
        if num > 3:
            tmp = line.split()
            if tmp[0] != 'lo':
                status['net_in_bytes_total'] += int(tmp[1])
                status['net_out_bytes_total'] += int(tmp[9])
                obj = {}
                obj['if_descr'] = tmp[0].strip(':')
                obj['in_bytes'] = int(tmp[1])
                obj['out_bytes'] = int(tmp[9])
                stats.append(obj)

    data['stats'] = stats
    data['status'] = status
    return data


def collect():
    data = {}
    iostat_data = iostat()
    network_data = network()
    data['status'] = dict(vmstat().items() + memory().items() + uptime().items() + iostat_data['status'].items() + \
                     network_data['status'].items())
    data['io'] = iostat_data['stats']
    data['disk'] = disk()
    data['network'] = network_data['stats']
    return data
