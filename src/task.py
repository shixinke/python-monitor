#! /usr/bin/python
# encoding:UTF-8

import sched,time
import mysql_monitor, os_monitor, func

def task():
    ip = func.get_local_ip()
    os_data = os_monitor.collect()
    os_data['status']['ip'] = ip
    mysql_data = mysql_monitor.collect()
    mysql_data['status']['host'] = ip
    func.report_os(os_data)
    func.report_mysql(mysql_data)

def main():
    while True:
        task()
        time.sleep(60)



if __name__ == '__main__':
    main()
    pass



