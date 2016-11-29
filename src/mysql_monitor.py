#! /usr/bin/python
# encoding:UTF-8
import MySQLdb
import MySQLdb.cursors
import func
import simplejson as json
import time
import commands
class MySqlMonitor:
    def __init__(self, host, port, user, password, db_name = 'information_schema'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.db = False
        self.error = ''

    def _connect(self):
        try:
            self.db = MySQLdb.connect(host=self.host,user=self.user,passwd=self.password,db=self.db_name,charset="utf8", cursorclass=MySQLdb.cursors.DictCursor)
            return self.db
        except:
            self.error = 'connect database sever failed'
            return False

    def query(self, sql, keepalive = False):
        self._connect()
        if self.db == False:
            return False
        # 使用cursor()方法获取操作游标
        cursor = self.db.cursor()
        # 使用execute方法执行SQL语句
        cursor.execute(sql)
        if keepalive != False:
            self.db.close()
        return cursor

    def row(self, sql):
        cursor = self.query(sql)
        try:
            return cursor.fetchone()
        except:
            return False

    def rows(self, sql):
        cursor = self.query(sql)
        try:
            return cursor.fetchall()
        except:
            return False

    def get_status(self, var = ''):
        if var:
            sql = 'SHOW GLOBAL status WHERE Variable_name = "'+var+'"'
            data = self.row(sql)
            if data:
                if data['Value'] == 'ON':
                    data['Value'] = 1
                elif data['Value'] == 'OFF':
                    data['Value'] = 0
                return data['Value']
            else:
                return ''
        else:
            sql = 'SHOW GLOBAL status'
            data = self.rows(sql)
            datalist = {}
            if data:
                for v in data:
                    if v['Value'] == 'ON':
                        v['Value'] = 1
                    elif v['Value'] == 'OFF':
                        v['Value'] = 0
                    var_name = v['Variable_name'].lower()
                    datalist[var_name] = v['Value']
            return datalist

    def get_vars(self, var = ''):
        if var:
            sql = 'SHOW variables WHERE Variable_name = "'+var+'"'
            data = self.row(sql)
            if data:
                if data['Value'] == 'ON':
                    data['Value'] = 1
                elif data['Value'] == 'OFF':
                    data['Value'] = 0
                return data['Value']
            else:
                return ''
        else:
            sql = 'SHOW variables'
            data = self.rows(sql)
            datalist = {}
            if data:
                for v in data:
                    if v['Value'] == 'ON':
                        v['Value'] = 1
                    elif v['Value'] == 'OFF':
                        v['Value'] = 0
                    var_name = v['Variable_name'].lower()
                    datalist[var_name] = v['Value']
            return datalist

    def get_processlist(self):
        sql = 'select * from information_schema.processlist where DB !="information_schema" and command !="Sleep"'
        data = self.rows(sql)
        datalist = []
        if data:
            for row in data:
                tmp = {}
                tmp['pid'] = row['ID']
                tmp['p_user'] = row['USER']
                tmp['p_host'] = row['HOST']
                tmp['p_db'] = row['DB']
                tmp['command'] = row['COMMAND']
                tmp['time'] = row['TIME']
                tmp['status'] = row['STATE']
                tmp['info'] = row['INFO']
                datalist.append(tmp)

        return datalist

    def get_connected(self, processlist):
        clients = []
        for value in processlist:
            if value['db'] and value['db'] != 'information_schema' and value['db'] != 'performance_schema':
                clients.append(value)

        return clients

    def get_role(self):
        roles = {'role':'alone', 'master_status':{}, 'slave_status':{}}
        master_status = self.rows('show master status')
        slave_status = self.rows('show slave status')
        if master_status and len(master_status) > 0 :
            roles['role'] = 'master'
            roles['master_status'] = master_status
        if slave_status  and len(slave_status):
            if roles['role'] == 'alone':
                roles['role'] = 'slave'
            else:
                roles['role'] = ['master', 'slave']
            roles['slave_status'] = slave_status
        return roles

    def get_bigtable(self, max_size):
        sql = 'SELECT table_schema as `db_name`,table_name as `table_name`,CONCAT(ROUND(( data_length + index_length ) / ( 1024 * 1024 ), 2), "") as `table_size` , table_comment  FROM information_schema.TABLES ORDER BY data_length + index_length DESC '
        data = self.rows(sql)
        bigtable = []
        max_size = float(max_size)
        if data:
            for row in data:
                if float(row['table_size']) >= max_size:
                    bigtable.append(row)
        return bigtable

    def get_replication(self, vars, role, processlist):
        master_threads = 0
        info = {}
        if role['role'] == 'alone':
            return info
        if len(processlist) >0 :
            for value in processlist:
                if value['command'] == 'Binlog Dump' or value['command'] == 'Binlog Dump GTID':
                    master_threads += 1
        info['is_slave'] = 0
        if role['role'] == 'slave' or ('slave' in ['master', 'slave'] == True):
            info['is_slave'] = 1
        info['is_master'] = 0
        if master_threads>= 1:
            info['is_master'] = 1
        info['gtid_mode'] = vars['gtid_mode'];
        info['read_only'] = vars['read_only'];
        info['master_server'] = '';
        info['master_port'] = '';
        info['slave_io_run'] = '';
        info['slave_sql_run'] = '';
        info['delay'] = 0;
        info['current_binlog_file'] = '';
        info['current_binlog_pos'] = '';
        info['master_binlog_file'] = '';
        info['master_binlog_pos'] = '';
        info['master_binlog_space'] = 0;

        if info['is_slave'] == 1 and len(role['slave_status']) > 0:
            info['master_server'] = role['slave_status']['master_host']
            info['master_port'] = role['slave_status']['master_port']
            info['slave_io_run'] = role['slave_status']['slave_io_running']
            info['slave_sql_run'] = role['slave_status']['slave_sql_running']
            info['delay'] = role['slave_status']['seconds_behind_master']
            info['current_binlog_file'] = role['slave_status']['relay_master_log_file']
            info['current_binlog_pos'] = role['slave_status']['exec_master_log_pos']
            info['master_binlog_file'] = role['slave_status']['master_log_file']
            info['master_binlog_pos'] = role['slave_status']['read_master_log_pos']
        elif master_threads >=1:
            info['master_binlog_file'] = role['master_status']['file']
            info['master_binlog_pos'] = role['master_status']['position']
            logs = self.query('show master logs')
            if logs and len(logs) > 0:
                for log in logs:
                    info['master_binlog_space'] += int(log)
        return info

    def get_slow_log(self):
        sql = '';

    def close(self):
        if self.db:
            self.db.close()

    def __del__(self):
        self.close()

def slow_log(log_file):
    bin_path = func.get_local_config('db', 'pt-query-digest_bin')
    (status, data) = commands.getstatusoutput(bin_path+' --output=json-an --limit=100% '+log_file)
    datalist = {}
    review = []
    history = []
    if data:
        data = json.loads(data)
        if data['classes']:
            classes = data['classes']
            for v in classes:
                obj = {}
                tmp = {}
                v['checksum'] = v['checksum']
                obj['checksum'] = v['checksum']
                tmp['checksum'] = v['checksum']
                obj['fingerprint'] = v['fingerprint']
                obj['sample'] = v['example']['query']
                tmp['sample'] = v['example']['query']
                obj['first_seen'] = v['ts_min']
                obj['last_seen'] = v['ts_max']
                tmp['ts_min'] = v['ts_min']
                tmp['ts_max'] = v['ts_max']
                #tmp['hostname_max'] = v['metrics']['host']['value']
                tmp['db_max'] = v['metrics']['db']['value']
                tmp['ts_cnt'] = v['query_count']
                tmp['Query_time_sum'] = v['metrics']['Query_time']['sum']
                tmp['Query_time_min'] = v['metrics']['Query_time']['min']
                tmp['Query_time_max'] = v['metrics']['Query_time']['max']
                tmp['Query_time_pct_95'] = v['metrics']['Query_time']['pct_95']
                tmp['Query_time_stddev'] = v['metrics']['Query_time']['stddev']
                tmp['Query_time_median'] = v['metrics']['Query_time']['median']
                tmp['Lock_time_sum'] = v['metrics']['Lock_time']['sum']
                tmp['Lock_time_min'] = v['metrics']['Lock_time']['min']
                tmp['Lock_time_max'] = v['metrics']['Lock_time']['max']
                tmp['Lock_time_pct_95'] = v['metrics']['Lock_time']['pct_95']
                tmp['Lock_time_stddev'] = v['metrics']['Lock_time']['stddev']
                tmp['Lock_time_median'] = v['metrics']['Lock_time']['median']
                tmp['Rows_sent_sum'] = v['metrics']['Rows_sent']['sum']
                tmp['Rows_sent_min'] = v['metrics']['Rows_sent']['min']
                tmp['Rows_sent_max'] = v['metrics']['Rows_sent']['max']
                tmp['Rows_sent_pct_95'] = v['metrics']['Rows_sent']['pct_95']
                tmp['Rows_sent_stddev'] = v['metrics']['Rows_sent']['stddev']
                tmp['Rows_sent_median'] = v['metrics']['Rows_sent']['median']
                tmp['Rows_examined_sum'] = v['metrics']['Rows_examined']['sum']
                tmp['Rows_examined_min'] = v['metrics']['Rows_examined']['min']
                tmp['Rows_examined_max'] = v['metrics']['Rows_examined']['max']
                tmp['Rows_examined_pct_95'] = v['metrics']['Rows_examined']['pct_95']
                tmp['Rows_examined_stddev'] = v['metrics']['Rows_examined']['stddev']
                tmp['Rows_examined_median'] = v['metrics']['Rows_examined']['median']
                review.append(obj)
                history.append(tmp)
    datalist['review'] = review
    datalist['history'] = history
    return datalist


def get_from_vars_and_status(mysql_vars, status):
    data = {}
    keys = ['innodb_version', 'innodb_buffer_pool_instances', 'innodb_buffer_pool_size', 'innodb_doublewrite', 'innodb_file_per_table',
        'innodb_flush_log_at_trx_commit',
        'innodb_flush_method',
        'innodb_force_recovery',
        'innodb_io_capacity',
        'innodb_read_io_threads',
        'innodb_write_io_threads',
        'innodb_buffer_pool_pages_total',
        'innodb_buffer_pool_pages_data',
        'innodb_buffer_pool_pages_dirty',
        'innodb_buffer_pool_pages_flushed',
        'innodb_buffer_pool_pages_free',
        'innodb_buffer_pool_pages_misc',
        'innodb_page_size',
        'innodb_pages_created',
        'innodb_pages_read',
        'innodb_pages_written',
        'innodb_row_lock_current_waits',
    ]
    for k in keys:
        if k in mysql_vars:
            data[k] = mysql_vars[k]
        elif k in status:
            data[k] = status[k]

    return data




def mysql_status_info(mysql_vars, status, next_status, role):
    data = get_from_vars_and_status(mysql_vars, status)
    if len(mysql_vars) > 0:
        data['connect'] = 1
        if (role['role'] in ['master', 'slave', 'alone']):
            data['role'] = role['role']
        else:
            data['role'] = 'master,slave'
        data['port'] = mysql_vars['port']
        data['uptime'] = next_status['uptime']
        data['version'] = mysql_vars['version']
        data['max_connections'] = mysql_vars['max_connections']
        data['max_connect_errors'] = mysql_vars['max_connect_errors']
        data['open_files_limit'] = mysql_vars['open_files_limit']
        data['open_files'] = next_status['open_files']
        data['table_open_cache'] = mysql_vars['table_open_cache']
        data['open_tables'] = next_status['open_tables']
        data['max_tmp_tables'] = mysql_vars['max_tmp_tables']
        data['max_heap_table_size'] = mysql_vars['max_heap_table_size']
        data['max_allowed_packet'] = mysql_vars['max_allowed_packet']
        data['threads_connected'] = next_status['threads_connected']
        data['threads_running'] = next_status['threads_running']
        #data['threads_waits'] = next_status['threads_waits']
        data['threads_created'] = next_status['threads_created']
        data['threads_cached'] = next_status['threads_cached']
        data['connections'] = next_status['connections']
        data['aborted_clients'] = next_status['aborted_clients']
        data['aborted_connects'] = next_status['aborted_connects']
        data['key_buffer_size'] = mysql_vars['key_buffer_size']
        data['sort_buffer_size'] = mysql_vars['sort_buffer_size']
        data['join_buffer_size'] = mysql_vars['join_buffer_size']
        data['key_blocks_not_flushed'] = next_status['key_blocks_not_flushed']
        data['key_blocks_unused'] = next_status['key_blocks_unused']
        data['key_blocks_used'] = next_status['key_blocks_used']

        data['connections_persecond'] = int(next_status['connections']) - int(status['connections'])
        data['bytes_received_persecond'] = (int(next_status['bytes_received']) - int(next_status['bytes_received'])) / 1024
        data['bytes_sent_persecond'] = (int(next_status['bytes_sent']) - int(status['bytes_sent'])) / 1024
        data['com_select_persecond'] = int(next_status['com_select']) - int(status['com_select'])
        data['com_insert_persecond'] = int(next_status['com_insert']) - int(status['com_insert'])
        data['com_update_persecond'] = int(next_status['com_update']) - int(status['com_update'])
        data['com_delete_persecond'] = int(next_status['com_delete']) - int(status['com_delete'])
        data['com_commit_persecond'] = int(next_status['com_commit']) - int(status['com_commit'])
        data['com_rollback_persecond'] = int(next_status['com_rollback']) - int(status['com_rollback'])
        data['questions_persecond'] = int(next_status['questions']) - int(status['questions'])
        data['queries_persecond'] = int(next_status['queries']) - int(status['queries'])
        data['transaction_persecond'] = int(next_status['com_commit']) + int(next_status['com_rollback']) - (int(status['com_commit']) + int(status['com_rollback']))
        data['created_tmp_disk_tables_persecond'] = int(next_status['created_tmp_disk_tables']) - int(status['created_tmp_disk_tables'])
        data['created_tmp_files_persecond'] = int(next_status['created_tmp_files']) - int(status['created_tmp_files'])
        data['created_tmp_tables_persecond'] = int(next_status['created_tmp_tables']) - int(status['created_tmp_tables'])
        data['table_locks_immediate_persecond'] = int(next_status['table_locks_immediate']) - int(status['table_locks_immediate'])
        data['table_locks_waited_persecond'] = int(next_status['table_locks_waited']) - int(status['table_locks_waited'])
        data['key_read_requests_persecond'] = int(next_status['key_read_requests']) - int(status['key_read_requests'])
        data['key_reads_persecond'] = int(next_status['key_reads']) - int(status['key_reads'])
        data['key_write_requests_persecond'] = int(next_status['key_write_requests']) - int(status['key_write_requests'])
        data['key_writes_persecond'] = int(next_status['key_writes']) - int(status['key_writes'])
        data['innodb_buffer_pool_read_requests_persecond'] = int(next_status['innodb_buffer_pool_read_requests']) - int(status['innodb_buffer_pool_read_requests'])
        data['innodb_buffer_pool_reads_persecond'] = int(next_status['innodb_buffer_pool_reads']) - int(status['innodb_buffer_pool_reads'])
        data['innodb_buffer_pool_write_requests_persecond'] = int(next_status['innodb_buffer_pool_write_requests']) - int(status['innodb_buffer_pool_write_requests'])
        data['innodb_buffer_pool_pages_flushed_persecond'] = int(next_status['innodb_buffer_pool_pages_flushed']) - int(status['innodb_buffer_pool_pages_flushed'])
        data['innodb_rows_deleted_persecond'] = int(next_status['innodb_rows_deleted']) - int(status['innodb_rows_deleted'])
        data['innodb_rows_inserted_persecond'] = int(next_status['innodb_rows_inserted']) - int(status['innodb_rows_inserted'])
        data['innodb_rows_read_persecond'] = int(next_status['innodb_rows_read']) - int(status['innodb_rows_read'])
        data['innodb_rows_updated_persecond'] = int(next_status['innodb_rows_updated']) - int(status['innodb_rows_updated'])
        data['query_cache_hitrate'] = 0
        if int(status['qcache_hits']) + int(status['com_select']) != 0:
            data['query_cache_hitrate'] = '%9.2f' %(int(status['qcache_hits']) / (int(status['qcache_hits']) + int(status['com_select'])))

        data['thread_cache_hitrate'] = 0
        if int(status['connections']) != 0:
            data['thread_cache_hitrate'] = '%9.2f' %(1 - int(status['threads_created']) / int(status['connections']))

        data['key_buffer_read_rate'] = 0
        if int(status['key_read_requests']) != 0:
            data['key_buffer_read_rate'] = '%9.2f' %(1 - int(status['key_reads']) / int(status['key_read_requests']))

        data['key_buffer_write_rate'] = 0
        if int(status['key_write_requests']) != 0:
            data['key_buffer_write_rate'] = '%9.2f' %(1 - int(status['key_writes']) / int(status['key_write_requests']))

        data['key_blocks_used_rate'] = 0
        if int(status['key_blocks_used'])+int(status['key_blocks_unused']) != 0:
            data['key_blocks_used_rate'] = '%9.2f' %(int(status['key_blocks_used']) / (int(status['key_blocks_used'])+ int(status['key_blocks_unused'])))


        data['created_tmp_disk_tables_rate'] = 0
        if int(status['created_tmp_disk_tables'])+int(status['created_tmp_tables']) != 0:
            data['created_tmp_disk_tables_rate'] = '%9.2f' %(int(status['created_tmp_disk_tables']) / (int(status['created_tmp_disk_tables'])+int(status['created_tmp_tables'])))

        data['connections_usage_rate'] = 0
        if int(mysql_vars['max_connections']) != 0 :
            data['connections_usage_rate'] = '%9.2f' %(int(status['threads_connected']) / int(mysql_vars['max_connections']))

        data['open_files_usage_rate'] = 0
        if int(mysql_vars['open_files_limit']) != 0:
            data['open_files_usage_rate'] = '%9.2f' %(int(status['open_files']) / int(mysql_vars['open_files_limit']))


        data['open_tables_usage_rate'] = 0
        if int(mysql_vars['table_open_cache']) != 0:
            data['open_tables_usage_rate'] = '%9.2f' %(int(status['open_tables']) / int(mysql_vars['table_open_cache']))
    else:
        data['connect'] = 0

    return data


def collect():
    reports = {}
    opts = func.get_local_config('db', 'db_host,db_port,db_user,db_pass')
    mon = MySqlMonitor(opts['db_host'], opts['db_port'], opts['db_user'], opts['db_pass'])
    mysql_vars = mon.get_vars()
    mysql_status = mon.get_status()
    max_size = func.get_api_config('table_max_size')
    if max_size:
        max_size = float(max_size)
    else:
        max_size = float(func.get_local_config('db', 'table_max_size'))
    processlist = mon.get_processlist()
    role = mon.get_role()
    connected = mon.get_connected(processlist)
    replication = mon.get_replication(mysql_vars, role, processlist)
    bigtable = mon.get_bigtable(max_size)
    slowlog = slow_log(mysql_vars['slow_query_log_file'])


    time.sleep(1)
    mysql_next_status = mon.get_status()
    reports['status'] = mysql_status_info(mysql_vars, mysql_status, mysql_next_status, role)
    reports['bigtable'] = bigtable
    reports['connected'] = connected
    reports['processlist'] = processlist
    reports['replication'] = replication
    reports['slowlog_review'] = slowlog['review']
    reports['slowlog_history'] = slowlog['history']

    return reports



