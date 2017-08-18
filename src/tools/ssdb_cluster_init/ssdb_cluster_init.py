#!/usr/bin/python
#encoding=utf-8
################################################################################
#
# Copyright (c) 2017 TAIHE. All Rights Reserved
#
################################################################################
"""
This module provide ssdb cluster initialize function.
Authors: zhangpeng(zhangpeng@taihe.com)
Date:    2017/06/28
"""

import urllib, os, socket, sys
import urllib2
import json
import cookielib
import httplib
import ssdb
import pdb
import zookeeper
import pdb
from optparse import OptionParser 

g_node_format = """{"ip":"{sg.ip}", "port":{sg.port}, "slave_ip":"{sg.slave_ip}", "slave_port":{sg.slave_port}}"""

def handle_output(result, message = ""):
    result_str = "failure"
    if result:
        result_str = "success"
    print "{\"action\":\"%s\", \"message\":\"%s\"}"%(result_str, message)

class ssdb_group:
    def __init__(self):
        self.ip = ""
        self.port = 0
        self.slave_ip = ""
        self.slave_port = 0

class ssdb_hook:
    def __init__(self):
        self.ip = ""
        self.port = 0
        self.url = ""

def set_slave(master_ip, master_port, slave_ip, slave_port):
    try:
        c = ssdb.Client(slave_ip, slave_port)
        c.change_master_to(master_ip, master_port, 0, "")
        c.start_slave()
        c.close()
    except ssdb.SSDBException,e:
        err_str =  "set slave ssdb exception %s"%str(e)
        handle_output(False, err_str)
    except socket.error, e:
        err_str = "set slave ssdb %s:%d connected error:%s, "%(ip, port, str(e))
        handle_output(False, err_str)

def init_ssdb(ip, port):
    try:
        c = ssdb.Client(ip, port)
        for i in range(0, 16384):
            #pdb.set_trace()
            c.set_slot(i)
        c.close()
    except ssdb.SSDBException,e:
        err_str =  "ssdb exception %s"%str(e)
        handle_output(False, err_str)
    except socket.error, e:
        err_str = "ssdb %s:%d connected error:%s, "%(ip, port, str(e))
        handle_output(False, err_str)

def init_slot_map(zk, node_size):
    try:
        size_per_node = 16384 / node_size
        node_max = size_per_node * node_size
        for i in range(node_max):
            slot_id = i / size_per_node
            path = "/slot_map/" + str(i)
            data = """{"node_index":%d, "migrating":"false"}"""%(slot_id)
            create_zookeeper(zk, path, data)
        
        for i in range(node_max, 16384):
            slot_id = i / size_per_node - 1
            path = "/slot_map/" + str(i)
            data = """{"node_index":%d, "migrating":"false"}"""%(slot_id)
            create_zookeeper(zk, path, data)
    except zookeeper.NodeExistsException, e:
            err_str = "path %s exist\n"%path
            handle_output(False, err_str)

def delete_zookeeper(zk, path):
    try:
        zk_children = zookeeper.get_children(zk, path)
        if len(zk_children) > 0:
            for tp in zk_children:
                tp = path + "/" + tp
                delete_zookeeper(zk, tp)
        zookeeper.delete(zk, path)
    except zookeeper.NodeExistsException, e:
        err_str = "path %s exist\n"%path
        handle_output(False, err_str)
    except zookeeper.NotEmptyException, e:
        err_str = "node not empty: %s \n"%path
        handle_output(False, err_str)
    except zookeeper.OperationTimeoutException, e:
        err_str = "OperationTimeoutException:%s, %s"%(ip_port_str, str(e))
        handle_output(False, err_str)

def create_zookeeper(zk, path, data):
    try:
        if zookeeper.exists(zk, path):
            delete_zookeeper(zk, path)
        zookeeper.create(zk,path,data,[{"perms":15,"scheme":"world","id":"anyone"}],0)
    except zookeeper.NodeExistsException, e:
        err_str = "path %s exist\n"%path
        handle_output(False, err_str)
    except zookeeper.OperationTimeoutException, e:
        err_str = "OperationTimeoutException:%s, %s"%(ip_port_str, str(e))
        handle_output(False, err_str)

def parse_ssdb_group(json_path):
    #pdb.set_trace()
    parsed = True
    ssdb_groups = []
    f = open(json_path, 'rw')
    confs = json.load(f)
    f.close()
    try:
        ssdb_group_json = confs["ssdb_group"]
    except KeyError,e:
        error_str = "KeyError %s"%str(e)
        handle_output(False, err_str)
        return False, ssdb_groups, ""

    for s in ssdb_group_json:
        try:
            sg = ssdb_group()
            sg.ip         = s["ip"]
            sg.port       = s["port"]
            sg.slave_ip   = s["slave_ip"]
            sg.slave_port = s["slave_port"]
            ssdb_groups.append(sg)
        except KeyError,e: 
            err_str = "no key messages in json %s" % json.dump(s)
            handle_output(False, err_str)
            parsed = False
            break
        
    return parsed, ssdb_groups

def parse_zookeeper(json_path):
    #pdb.set_trace()
    parsed = True
    ssdb_groups = []
    f = open(json_path, 'rw')
    confs = json.load(f)
    f.close()
    try:
        zookeeper_str   = confs["zookeeper"]
    except KeyError,e:
        err_str = "KeyError %s"%str(e)
        handle_output(False, err_str)
        return False, ssdb_groups, ""
        
    return parsed, zookeeper_str

def get_next_node_index(zk):
    try:
        zk_children = zookeeper.get_children(zk, "/nodes")
        max_index = 0
        ssdb_groups = []
        for tp in zk_children:
            index = int(tp)
            path_str = "/nodes/" + tp
            data = zookeeper.get(zk, path_str)
            if len(data) > 0:
                c = json.loads(data[0])
                sg = ssdb_group()
                sg.ip         = c["ip"]
                sg.port       = c["port"]
                sg.slave_ip   = c["slave_ip"]
                sg.slave_port = c["slave_port"]
                ssdb_groups.append(sg)
            if index > max_index:
                max_index = index
        return max_index +1, ssdb_groups
    except zookeeper.NodeExistsException, e:
        err_str = "path %s exist\n"%pathstr
        handle_output(False, err_str)
    except zookeeper.OperationTimeoutException, e:
        err_str = "OperationTimeoutException:%s, %s"%(ip_port_str, str(e))
        handle_output(False, err_str)
    return -1

def add_node(path_str, zookeeper_str):
    zk = zookeeper.init(zookeeper_str)
    ret, ssdb_groups = parse_ssdb_group(path_str)
    if ret == False:
        handle_output(False, "parse json error")
        return False
    zk = zookeeper.init(zookeeper_str)
    node_index, old_ssdb_groups = get_next_node_index(zk)
    if node_index < 0:
        handle_output(False, "get node index error")
        return False
    for s in ssdb_groups:
        if not check_repeat_node(old_ssdb_groups, s):
            err_str = "repeat node error node:%s, %d" % (s.ip, s.port)
            handle_output(False, err_str)
            return False
        if not check_network(s.ip, s.port):
            err_str = "failed to connected %s:%d"%(s.ip, s.port)
            handle_output(False, err_str)
            return False
        if not check_network(s.slave_ip, s.slave_port):
            err_str = "failed to connected %s:%d"%(s.slave_ip, s.slave_port)
            handle_output(False, err_str)
            return False
    for s in ssdb_groups:
        data = "{" + """\"status\":0, \"ip\":\"{sg.ip}\", \"port\":{sg.port}, \"slave_ip\":\"{sg.slave_ip}\", \"slave_port\":{sg.slave_port}""".format(sg = s) + "}"
        init_ssdb(s.ip, s.port)
        #init_ssdb(s.slave_ip, s.slave_port)
        set_slave(s.ip, s.port, s.slave_ip, s.slave_port)
        path = "/nodes/" + str(node_index)
        create_zookeeper(zk, path, data)
        node_index = node_index + 1
    return True

def add_twemproxy(host, port, zookeeper_str):
    try:
        zk = zookeeper.init(zookeeper_str)
        if not zookeeper.exists(zk, "/twemproxy"):
            create_zookeeper(zk, "/twemproxy", "")
        zk_twproxy_path = "/twemproxy/%s:%d"%(host, port)
        if not zookeeper.exists(zk, zk_twproxy_path):
            create_zookeeper(zk, zk_twproxy_path, "")
            return True
        else:
            return False
    except zookeeper.NodeExistsException, e:
        handle_output(False, "path %s exist\n"%pathstr)
    except zookeeper.OperationTimeoutException, e:
        handle_output(False, "OperationTimeoutException:%s, %s"%(ip_port_str, str(e)))
    return False

def init_all_node(path_str, zookeeper_str):
    ret, ssdb_groups = parse_ssdb_group(path_str)
    if ret == False:
        print "parse json error"
        return

    zk = zookeeper.init(zookeeper_str)
    create_zookeeper(zk, "/nodes", "1")
    create_zookeeper(zk, "/slot_map", "1")

    node_index = 0
    node_size = len(ssdb_groups)
    for s in ssdb_groups:
        data = "{" + """\"status\":0, \"ip\":\"{sg.ip}\", \"port\":{sg.port}, \"slave_ip\":\"{sg.slave_ip}\", \"slave_port\":{sg.slave_port}""".format(sg = s) + "}"
        set_slave(s.ip, s.port, s.slave_ip, s.slave_port)
        init_ssdb(s.ip, s.port)
        #init_ssdb(s.slave_ip, s.slave_port)
        path = "/nodes/" + str(node_index)
        create_zookeeper(zk, path, data)
        node_index = node_index + 1
    init_slot_map(zk, node_size)

def create_slotmap_file(pathstr, zookeeper_str):
    try:
        write_tmp_file_path = pathstr + "/" + "slotmap.tmp"
        write_file_path     = pathstr + "/" + "slotmap"
        wft = os.open(write_tmp_file_path, os.O_RDWR|os.O_CREAT|os.O_TRUNC)
        zk = zookeeper.init(zookeeper_str)
        for i in range(16384):
            slotmap_path = "/slot_map/%d"%i
            data = zookeeper.get(zk, slotmap_path)
            if(len(data) > 0):
                c = json.loads(data[0])
                c["num"] = i
                str = json.dumps(c)
                str += "\n"
                os.write(wft, str)
        os.close(wft)

        os.system("rm -fr %s"%write_file_path)
        os.system("mv %s %s"%(write_tmp_file_path, write_file_path))

    except zookeeper.NodeExistsException, e:
        print "path %s exist\n"%pathstr
    except zookeeper.OperationTimeoutException, e:
        print "OperationTimeoutException:%s, %s"%(ip_port_str, str(e))

    return

def check_repeat_node(ssdb_groups, sg):
    for t in ssdb_groups:
        if (t.ip == sg.ip and t.port == sg.port) or (t.ip == sg.slave_ip and t.port == sg.slave_port):
            return False
    return True

def check_network(add, port):
    try:
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.connect((add, port))
        return True
    except socket.error, e:
        return False
    finally:
        sock.close()

def get_http_response(ip, port, url, timeout):
    result = "unknown"
    try:
        httpClient = httplib.HTTPConnection(ip, port, timeout)
        httpClient.request('GET', url)
        response = httpClient.getresponse()
        if 200 == response.status:
            mem_info_str = response.read()
            c = json.loads(mem_info_str)
            result = c["mem_info"]
            if len(mem_info_str) > 0:
                c = json.loads(mem_info_str)
                result = c["mem_info"]
    except socket.error, e:
        #print "socket error:%s"%str(e)
        sys.stderr.write("socket error:%s"%str(e))
    except httplib.error, e:
        #print "HTTP connection failed: %s" % e
        sys.stderr.write("HTTP connection failed: %s" % str(e))
    finally:
        httpClient.close()
    return result

def get_ssdb_mem_info(zookeeper_str):
    path = "/nodes"
    mem_info = "unknown"
    slave_mem_info = "unknown"
    ip = "unknown"
    slave_ip = "unknown"
    data_list = []
    try:
        zk = zookeeper.init(zookeeper_str)
        zk_children = zookeeper.get_children(zk, path)
        if len(zk_children) > 0:
            for tp in zk_children:
                tp = path + "/" + tp
                mem_info = "unknown"
                slave_mem_info = "unknown"
                ip = "unknown"
                slave_ip = "unknown"
                data = zookeeper.get(zk, tp)
                if(len(data) > 0):
                    s = json.loads(data[0])
                    ip         = s["ip"]
                    port       = s["port"]
                    url        = "/ssdb/mem_info?port=%s"%(port)
                    slave_ip   = s["slave_ip"]
                    slave_port = s["slave_port"]
                    url_slave  = "/ssdb/mem_info?port=%s"%(slave_port)
                    mem_info   = get_http_response(ip, 33333,  url, 5)
                    slave_mem_info = get_http_response(slave_ip, 33333, url_slave, 5)
                    cd  = {}
                    cd["ip"]             = ip
                    cd["port"]           = port
                    cd["mem_info"]       = mem_info
                    cd["slave_ip"]       = slave_ip
                    cd["slave_port"]     = slave_port
                    cd["slave_mem_info"] = slave_mem_info
                    data_list.append(cd)
        json_str = json.dumps(data_list)
        print json_str
        return json_str
    except zookeeper.NodeExistsException, e:
        print "path %s exist\n"%path
    except zookeeper.NotEmptyException, e:
        print "node not empty: %s \n"%path
    except zookeeper.OperationTimeoutException, e:
        print "OperationTimeoutException:%s, %s"%(ip_port_str, str(e))

def main():
    parser = OptionParser(usage="%prog [options]")
    parser.add_option("-i","--initialize",action="store",type="string",dest="init_file_path",help="init all ssdb in file")
    parser.add_option("-a","--add",action="store",type="string",dest="add_node_file_path",help="add_node in file")
    parser.add_option("-c","--create_node_file",action="store",type="string",dest="create_node_file_path",help="create node file path")
    parser.add_option("-z","--zookeeper_str",action="store",type="string",dest="zookeeper_str",help="zookeeper ip:port")
    parser.add_option("-m","--model_str",action="store",type="string",dest="model_str",help="model just like ssdb_meminfo, add_twemproxy")
    parser.add_option("-p","--port",action="store",type="int",dest="port",help="port")
    parser.add_option("-t","--twemproxy_host",action="store",type="string",dest="twemproxy_host",help="twemproxy_host")
    (options, args) = parser.parse_args()
    #pdb.set_trace()
    if options.init_file_path and options.zookeeper_str: 
        init_file_path = options.init_file_path
        zookeeper_str  = options.zookeeper_str
        if not os.path.exists(options.init_file_path):
            error_str = "Initialize path not exist:%s, zookeeper:%s"%(init_file_path, zookeeper_str)
            handle_output(False, error_str)
            return
        init_all_node(init_file_path, zookeeper_str)
    elif options.add_node_file_path and options.zookeeper_str: 
        add_node_file_path = options.add_node_file_path
        zookeeper_str  = options.zookeeper_str
        if not os.path.exists(add_node_file_path):
            err_str = "Add node path file not exist:%s, zookeeper:%s"%(add_node_file_path, zookeeper_str)
            handle_output(False, err_str)
            return
        if add_node(add_node_file_path, zookeeper_str):
            handle_output(True)
    elif options.create_node_file_path and options.zookeeper_str:
        create_node_file_path = options.create_node_file_path
        zookeeper_str         = options.zookeeper_str
        print "create %s, zookeeper %s"%(options.create_node_file_path, zookeeper_str)
        if not os.path.isdir(create_node_file_path):
            print "No such file or dictionary :%s"%(create_node_file_path)
            return
        create_slotmap_file(create_node_file_path, zookeeper_str)
    elif options.model_str and options.zookeeper_str:
        model_str             = options.model_str
        zookeeper_str         = options.zookeeper_str
        if model_str == "ssdb_mem_info":
            get_ssdb_mem_info(zookeeper_str)
        if model_str == "add_twemproxy":
            if options.twemproxy_host and options.port:
                if not add_twemproxy(options.twemproxy_host, options.port, zookeeper_str):
                    err_str = "twemproxy node exist:%s:%d, zookeeper:%s"%(options.twemproxy_host, options.port, zookeeper_str)
                    handle_output(False, err_str)
                else:
                    handle_output(True)
                    return
    else:
        print "unknown option"

if __name__ == "__main__":  
    main()
