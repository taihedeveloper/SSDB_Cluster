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
Date:    2017/07/08
"""

import urllib, os, socket
import urllib2
import json
import cookielib
import httplib
import ssdb
import pdb
import commands
import SimpleHTTPServer
import SocketServer
from optparse import OptionParser 

from BaseHTTPServer import BaseHTTPRequestHandler
import urlparse

class HookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse.urlparse(self.path)
        rs = urlparse.urlparse(self.path)
        q = urlparse.parse_qs(rs.query)
        real_path   = parsed_path.path
        if real_path == "/":
            self.send_error(404,"file not found");
            return
        if real_path == "/ssdb/mem_info":
            mem_info = "unkonwn"
            if q["port"]:
                port = q["port"]
                status, output = commands.getstatusoutput("""netstat -nalp 2>/dev/null|grep LISTEN|grep %s|awk '{print $7}'"""%(port[0]))
                if not status:
                    output.strip(output)
                    if '-' != output and len(output) > 0 :
                        print output, len(output)
                        process_id = int(output.split('/')[0])
                        status, output = commands.getstatusoutput("""cat /proc/%s/status |grep VmRSS|awk  '{print $2$3}'"""%(process_id))
                        mem_info = output
                        print status, output
            message = """{"mem_info":"%s"}"""%(mem_info)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(message)
        else:
            self.send_response(404)
            message = "url not found"
            self.end_headers()
            self.wfile.write(message)
        return

if __name__ == '__main__':
    parser = OptionParser(usage="%prog [options]")
    parser.add_option("-p","--port",action="store",type="string",dest="port",help="http server listen port")
    (options, args) = parser.parse_args()
    if options.port:
        server_port = int(options.port)
        from BaseHTTPServer import HTTPServer
        server = HTTPServer(('0.0.0.0', server_port), HookHandler)
        print 'Starting server, use <Ctrl-C> to stop'
        server.serve_forever()
    else:
        print("Unknown option")
