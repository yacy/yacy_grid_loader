#!/usr/bin/env python

import os
import socket

path_apphome = os.path.dirname(os.path.abspath(__file__)) + '/..'
os.chdir(path_apphome)

def checkportopen(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    return sock.connect_ex(('127.0.0.1', port)) == 0

if not checkportopen(8200):
    os.chdir(path_apphome)
    os.system('cd ../yacy_grid_loader')
    os.system('gradle run')
    
    
