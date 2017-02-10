#!/bin/bash

pid=`ps -ef | grep 'python spider.py' | grep -v grep | awk '{print $2}'`
echo "kill ${pid}"
kill -9 ${pid}

