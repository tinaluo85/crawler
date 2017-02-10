#!/bin/env python
# -*-coding:utf8-*-

import json
import sys
import os
import signal
import time
import os.path
import types
import subprocess
import logging
import logging.handlers
import pdb
import datetime
import shutil
import traceback
import ConfigParser
import getopt
import urllib2
from sgmllib import SGMLParser
import chardet
import gzip
from StringIO import StringIO
import threading
import Queue
import re
import urlparse

QUIT = False
URL_PATTERN = ""

##------------------------------------------
class SourceParser(SGMLParser):
	def __init__(self):
		SGMLParser.__init__(self)
		self.hrefs = []
		self.src = []
	
	def start_a(self, attrs):
		for attr in attrs:
			if len(attr) == 2 and attr[0] == "href":
				if attr[1].find("http://") != -1 or attr[1].find("https://") != -1:
					self.hrefs.append(attr[1])
					
	def start_img(self, attrs):
			for attr in attrs:
				if len(attr) == 2 and attr[0] == "src":
					global URL_PATTERN
					if re.search(URL_PATTERN, attr[1], re.M|re.I):
						self.src.append(attr[1])
						

##------------------------------------------
class Task(object):
	def __init__(self, url, depth, maxdepth):
		self.url = url
		self.depth = depth
		self.maxdepth = maxdepth
		
##------------------------------------------ 
def save_file(url, src):
	try:
		#生成文件名
		maxlen = 50
		name = urllib2.quote(src).replace("/", "")
		if len(name) > maxlen:
			point = name.rfind(".")
			if point != -1:
				name = name[:min(maxlen, point)] + name[point:]

		if src.find("//") != -1:
			#绝对路径
			if src.find("http://") == -1 and src.find("https://") == -1:
				src = src.replace("//", "http://")
		else:
			#相对路径
			src = urlparse.urljoin(url, src)
		
		#写入文件
		filepath = "./output/" + name
		with open(filepath, "wb") as code:
			f = urllib2.urlopen(src)
			code.write(f.read())
		
	except Exception as e:
		logger.error("%s,%s, src=%s", str(type(e)), str(e), src)

##------------------------------------------ 
def test():
	pass
	
##------------------------------------------
class WorkerThread(threading.Thread):
	def __init__(self, name, tasks):
		threading.Thread.__init__(self, name=name)
		self.tasks = tasks
		self.isidle = False
		
	def is_idle(self):
		return self.isidle
		
	def run(self):
		logger.info("start thread, tid=%s", self.getName())
		
		global QUIT
		while not QUIT:
			try:
				task = self.tasks.get(True, 1)
				self.isidle = False
				self.run_spider(task.url, task.depth, task.maxdepth)
			except Queue.Empty:
				self.isidle = True
				pass
			except Exception as e:
				print e
				
		logger.info("exit thread, tid=%s", self.getName())

	def run_spider(self, url, depth, maxdepth):
		try:
			stime = time.time()
			site = urllib2.urlopen(url)
			source = site.read()
			
			#gzip解压
			if site.info().get("Content-Encoding") == "gzip":
				buffer = StringIO(source)
				zfile = gzip.GzipFile(fileobj = buffer)
				source = zfile.read()
			
			charset = chardet.detect(source)["encoding"]
			#print charset
			
			parser = SourceParser()
			parser.feed(source)
		

			#添加子链接
			subtask = 0
			if depth < maxdepth:
				subtask = len(parser.hrefs)
				for href in parser.hrefs:
					self.tasks.put(Task(href, depth + 1, maxdepth))
					
			#保存文件
			for src in parser.src:
				save_file(url, src)
			
			logger.info("tid=%s, depth=%d, time=%.2f, subtasks=%d, url=%s", 
				self.getName(), depth, time.time() - stime, subtask, url)

		except Exception as e:
			logger.error("%s,%s", str(type(e)), str(e))
			#traceback.print_exc()
	
##------------------------------------------
def load_seeds():
	seeds = []
	filepath = config["spider"]["url_list_file"]
	
	if not os.path.exists(filepath):
		logger.error("file not exist: %s", filepath)
		sys.exit(1)
	
	seeds = [ line.strip() for line in open(filepath) ]	
	if not seeds:
		logger.info("empty seeds, quit")
		sys.exit(1)
	return seeds
		
##------------------------------------------ 
def sig_handler(signum, frame):
	if signum == signal.SIGTERM or signum == signal.SIGINT:
		global QUIT 
		QUIT = True
		logger.info("quit by signal")
		
##------------------------------------------
def process_opts():
	configfile = None
	try:
		opts,args  = getopt.getopt(sys.argv[1:], "c:hv")
	except getopt.GetoptError ,err:
		print str(err)

	for op , value in opts:
		if op == "-c" and value:
			configfile = value
		elif op == "-h":
			print "help"
		elif op == "-v":
			print "version"

	if configfile == None:
		print "can not find config file"
		sys.exit(1)
	return configfile
	
##------------------------------------------
def parse_config(configfile):
	config = ConfigParser.ConfigParser()
	parse = {}
	try:
		config.read(configfile)
		for sec in config.sections():
			parse[sec] = {}
		for sec in parse:
			for key,value in config.items(sec):
				parse[sec][key] = value
	except ConfigParser.Error,e:
		print e
		sys.exit(1)
	return parse

##------------------------------------------
if __name__ == "__main__":
	reload(sys)
	sys.setdefaultencoding("utf8")
	
	#绑定信号处理函数
	#signal.signal(signal.SIGTERM, sig_handler)
	#signal.signal(signal.SIGINT, sig_handler)

	#解析命令行参数
	configfile = process_opts()

	#读取配置文件
	config = parse_config(configfile)
	print config
	
	#日志
	logger = logging.getLogger("root")
	handler = logging.FileHandler(config["file_info"]["log_file"])
	formatter = logging.Formatter('[%(asctime)s][%(lineno)d][%(levelname)s] %(message)s',
		datefmt="%Y-%m-%d %H:%M:%S")
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	logger.addHandler(logging.StreamHandler())
	logger.setLevel(logging.INFO)
	
	logger.info("spider start")
	test()
	
	#读取种子文件
	seeds = load_seeds()
	
	threadnum = int(config["spider"]["thread_count"])
	maxdepth = int(config["spider"]["max_depth"])
	URL_PATTERN = config["spider"]["target_url"]
		
	#task队列
	tasks = Queue.Queue(0)
	for url in seeds:
		tasks.put(Task(url, 0, maxdepth))
		
	#启动工作线程
	workers = []
	for tid in range(threadnum):
		thread = WorkerThread(str(tid + 1), tasks)
		thread.setDaemon(True)
		thread.start()
		workers.append(thread)
		
	
	while True:
		#若所有子线程均为idle状态，且无task，主动退出
		idle_state = [ thread.is_idle() for thread in workers ]
		if False not in idle_state and tasks.empty():
			break
	
		#若所有子线程都已结束，退出
		#alive_state = [ thread.isAlive() for thread in workers ]
		#if True not in alive_state:
		#	break
		
		time.sleep(1)
	
	logger.info("spider exit")
##------------------------------------------
