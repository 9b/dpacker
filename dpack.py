import urllib
from urllib2 import Request, urlopen, URLError, HTTPError
import simplejson as json
import random
import os
import logging
import zipfile
import hashlib
import pymongo
from pymongo import Connection
import gridfs
import inspect
import sys
import gevent
from gevent import monkey
from gevent.pool import Pool

def logger(handler,level):
	'''
	Get a logging instance
	@param	handler name of the logging instance
	@param	level	level in which to log
	@return logging object used for later on
	'''
	import logging

	log = logging.getLogger(handler)
	if level == "INFO":
		logging.basicConfig(level=logging.INFO)
	elif level == "DEBUG":
		logging.basicConfig(level=logging.DEBUG)
	elif level == "ERROR":
		logging.basicConfig(level=logging.ERROR)
	else:
		pass

	return log

class dpack:
	def __init__(self,api,cx):
		self.__log = logger(self.__class__.__name__,"DEBUG")
		self.__baseUrl = 'https://www.googleapis.com/customsearch/v1?'
		self.__apiKey = api
		self.__apiCx = cx

		self.__ftypes = ['pdf','doc','docx','xls','ppt','pptx','xlsx']
		
		self.__urlList = []
		self.__fileList = []
		
		self.__outPath = ''
		self.__urlQuery = None
		
	def dumpZip(self):
		name = self.__outPath + self.__urlQuery.replace(' ','') + ".zip"
		self.__log.info("Creating zip package: %s" % name)
		zip = zipfile.ZipFile(name, 'w')
		for f in self.__fileList:
			zip.writestr(f['fname'],f['file'])
		zip.close()
		
	def __fetchFile(self, url):
		try:
			req = Request(url)

			try:
				f = urlopen(req,timeout = 2)
				fname = url.split("/")[-1]
				contents = f.read()
				hashed = hashlib.md5(contents).hexdigest()
				kwords = self.__urlQuery.split(" ")
				obj = { 'filename':fname, 'file':f.read(), 'query':self.__urlQuery, 'filehash':hashed, 'filesize':len(contents), 'filetype':fname.split(".")[-1], 'url':url, 'keywords': kwords }
				self.__log.info("Downloaded %s [%s]" % (fname,str(obj['filesize'])))
				self.__fileList.append(obj)
			except HTTPError, e:
				print "HTTP Error:",e.code , url
			except URLError, e:
				print "URL Error:",e.reason , url
			except Exception, e:
				pass
				
		except Exception, e:
			print e
		
	def __obtainFiles(self):
		# use gevent to spawn a bunch of greenlets to request different files
		monkey.patch_all()
		jobs = [gevent.spawn(self.__fetchFile, url) for url in self.__urlList]
		gevent.joinall(jobs,timeout=60) # timeout because we don't care how much we get
		
	def __obtainUrls(self):
		for i in range(0,3): # this should be a config
			prevIndex = "1"
			try:
				ftype = self.__ftypes[random.randint(0,len(self.__ftypes)-1)]
				self.__ftypes.remove(ftype) # pop our choice out of the list to avoid dups
			except:
				self.__ftypes = ['doc']
				
			kill = False # flag bit for when Google doesn't want to return content
			
			for i in range(0,5): # this should be a config
				if not kill:
					query = urllib.urlencode({'q' : '%s filetype:%s' % (self.__urlQuery, ftype)})
					self.__log.info("Using query: %s" % query)
					url = "%skey=%s&cx=%s&start=%s&%s" % (self.__baseUrl,self.__apiKey,self.__apiCx,prevIndex,query)
					search_results = urllib.urlopen(url)
					
					try:
						tmp = json.loads(search_results.read())
						results = tmp['items']
						for r in results:
							self.__urlList.append(r['link'])
							self.__log.info("%s added to list" % (r['link']))
						prevIndex = str(tmp['queries']['nextPage'][0]['startIndex'])
					except Exception, e:
						kill = True
						self.__log.error(str(e))
					
		self.__log.info("Processing %s URLs" % str(len(self.__urlList)) )
		
	def setFtypes(self,ftypes):
		if type(ftypes) != list:
			raise Exception("Value must be a list of file types")
		else:
			self.__ftypes = ftypes
	
	def setQuery(self,query):
		self.__log.info("Query set: %s" % query)
		self.__urlQuery = query
		
	def setOutPath(self,path): # should check the path, but lazy
		self.__log.info("Path set: %s" % path)
		self.__outPath = path
		
	def createDpack(self):
		if self.__urlQuery != None:
			self.__log.info("Obtaining URLs")
			self.__obtainUrls()
			self.__log.info("Obtaining files")
			self.__obtainFiles()
		else:
			raise Exception("Query not set")
			
	def sprayPack(self,col):
		self.__log.info("Spraying into %s" % col)
		gfsDb = Connection()[col] # assume local host
		fs = gridfs.GridFS(gfsDb)
		for handle in self.__fileList:
			try:
				if not fs.exists({"hash":handle['filehash']}):
					with fs.new_file(                                                            
						filename = handle['filename'],
						type = handle['filetype'],
						size = handle['filesize'],
						hash = handle['filehash'],
						query = handle['query'],
						kwords = handle['keywords'],
						url = handle['url']
					) as fp:
						fp.write(handle['file'])
				else:
					self.__log.info("%s already inserted" % handle['filename'])
			except Exception, e:
				self.__log.error("Failed to spray: %s" % (str(e)))
	
	def writePack(self,path):
		self.__outPath = path
		for f in self.__fileList:
			handle = open(self.__outPath + f['filename'],'w')
			handle.write(f['file'])
			self.__log.debug("Wrote %s [%s]" % (f['filename'],f['filehash']) )
			handle.close()
		self.__log.info("Files saved to %s" % self.__outPath)
