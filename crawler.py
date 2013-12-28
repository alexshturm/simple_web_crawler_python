#!/usr/bin/env python

from HTMLParser import HTMLParser
import urlparse
import urllib2
import Queue
from threading import Thread, Lock
import os, shutil
import tempfile
import time

START_URL = "https://archive.org"
MAX_DEPTH = 1
NUM_THREADS = 5
DIRECTORY = "files"

debug = True
print_lock = Lock()

class CrawlRequest:
    def __init__(self, url, level):
        self.url = url
        self.level = level

class CrawlResult:
    def __init__(self, url, level, data, html = False):
        self.url = url
        self.level = level
        self.data = data
        self.html = html

class CrawlThread(Thread):
    def __init__(self, in_queue, out_queue):
        Thread.__init__(self)
        self.in_queue = in_queue    # queue of CrawlRequest
        self.out_queue = out_queue  # queue of CrawlResult

    def run(self):
        while True:
            request = self.in_queue.get()
            url, level = request.url, request.level
            if url is None: # command to stop the thread
                self.in_queue.task_done()
                break
            if debug:
                with print_lock: print self.name, "- START", level, url
            try:
                response = urllib2.urlopen(url)
            except urllib2.URLError:
                if debug:
                    with print_lock: print self.name, "- FAIL ", level, url
                self.out_queue.put(CrawlResult(url, level, None))  # no data
            else:
                try:
                    html = response.headers['content-type'][:9] == 'text/html'
                except KeyError:
                    html = True  # ???
                data = response.read()
                self.out_queue.put(CrawlResult(url, level, data, html))
                if debug:
                    with print_lock: print self.name, "- END  ", level, url
            self.in_queue.task_done()
        if debug:
            with print_lock: print self.name, "stopped"

class LinkParser(HTMLParser):
    def __init__(self, base_url):
        HTMLParser.__init__(self)
        self.base_url = base_url
        self.urls = list()

    def handle_starttag(self, tag, attrs):
        url = None
        if tag == 'a':
            for key, value in attrs:
                if key == 'href':
                    url = value
                    break
        elif tag == 'img':
            for key, value in attrs:
                if key == 'src':
                    url = value
                    break
        if url is not None:
            url = urlparse.urljoin(self.base_url, url)
            url = urlparse.urldefrag(url)[0]  # remove fragment
            url = url.rstrip('/')
            self.urls.append(url)

    def run(self, data):
        self.feed(data)
        return self.urls

class Crawler():
    def __init__(self, start_url, num_threads=NUM_THREADS, directory=DIRECTORY, max_depth=MAX_DEPTH):
        self.num_threads = num_threads
        self.max_depth = max_depth
        self.urls = Queue.Queue()
        self.urls.put(CrawlRequest(start_url, 0))
        self.contents = Queue.Queue()
        self.seen = set()
        self.seen.add(start_url)
        self.directory = directory
        self.files = dict()
        self.errors = dict()
        self.counter = 1

    def run(self):
        shutil.rmtree(self.directory, True)
        os.mkdir(self.directory)
        threads = [CrawlThread(self.urls, self.contents) for _ in range(self.num_threads)]
        for t in threads:
            t.start()
        while self.counter > 0:
            try:
                result = self.contents.get()
                self.counter -= 1
                with print_lock: print "MainThrd - RCVD ", result.level, result.url, \
                                       "[", len(result.data) if result.data is not None else 0, "]"
                if result.data is not None:
                    # write into a file
                    filehandle, filename = tempfile.mkstemp(dir=self.directory)
                    os.write(filehandle, result.data)
                    os.close(filehandle)
                    # update the map: URL -> filename
                    self.files[result.url] = filename
                    # process HTML page
                    if result.level < self.max_depth:  # don't go deeper
                        if result.html:
                            parser = LinkParser(result.url)
                            for url in parser.run(result.data):
                                if url not in self.seen:
                                    self.seen.add(url)
                                    self.urls.put(CrawlRequest(url, result.level+1))
                                    self.counter += 1
                else:  # some error
                    self.errors[result.url] = "Could not access"
                self.contents.task_done()
            except KeyboardInterrupt:
                with print_lock: print "*** Interrupted ***"
                # flush the queues
                while not self.urls.empty():
                    self.urls.get()
                    self.urls.task_done()
                while not self.contents.empty():
                    self.contents.get()
                    self.contents.task_done()
                break
        for t in threads:
            self.urls.put(CrawlRequest(None, result.level+1))  # command to stop the thread
        for t in threads:
            t.join()  # wait for threads to stop
        return (self.files, self.errors)


if __name__ == "__main__":
    crawler = Crawler(START_URL)
    files, errors = crawler.run()
    print '*'*10, "Mapping of URLs to files", '*'*10
    for url, filename in sorted(files.iteritems()):
        print url, "->", filename
    print '*'*10, "Errors", '*'*10
    for url, error in sorted(errors.iteritems()):
        print url, "->", error


