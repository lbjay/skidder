'''
Created on Jun 19, 2013

@author: jluker
'''
import os
import re
import sys
import gzip
import pytz
import redis
import socket
import logging
import datetime
import itertools
import simplejson as json
from multiprocessing import Process, JoinableQueue, cpu_count
from optparse import OptionParser

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger()

def redis_connection(uri):
    if '/' in uri:
        (host, db) = uri.split('/')
    else:
        host = uri
        db = "0" 
    if ':' in host:
        (host, port) = host.split(':')
    else:
        port = "6379"
    rc = redis.Redis(host=host, port=int(port), db=int(db))
    return rc

def generate_payload(event, type, tags, source_host):
    line, filename, line_number = event
    now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    payload = {
        '@message': line,
        '@tags': tags,
        '@type': type,
        '@source': "%s:%d" % (filename, line_number),
        '@source_path': filename,
        '@source_host': source_host,
        '@timestamp': now.isoformat()
    }      
    return payload
                
class Worker(Process):
    def __init__(self, task_queue, opts):
        Process.__init__(self)
        self.task_queue = task_queue
        self.opts = opts
        self.redis = redis_connection(opts.redis)
        
    def run(self):
        while True:
            event = self.task_queue.get()
            if event is None:
                self.task_queue.task_done()
                break
            else:
                payload = generate_payload(event, self.opts.type, self.opts.tags, self.opts.source_host)
                self.redis.rpush(self.opts.key, json.dumps(payload))
                self.task_queue.task_done()
  
def run_syncronous(opts, files):
    rc = redis_connection(opts.redis)
    for event in events(files, opts):
        payload = generate_payload(event, opts.type, opts.tags, opts.source_host)
        rc.rpush(opts.key, json.dumps(payload))
    
def enum_with_filename(f):
    if f == 'stdin':
        enum = enumerate(sys.stdin, 1)
    else:
        f = os.path.abspath(f)
        if f.endswith('.gz'):
            fh = gzip.open(f, 'rb')
        else:
            fh = open(f, 'r')
        enum = enumerate(fh, 1)
    for i, line in enum:
        yield (line.rstrip(), f, i)
        
def events(files, opts):
    if '-' in files:
        if len(files) > 1:
            log.warn("cannot read from both stdin and files")
        log.info("generating input from stdin")
        event_iter = enum_with_filename('stdin')
    else:
        inputs = []
        for f in files:
            log.info("generating input from %s" % f)
            inputs.append(enum_with_filename(f))
        event_iter = itertools.chain.from_iterable(inputs)
    if opts.multiline:
        return multiline_events(event_iter, opts)
    return event_iter
    
def merge_events(events):
    if not len(events) > 1: 
        return events[0]
    msg, source, lineno = events[0]
    for e in events[1:]:
        msg += "\n%s" % e[0]
    return msg, source, lineno
    
def multiline_events(event_iter, opts):
    pending = []
    multiline_match = re.compile(opts.multiline)

    for event in event_iter:
        match = multiline_match.search(event[0])
        match = (match and not opts.multiline_negate) or (not match and opts.multiline_negate)
        if match:
            if pending:
                yield merge_events(pending)
                pending = []
        pending.append(event)
    
def main(opts, files):
    
    if opts.threads == 1:
        log.info("running synchronously")
        run_syncronous(opts, files)
    else:
        Q = JoinableQueue()
        workers = [Worker(Q, opts) for i in xrange(opts.threads)]
        
        log.info("initializing %d threads" % opts.threads)
        for w in workers:
            w.start()
            
        # push log events onto the queue
        events_iter = events(files, opts)
        if opts.limit:
            events_iter = itertools.islice(events_iter, opts.limit)
            
        for event in events_iter:
            Q.put(event)
        
        # add poison pills 
        for i in xrange(opts.threads):
            Q.put(None)
            
        Q.join()
        log.info("work complete. shutting down threads.")
        for w in workers:
            w.join()   
        
if __name__ == '__main__':
    
    op = OptionParser()
    op.set_usage("usage: skidder.py [options] file, file, ...") 
    op.add_option('-T','--threads', dest="threads", action="store", type=int, default=cpu_count()) # * 2)
    op.add_option('-s','--source_host', dest="source_host", action="store")
    op.add_option('-t','--type', dest="type", action="store", default="file")
    op.add_option('-g','--tag', dest="tags", action="append", default=[])
    op.add_option('-r','--redis', dest="redis", action="store", default="localhost:6379/0")
    op.add_option('-k','--key', dest="key", action="store")
    op.add_option('-l','--limit', dest="limit", action="store", type=int)
    op.add_option('-m','--multiline', dest="multiline", action="store")
    op.add_option('-N','--multiline_negate', dest="multiline_negate", action="store_true")
    op.add_option('-d','--debug', dest="debug", action="store_true", default=False)
    opts, args = op.parse_args() 
    
    if not opts.source_host:
        opts.source_host = socket.gethostname()
        
    if not opts.key:
        opts.key = "skidder:" + opts.type
        
    if opts.debug:
        log.setLevel(logging.DEBUG)
            
    main(opts, args)