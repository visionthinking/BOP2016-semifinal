# coding=utf8

import requests, json
import threading, time
from mythreads import *
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from itertools import chain

base_url = 'http://oxfordhk.azure-api.net/academic/v1.0/evaluate?expr=%s&count=%d&attributes=%s&subscription-key=%s'
query_cnt = 0
cache_no_hit = 0
query_cache = {}
cache_lock = threading.Lock()

ENABLE_CACHE = True
CLEAR_CACHE = True
ENABLE_SHOW_QUERY = False
ENABLE_SHOW_LONG_QUERY = False
ENABLE_SESSION = True
MAX_CONNECTIONS = 1000

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_CONNECTIONS, pool_maxsize=MAX_CONNECTIONS)
session.mount('http://', adapter)

def clear_cache():
    global query_cache
    global query_cnt, cache_no_hit
    cache_lock.acquire()
    query_cache = {}
    cache_lock.release()
    query_cnt = 0
    cache_no_hit = 0

def query(expr, attributes, count=99999999, _key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'):
    if ENABLE_CACHE:
        return query_with_cache(expr, attributes, count, _key)
    return query_without_cache(expr, attributes, count, _key)

def query_with_cache(expr, attributes, count=99999999, _key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'):
    url = base_url % (expr, count, attributes, _key)
    global query_cache
    cache_lock.acquire()
    r = query_cache.get(url)
    cache_lock.release()
    if r != None:
        return r
    for _timeout in [5, 10, 20]:
        try:
            start = time.time()
            r = json.loads(session.get(url, timeout=_timeout).text)
            time_secs = time.time() - start
            
            if ENABLE_SHOW_QUERY:
                cache_lock.acquire()
                print '  query: [%4.2fs] %s' % (time_secs, expr[:])
                cache_lock.release()
            if ENABLE_SHOW_LONG_QUERY and time_secs > 2:
                cache_lock.acquire()
                print '  slow query: [%4.2fs] %s' % (time_secs, expr[:])
                cache_lock.release()

            if len(url) <= 500:
                cache_lock.acquire()
                query_cache[url] = r
                cache_lock.release()
            return r
        except Exception, e:
            cache_lock.acquire()
            print 'timeout.', url[:]
            cache_lock.release()
    return {}

def query_without_cache(expr, attributes, count=99999999, _key = 'f7cc29509a8443c5b3a5e56b0e38b5a6'):
    url = base_url % (expr, count, attributes, _key)
    global query_cnt
    cache_lock.acquire()
    query_cnt += 1
    cache_lock.release()
    for _timeout in [5, 10, 20]:
        try:
            start = time.time()
            r = json.loads(session.get(url, timeout=_timeout).text)
            time_secs = time.time() - start
            
            if ENABLE_SHOW_QUERY:
                cache_lock.acquire()
                print '  query: [%4.2fs] %s' % (time_secs, expr[:])
                cache_lock.release()
            if ENABLE_SHOW_LONG_QUERY and time_secs > 2:
                cache_lock.acquire()
                print '  slow query: [%4.2fs] %s' % (time_secs, expr[:])
                cache_lock.release()
            return r
        except Exception, e:
            cache_lock.acquire()
            print 'timeout.', url[:]
            cache_lock.release()
    return {}


def entity_convert(e):
    e['RId'] = set(e['RId']) if 'RId' in e else set()
    
    authors = set()
    if 'AA' in e:
        for x in e['AA']:
            authors.add(x['AuId'])
    e['AuId'] = authors
    
    attrs = set()
    e['FId'] = set()
    e['CId'] = set()
    e['JId'] = set()
    if 'F' in e:
        for x in e['F']:
            attrs.add(x['FId'])
            e['FId'].add(x['FId'])
    if 'C' in e:
        _cid = e['C']['CId']
        attrs.add(_cid)
        e['CId'].add(_cid)
    if 'J' in e:
        _jid = e['J']['JId']
        attrs.add(_jid)
        e['JId'].add(_jid)
    e['Attrs'] = attrs
    return e

def get_paper_entities(paper_ids, attr="Id,AA.AuId,F.FId,J.JId,C.CId,RId"):
    if len(paper_ids) == 0:
        return {}
    expr_ids = map(lambda x : 'Id=%d' % x, paper_ids)
    expr = reduce(lambda x,y:'Or(%s,%s)'%(x, y), expr_ids)
    data = query(expr, attr)
    r = {}
    if 'entities' in data:
        for x in data['entities']:
            _id = int(x['Id'])
            r[_id] = entity_convert(x)
    return r

# parallel query
def get_paper_entities_threads(ids, attr="Id,AA.AuId,F.FId,J.JId,C.CId,RId", expr_format='Id=%d'):
    if len(ids) == 0:
        return {}
    _exprs = map(lambda x : expr_format % x, ids)

    def _query(__expr):
        return query(__expr, attr)
    results = threads_same_job(_query, _exprs, n=len(_exprs))
    r = {}
    for data in results:
        if 'entities' in data:
            for x in data['entities']:
                _id = int(x['Id'])
                r[_id] = entity_convert(x)
    return r

# parallel query
# return [(id1, [es1A, es1B, ...])]
def get_paper_entities_orderly(ids, attr, expr_format, convert=False):
    if len(ids) == 0:
        return []
    _exprs = map(lambda x : expr_format % x, ids)
    results = threads_same_job(lambda ex : query(ex, attr), _exprs, n=len(_exprs))
    results = map(lambda x : x['entities'] if 'entities' in x else [], results)
    if convert:
        for es in results:
            for e in es:
                e = entity_convert(e)
    return zip(ids, results)

# mulit-threaded
def get_paper_entities_by_exprs_orderly(ids_attrs_expr, convert=False):
    if len(ids_attrs_expr) == 0:
        return []
    def _query((id, attr, expr)):
        return query(expr, attr)
    results = threads_same_job(_query, ids_attrs_expr, n=len(ids_attrs_expr))
    results = map(lambda x : x['entities'] if 'entities' in x else [], results)
    if convert:
        for es in results:
            for e in es:
                e = entity_convert(e)
    return zip(ids_attrs_expr, results)

def get_back_ref_entities_by_id(paper_id, attr='Id,AA.AuId,F.FId,J.JId,C.CId'):
    data = query("RId=%d" % paper_id, attr)
    r = {}
    if 'entities' in data:
        for x in data['entities']:
            _id = int(x['Id'])
            r[_id] = entity_convert(x)
    return r

# deprecated
def get_back_ref_entities_advanced(paper_id, paper_ids=[], auids=[], jids=[], cids=[], fids=[], attr='Id,AA.AuId,F.FId,J.JId,C.CId'):
    expr_ids = map(lambda x : 'And(Id=%d,RId=%d)' % (x, paper_id), paper_ids)
    expr_auids = map(lambda x : 'And(Composite(AA.AuId=%d),RId=%d)' % (x, paper_id), auids)
    expr_jids = map(lambda x : 'And(Composite(J.JId=%d),RId=%d)' % (x, paper_id), jids)
    expr_cids = map(lambda x : 'And(Composite(C.CId=%d),RId=%d)' % (x, paper_id), cids)
    expr_fids = map(lambda x : 'And(Composite(F.FId=%d),RId=%d)' % (x, paper_id), fids)
    _exprs = expr_ids
    _exprs.extend(expr_auids)
    _exprs.extend(expr_jids)
    _exprs.extend(expr_cids)
    _exprs.extend(expr_fids)
    
    if len(_exprs) == 0:
        return {}
    
    # parallel query
    def _query(__expr):
        return query(__expr, attr)
    results = threads_same_job(_query, _exprs, n=len(_exprs))
    # combine results
    r = {}
    for data in results:
        if 'entities' in data:
            for x in data['entities']:
                _id = int(x['Id'])
                r[_id] = entity_convert(x)
    return r

def get_paper_entities_by_auid(auid, attr='Id,AA.AuId,AA.AfId,F.FId,J.JId,C.CId,RId'):
    data = query("Composite(AA.AuId=%d)" % auid, attr)
    r = {}
    if 'entities' in data:
        for x in data['entities']:
            _id = int(x['Id'])
            r[_id] = entity_convert(x)
    return r

# multi-or / multi-threads-multi-or
# 设计思路：必须是同类请求，直接返回entities_list
# 如果or表达式过长，则自动分开并行执行
def get_paper_entities_or_query(exprs, attr, convert=False, or_limit=1000):
    if len(exprs) == 0:
        return []
    or_exprs = ''
    expr_fragments = []
    for expr in exprs:
        if len(or_exprs) == 0:
            or_exprs = expr
        else:
            or_exprs = 'Or(%s,%s)' % (or_exprs, expr)
            if len(or_exprs) >= or_limit:
                expr_fragments.append(or_exprs)
                or_exprs = ''
    if len(or_exprs) > 0:
        expr_fragments.append(or_exprs)
    # print 'How many fragments?  ', len(expr_fragments)
    # print expr_fragments
    def _query(expr):
        data = query(expr, attr)
        r = data['entities'] if 'entities' in data else []
        if convert:
            r = map(lambda e : entity_convert(e), r) 
        return r
    rs = threads_same_job(_query, expr_fragments, n=len(expr_fragments))
    results = []
    for r in rs:
        results.extend(r)
    return results

def get_paper_entities_or_query_limited(exprs, attr, convert=False):
    if len(exprs) == 0:
        return []
    or_exprs = reduce(lambda x, y : 'Or(%s,%s)' % (x, y), exprs)
    data = query(or_exprs, attr)
    r = data['entities'] if 'entities' in data else []
    if convert:
        r = map(lambda e : entity_convert(e), r)
    return r

if __name__ == '__main__':
    auids = [
        1985946277,
        712236833,
        2108758481,
        2311341733,
        2163456349,
        2091521132,
        2165430648,
        1984995899,
        2100656703,
        2145456028,
        2042038231
    ]
    # parrel query
    for i in xrange(10):
        e1_authors_papers = get_paper_entities_orderly(auids, 'Id,AA.AuId,AA.AfId', 'Composite(AA.AuId=%d)')
        # print e1_authors_papers
        print 'Finished.'

    # for i in xrange(10):
    #     exprs = map(lambda _id : 'Composite(AA.AuId=%d)' % _id, auids)
    #     es = get_paper_entities_or_query(exprs, 'Id,AA.AuId,AA.AfId', convert=True, or_limit=200)
    #     print 'Finished.'