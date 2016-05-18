# coding=utf8

from mythreads import *
import time, json
import sys
from functools import partial
from itertools import izip
import query as q

def select_afid_by_auid(auid, paper_entity_list):
    afids = set()
    for e in paper_entity_list:
        if 'AA' in e:
            for AA in e['AA']:
                if 'AuId' in AA and AA['AuId'] == auid and 'AfId' in AA:
                    afids.add(AA['AfId'])
    return afids

def select_afid_auids_from_papers(author_id, paper_entities):
    afids = set()
    for paper_id, _e in paper_entities.iteritems():
        for x in _e['AA']:
            if 'AuId' in x and x.get('AuId') == author_id and 'AfId' in x:
                afids.add(x['AfId'])
    afid_auid = {}
    for paper_id, _e in paper_entities.iteritems():
        for x in _e['AA']:
            if 'AuId' in x and 'AfId' in x:
                if x.get('AfId') in afids:
                    _auid_set = afid_auid.setdefault(x['AfId'], set())
                    _auid_set.add(x['AuId'])
    return afid_auid, afids

def get_afid_auids_from_papers(paper_entities):
    afid_auid = {}
    for paper_id, paper_entity in paper_entities.iteritems():
        if 'AA' in paper_entity:
            for x in paper_entity['AA']:
                if 'AfId' in x:
                    auid_set = afid_auid.setdefault(x['AfId'], set())
                    if 'AuId' in x:
                        auid_set.add(x['AuId'])
    return afid_auid

def merge_set(entities, field):
    _r = []
    for x in entities.itervalues():
        _r.extend(x[field])
    _r = set(_r)
    return _r

def paper_paper_paths(id1, id2, e1, e2):
    return threads_jobs_first_result([
        partial(paper_paper_paths_backward, id1, id2, e1, e2),
        partial(paper_paper_paths_forward, id1, id2, e1, e2)], n=2)

# e1, e2: paper_entity
def paper_paper_paths_forward(id1, id2, e1, e2):
    # init
    paths = []
    e1_rids = e1['RId']

    # parallel query
    exprs = map(lambda _id : 'Id=%d' % _id, e1['RId'])
    e1_rid_entities = q.get_paper_entities_or_query(exprs, 'Id,AA.AuId,F.FId,J.JId,C.CId,RId', convert=True)    

    # ---- 1 hop ----
    if id2 in e1_rids:
        paths.append([id1, id2])

    # ---- 2 hop ----
    # Paper ----> Paper   ----> Paper
    for x in e1_rid_entities:
        idx = x['Id']
        if id2 in x['RId']:
            paths.append([id1, idx, id2])

    # Paper ----> Author  ----> Paper
    _u = e1['AuId'].intersection(e2['AuId'])
    _paths = map(lambda idx : [id1, idx, id2], _u)
    paths.extend(_paths)

    # Paper ----> J, F, C ----> Paper
    _u = e1['Attrs'].intersection(e2['Attrs'])
    _paths = map(lambda idx : [id1, idx, id2], _u)
    paths.extend(_paths)

    # ---- 3 hop ----
    # Paper ----> Paper ----> AuId, J, F, C ----> Paper
    for x in e1_rid_entities:
        idx = x['Id']
        idx_attrs = x['Attrs']
        _u = idx_attrs.intersection(e2['Attrs'])
        _paths = map(lambda idy : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

        idx_authors = x['AuId']
        _u = idx_authors.intersection(e2['AuId'])
        _paths = map(lambda idy : [id1, idx, idy, id2], _u)
        paths.extend(_paths)
    
    # Paper ----> Paper ----> Paper ----> Paper
    # Paper ----> AuId, J, F, C ----> Paper ----> Paper
    _paths_list = threads_jobs_orderly([
        partial(paper_rid_paper_paper_paths, id1, id2, e1, e2, e1_rid_entities),
        partial(paper_auid_paper_paper_paths, id1, id2, e1, e2),
        partial(paper_j_paper_paper_paths, id1, id2, e1, e2),
        partial(paper_f_paper_paper_paths, id1, id2, e1, e2),
        partial(paper_c_paper_paper_paths, id1, id2, e1, e2)
    ], n=5)
    for _paths in _paths_list:
        paths.extend(_paths)

    return paths

def paper_rid_paper_paper_paths(id1, id2, e1, e2, e1_rid_entities):
    paths = []
    ID2_EXPR = 'RId=%d' % id2
    exprs = []
    for x in e1_rid_entities:
        idx = x['Id']
        for idy in x['RId']:
            exprs.append('And(Id=%d,%s)' % (idy, ID2_EXPR))
    es = q.get_paper_entities_or_query(exprs, 'Id')
    ids = set(map(lambda e : e['Id'], es))
    for x in e1_rid_entities:
        idx = x['Id']
        _u = x['RId'].intersection(ids)
        _paths = map(lambda idy : [id1, idx, idy, id2], _u)
        paths.extend(_paths)
    return paths

def paper_auid_paper_paper_paths(id1, id2, e1, e2):
    paths = []
    ID2_EXPR = 'RId=%d' % id2
    exprs = map(lambda _id : 'And(Composite(AA.AuId=%d),%s)' % (_id, ID2_EXPR), e1['AuId'])
    es = q.get_paper_entities_or_query(exprs, 'Id,AA.AuId', convert=True)
    for e in es:
        idy = e['Id']
        _u = e['AuId'].intersection(e1['AuId'])
        _paths = map(lambda idx : [id1, idx, idy, id2], _u)
        paths.extend(_paths)
    return paths

def paper_j_paper_paper_paths(id1, id2, e1, e2):
    paths = []
    ID2_EXPR = 'RId=%d' % id2
    exprs = map(lambda _id : 'And(Composite(J.JId=%d),%s)' % (_id, ID2_EXPR), e1['JId'])
    es = q.get_paper_entities_or_query(exprs, 'Id,J.JId', convert=True)
    for e in es:
        idy = e['Id']
        _u = e['JId'].intersection(e1['JId'])
        _paths = map(lambda idx : [id1, idx, idy, id2], _u)
        paths.extend(_paths)
    return paths

def paper_f_paper_paper_paths(id1, id2, e1, e2):
    paths = []
    ID2_EXPR = 'RId=%d' % id2
    exprs = map(lambda _id : 'And(Composite(F.FId=%d),%s)' % (_id, ID2_EXPR), e1['FId'])
    es = q.get_paper_entities_or_query(exprs, 'Id,F.FId', convert=True)
    for e in es:
        idy = e['Id']
        _u = e['FId'].intersection(e1['FId'])
        _paths = map(lambda idx : [id1, idx, idy, id2], _u)
        paths.extend(_paths)
    return paths

def paper_c_paper_paper_paths(id1, id2, e1, e2):
    paths = []
    ID2_EXPR = 'RId=%d' % id2
    exprs = map(lambda _id : 'And(Composite(C.CId=%d),%s)' % (_id, ID2_EXPR), e1['CId'])
    es = q.get_paper_entities_or_query(exprs, 'Id,C.CId', convert=True)
    for e in es:
        idy = e['Id']
        _u = e['CId'].intersection(e1['CId'])
        _paths = map(lambda idx : [id1, idx, idy, id2], _u)
        paths.extend(_paths)
    return paths

def paper_paper_paths_backward(id1, id2, e1, e2):
    # init
    paths = []
    e1_rids = e1['RId']

    # 'parallel'
    def get_e1_rid_entities():
        exprs = map(lambda _id : 'Id=%d' % _id, e1['RId'])
        e1_rid_entities = q.get_paper_entities_or_query(exprs, 'Id,AA.AuId,F.FId,J.JId,C.CId,RId', convert=True)    
        return e1_rid_entities
    e1_rid_entities, e2_back_entities = threads_jobs_orderly([
        partial(get_e1_rid_entities), 
        partial(q.get_back_ref_entities_by_id, id2)], n=2)

    # ---- 1 hop ----
    if id2 in e1_rids:
        paths.append([id1, id2])

    # ---- 2 hop ----
    # Paper ----> Paper   ----> Paper
    for x in e1_rid_entities:
        idx = x['Id']
        if id2 in x['RId']:
            paths.append([id1, idx, id2])

    # Paper ----> Author  ----> Paper
    _u = e1['AuId'].intersection(e2['AuId'])
    _paths = map(lambda idx : [id1, idx, id2], _u)
    paths.extend(_paths)

    # Paper ----> J, F, C ----> Paper
    _u = e1['Attrs'].intersection(e2['Attrs'])
    _paths = map(lambda idx : [id1, idx, id2], _u)
    paths.extend(_paths)

    # ---- 3 hop ----
    # Paper ----> Paper ----> Paper ----> Paper
    e2_back_ids = set(e2_back_entities.iterkeys())
    for x in e1_rid_entities:
        idx = x['Id']
        idx_rids = x['RId']
        _u = idx_rids.intersection(e2_back_ids)
        _paths = map(lambda idy : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

    # Paper ----> Paper ----> AuId, J, F, C ----> Paper
    for x in e1_rid_entities:
        idx = x['Id']
        idx_attrs = x['Attrs']
        _u = idx_attrs.intersection(e2['Attrs'])
        _paths = map(lambda idy : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

        idx_authors = x['AuId']
        _u = idx_authors.intersection(e2['AuId'])
        _paths = map(lambda idy : [id1, idx, idy, id2], _u)
        paths.extend(_paths)
    
    # Paper ----> AuId, J, F, C ----> Paper ----> Paper
    for idy, y in e2_back_entities.iteritems():
        idy_attrs = y['Attrs']
        _u = idy_attrs.intersection(e1['Attrs'])
        _paths = map(lambda idx : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

        idy_authors = y['AuId']
        _u = idy_authors.intersection(e1['AuId'])
        _paths = map(lambda idx : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

    return paths

def paper_author_paths(id1, id2, e1, e2_papers):
    # init
    paths = []
    e1_rids = e1['RId']

    exprs = map(lambda _id : 'Id=%d' % _id, e1['RId'])
    e1_rid_entities = q.get_paper_entities_or_query(exprs, 'Id,AA.AuId,F.FId,J.JId,C.CId,RId', convert=True)    
        
    e2_paper_ids = set(e2_papers.iterkeys())
    e1_authors = e1['AuId']

    # 1-hop
    # Paper –AA.AuId–> Author
    if id2 in e1['AuId']:
        paths.append([id1, id2])

    # 2-hop
    # Paper ----> Paper ----> Author
    _u = e1['RId'].intersection(e2_paper_ids)
    _paths = map(lambda idx : [id1, idx, id2], _u)
    paths.extend(_paths)
    
    # 3-hop
    # Paper ----> Paper ----> Paper ----> Author
    for x in e1_rid_entities:
        idx = x['Id']
        idx_rids = x['RId']
        _u = idx_rids.intersection(e2_paper_ids)
        _paths = map(lambda idy : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

    # Paper ----> Author ----> Paper ----> Author (same author)
    for idy, y in e2_papers.iteritems():
        idy_authors = y['AuId']
        _u = e1_authors.intersection(idy_authors)
        _paths = map(lambda idx : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

    # Paper ----> J, F, C ----> Paper ----> Author
    for idy, y in e2_papers.iteritems():
        idy_attrs = y['Attrs']
        _u = e1['Attrs'].intersection(idy_attrs)
        _paths = map(lambda idx : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

    # Paper ----> Author ----> Affiliation ----> Author
    e2_afids = select_afid_by_auid(id2, e2_papers.itervalues())
    e1_authors_papers = q.get_paper_entities_orderly(e1['AuId'], 'Id,AA.AuId,AA.AfId', 'Composite(AA.AuId=%d)')
    for auid, paper_entities in e1_authors_papers:
        afids = select_afid_by_auid(auid, paper_entities)
        _u = e2_afids.intersection(afids)
        _paths = map(lambda idy : (id1, auid, idy, id2), _u)
        paths.extend(_paths)

    return paths

def author_paper_paper_paper_paths(id1, id2, e1_paper_entities):
    return threads_jobs_first_result([
        partial(author_paper_paper_paper_paths_backward, id1, id2, e1_paper_entities),
        partial(author_paper_paper_paper_paths_forward,  id1, id2, e1_paper_entities)], n=2)

def author_paper_paper_paper_paths_forward(id1, id2, e1_paper_entities):
    # Author ----> Paper ----> Paper ----> Paper
    paths = []
    ID2_EXPR = 'RId=%d' % id2
    exprs = []
    for idx, x in e1_paper_entities.iteritems():
        for idy in x['RId']:
            exprs.append('And(Id=%d,%s)' % (idy, ID2_EXPR))
    es = q.get_paper_entities_or_query(exprs, 'Id')
    ids = set(map(lambda e : e['Id'], es))
    for idx, x in e1_paper_entities.iteritems():
        _u = x['RId'].intersection(ids)
        _paths = map(lambda idy : (id1, idx, idy, id2), _u)
        paths.extend(_paths)
    return paths

def author_paper_paper_paper_paths_backward(id1, id2, e1_paper_entities):
    # Author ----> Paper ----> Paper ----> Paper
    paths = []
    idy_papers = q.get_back_ref_entities_by_id(id2, attr='Id')
    idys = set(idy_papers.iterkeys())
    for idx, x in e1_paper_entities.iteritems():
        _u = x['RId'].intersection(idys)
        _paths = map(lambda idy : (id1, idx, idy, id2), _u)
        paths.extend(_paths)
    return paths

def author_affi_author_paper(id1, id2, e1_paper_entities, e2):
    # Author ----> Affiliation ----> Author ----> Paper
    paths = []
    e1_afid_auid, e1_afids = select_afid_auids_from_papers(id1, e1_paper_entities)
    e2_authors_papers = q.get_paper_entities_orderly(e2['AuId'], 'Id,AA.AuId,AA.AfId', 'Composite(AA.AuId=%d)')
    for auid, paper_entities in e2_authors_papers:
        afids = select_afid_by_auid(auid, paper_entities)
        _u = e1_afids.intersection(afids)
        _paths = map(lambda idx : (id1, idx, auid, id2), _u)
        paths.extend(_paths)
    return paths

def author_paper_paths(id1, id2, e1_papers, e2):
    # init
    paths = []
    # e1_papers = q.get_paper_entities_by_auid(id1)
    e2_authors = e2['AuId']
    
    # 1-hop
    # Author ----> Paper
    if id1 in e2['AuId']:
        paths.append((id1, id2))

    # 2-hop
    # Author ----> Paper ----> Paper
    for idx, x in e1_papers.iteritems():
        idx_rids = x['RId']
        if id2 in idx_rids:
            paths.append((id1, idx, id2))

    # 3-hop
    # Author ----> Paper ----> Paper ----> Paper
    # Author ----> Affiliation ----> Author ----> Paper
    _paths_list = threads_jobs_orderly([
        partial(author_paper_paper_paper_paths, id1, id2, e1_papers),
        partial(author_affi_author_paper, id1, id2, e1_papers, e2)], n=2)
    for _paths in _paths_list:
        paths.extend(_paths)

    # Author ----> Paper ----> Author ----> Paper
    for idx, x in e1_papers.iteritems():
        idx_authors = x['AuId']
        _u = idx_authors.intersection(e2['AuId'])
        _paths = map(lambda idy : (id1, idx, idy, id2), _u)
        paths.extend(_paths)

    # Author ----> Paper ----> J, F, C ----> Paper
    for idx, x in e1_papers.iteritems():
        idx_attrs = x['Attrs']
        _u = idx_attrs.intersection(e2['Attrs'])
        _paths = map(lambda idy : (id1, idx, idy, id2), _u)
        paths.extend(_paths)

    return paths

def author_author_paths(id1, id2, e1_paper_entities, e2_paper_entities):
    # init
    paths = []
    
    e1_paper_ids = set(e1_paper_entities.iterkeys())
    e2_paper_ids = set(e2_paper_entities.iterkeys())
    _, e1_afid = select_afid_auids_from_papers(id1, e1_paper_entities)
    _, e2_afid = select_afid_auids_from_papers(id2, e2_paper_entities)
    
    # 2-hop
    # Author ----> Paper ----> Author (same author cycle)
    _u = e1_paper_ids.intersection(e2_paper_ids)
    _paths = map(lambda idx : [id1, idx, id2], _u)
    paths.extend(_paths)

    # Author ----> Affiliation ----> Author
    _u = e1_afid.intersection(e2_afid)
    _paths = map(lambda idx : [id1, idx, id2], _u)
    paths.extend(_paths)

    # 3-hop
    # Author ----> Paper ----> Paper ----> Author
    for idx, x in e1_paper_entities.iteritems():
        idx_rids = x['RId']
        _u = idx_rids.intersection(e2_paper_ids)
        _paths = map(lambda idy : [id1, idx, idy, id2], _u)
        paths.extend(_paths)

    return paths

def get_type(id, es, papers):
    # is id author ?
    type_num = 0
    if len(papers) > 0:
        type_num = 2
    else:
        # is id paper ?
        if id in es:
            type_num = 1
    return type_num

def get_answer(id1, id2):
    # es = q.get_paper_entities([id1, id2])
    es, e1_papers, e2_papers = threads_jobs_orderly([
        partial(q.get_paper_entities, [id1, id2]),
        partial(q.get_paper_entities_by_auid, id1),
        partial(q.get_paper_entities_by_auid, id2),
    ], n=3)

    # 1 for paper, 2 for author
    type1 = get_type(id1, es, e1_papers)
    type2 = get_type(id2, es, e2_papers)

    paths = []
    if type1 == 1 and type2 == 1:
        paths = paper_paper_paths(id1, id2, es[id1], es[id2])
    elif type1 == 1 and type2 == 2:
        paths = paper_author_paths(id1, id2, es[id1], e2_papers)
    elif type1 == 2 and type2 == 1:
        paths = author_paper_paths(id1, id2, e1_papers, es[id2])
    elif type1 == 2 and type2 == 2:
        paths = author_author_paths(id1, id2, e1_papers, e2_papers)
    return paths, type1, type2

