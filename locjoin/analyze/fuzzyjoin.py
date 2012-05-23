"""
Types of joins that make sense
Location
- same zoom level
  - nearest neighbor with radius
- different zoom level
  - within
  - aggregate (avg, std, max, count)
Time
- same hour, same day, same day of week, same month, same day of month

Location: has shape file, no shape file

keep track of table that have
1) been tested for joinability
2) the joins to use

invalidate table pairs where
1) latlon or shape data has changed

keep track of
- column pairs to be compared
  - within tables
  - between tables and the joins to use

compute correlations on
- ordered subsets of the data
- samples
"""
import math
import csv
import os
import sys
import numpy as np
import pdb
import traceback
#import xstats.MINE
sys.path.extend(['..', '.', '../exporters'])

from itertools import imap
from geopy import geocoders
from collections import defaultdict

from models import *


def get_correlations(db, db_session):#, t1, t2):
    """Loops through tables and computes pearson correlations between columns"""
    
    meta = MetaData(db)
    meta.reflect()
    tablenames = meta.tables.keys()

    tablemds = [Metadata.load_from_tablename(db, tn) for tn in tablenames]
    tablemds = filter(lambda md: md.state >= 3, tablemds)

    tablestats = [compute_table_stats(db, tmd.tablename) for tmd in tablemds]
    tablestats = filter(lambda s:s, tablestats)
    tablestats.sort()


    res = []
    for idx, (tablename1, r1, s1) in enumerate(tablestats):
        for tablename2, r2, s2 in tablestats[idx+1:]:

            if correlation_exists(db, tablename1, tablename2):
                print 'skipping', tablename1 ,tablename2
                continue
            print "analyzing", tablename1, tablename2
            for cp in compute_pairwise_correlations(db, db_session, tablename1, tablename2):
                db_session.add(cp)
                db_session.commit()
                res.append(cp)
    return res

def recompute_correlations(db, db_session, tablename):
    
    meta = MetaData(db)
    meta.reflect()
    tablenames = meta.tables.keys()

    tablemds = [Metadata.load_from_tablename(db_session, tn) for tn in tablenames]
    tablemds = filter(lambda md: md.state >= 3, tablemds)

    tablestats = [compute_table_stats(db, tmd.tablename) for tmd in tablemds]
    tablestats = filter(lambda s:s, tablestats)
    tablestats.sort()

    q = "delete from __dbtruck_corrpair__ where table1 = %s or table2 = %s"
    db.execute(q, [tablename, tablename])

    for idx, (tablename1, r1, s1) in enumerate(tablestats):
        for tablename2, r2, s2 in tablestats[idx+1:]:
            if tablename1 != tablename and tablename2 != tablename:
                continue

            print "analyzing:", tablename1, tablename2
            for cp in compute_pairwise_correlations(db, db_session, tablename1, tablename2):
                db_session.add(cp)
                db_session.commit()


    

def correlation_exists(db, t1, t2):
    try:
        q = """select count(*) from __dbtruck_corrpair__
               where (table1 = %s and table2 = %s) or
                     (table1 = %s and table2 = %s)"""
        n = db.execute(q, (t1, t2, t2, t1)).fetchone()[0]
        return n > 0
    except Exception as e:
        print e
        return False

def compute_table_stats(db, tablename):
    radius = compute_radius(db, tablename)
    if radius == None:
        return None
    bshape = has_shape(db, tablename)
    return tablename, radius, bshape

def compute_pairwise_correlations(db, db_session, tablename1, tablename2, join_func=None):
    stats1 = compute_table_stats(db, tablename1)
    stats2 = compute_table_stats(db, tablename2)
    tmd1 = Metadata.load_from_tablename(db_session, tablename1)
    tmd2 = Metadata.load_from_tablename(db_session, tablename2)

    r1, s1 = stats1[1:]
    r2, s2 = stats2[1:]
    r = float(np.mean([r1, r2]))

    join_func = join_func or dist_join
    joinres = join_func(db, tablename1, tablename2, r)

    ret = []
    for corr, statname, col1, col2 in test_correlation(joinres):
        agg1 = col1[2] if len(col1) > 2 else None
        agg2 = col2[2] if len(col2) > 2 else None
        ret.append( CorrelationPair(corr, r, statname,
                              col1[0], col1[1], agg1,
                              col2[0], col2[1], agg2,
                              tmd1, tmd2) )
    return ret
        

def test_correlation(join):
    # 2 el tuples can be correlated together
    # 2 el tuples can only be correlated with 3 el tuples from other table
    if not join:
        return 

    cols = join[0].keys()
    cols.sort()

    for idx, col1 in enumerate(cols):
        for col2 in cols:
            if col1 <= col2:
                continue

            for statfunc in __statfuncs__:
                try:
                    cd1 = [row[col1] for row in join]
                    cd2 = [row[col2] for row in join]
                    cd1 = map(lambda v: v and float(v) or 0, cd1)
                    cd2 = map(lambda v: v and float(v) or 0, cd2)

                    corr = statfunc(cd1, cd2)

                    if math.isnan(corr):
                        continue
                    yield corr, statfunc.__name__, col1, col2
                except Exception as e:
                    print e


def pearson_correlation(x, y):
  # Assume len(x) == len(y)
  n = len(x)
  sum_x = float(sum(x))
  sum_y = float(sum(y))
  sum_x_sq = sum(map(lambda x: pow(x, 2), x))
  sum_y_sq = sum(map(lambda x: pow(x, 2), y))
  psum = sum(imap(lambda x, y: x * y, x, y))
  num = psum - (sum_x * sum_y/n)
  den = pow((sum_x_sq - pow(sum_x, 2) / n) * (sum_y_sq - pow(sum_y, 2) / n), 0.5)
  if den == 0: return 0
  return num / den

def mine_correlation(arr1, arr2):
    arr1 = np.array(arr1)
    arr2 = np.array(arr2)
    mic = xstats.MINE.analyze_pair(arr1, arr2)['MIC']    
    return mic

try:
    import xstats.MINE
    __statfuncs__ = [mine_correlation, pearson_correlation]
    raise
except:
    __statfuncs__ = [pearson_correlation]
    

def run_join(db, t1, t2, r1, r2, s1, s2, prefix):
    if s1 and s2:
        raise Exception()
        suffix = "3(db, t1, t2)"
    elif s1:
        raise Exception()
        suffix = "2(db, t2, t1, r2)"
    elif s2:
        raise Exception()
        suffix = "2(db, t1, t2, r1)"
    else:
        suffix = "1(db, t1, t2, r1, r2)"

    cmd = "res = %s_%s" % (prefix, suffix)
    exec(cmd)
    return res
        
    

### Same zoom level

def dist_join(db, t1, t2, r, limit=7000):
    " given radius r1, r2, find overlapping lat lons "
    
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    
    if not sels:
        return []
    
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]

    q = """select count(*) from %s, %s
           where (%s._latlon <-> %s._latlon) < %%s and
           %s._latlon is not null and %s._latlon is not null
           """ % (t1, t2, t1, t2, t1, t2)
    count = db.execute(q, (r,)).fetchone()[0]
    if count == 0:
        return []
    thresh = float(limit) / count
    
    q = """select %s from %s, %s
           where (%s._latlon <-> %s._latlon) < %%s and
           %s._latlon is not null and %s._latlon is not null and
           random() <= %%s
           """ % (','.join(sels), t1, t2, t1, t2, t1, t2)
    res = db.execute(q, (r, thresh)).fetchall()
    return [dict(zip(cols, row)) for row in res]
    



### Same zoom level

def loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2, find overlapping lat lons
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    if not sels:
        return []
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     circle(%s._latlon, %%s) &&
     circle(%s._latlon, %%s) and
     %s._latlon is not null and
     %s._latlon is not null
    """ % (','.join(sels), t1, t2, t1, t2, t1, t2)

    res = db.execute(q, (r1, r2)).fetchall()
    return [dict(zip(cols, row)) for row in res]    

def loc_join_2(db, t1, t2, r1):
    # given radius r1, and t2 has shape file
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    if not sels:
        return []
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     circle(point(%s.latitude, %s.longitude), %%s) &&
     %s.shape""" % (','.join(sels), t1, t2, t1, t1, t2)
    
    res = db.execute(q, (r1, )).fetchall()
    return [dict(zip(cols, row)) for row in res]    

def loc_join_3(db, t1, t2):
    # given both shape files
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    if not sels:
        return []
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     %s.shape &&  %s.shape""" % (','.join(sels), t1, t2, t1, t2)

    res = db.execute(q).fetchall()
    return [dict(zip(cols, row)) for row in res]    


### Different zoom level -- t1 contains t2

def check_agg_loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2
    q = """select %s.id, count(*) from %s, %s where
      circle (point(%s.latitude, %s.longitude), %%s) ~
      circle (point(%s.latitude, %s.longitude), %%s)
      group by %s.id""" % (t1, t1, t2, t1, t1, t2, t2, t1)
    return db.execute(q, (r1, r2)).fetchall()

def check_agg_loc_join_2(db, t1, t2, r1):
    # given radius r1, t2.shape
    q = """select %s.id, count(*) from %s, %s where
      circle (point(%s.latitude, %s.longitude), %%s) ~ %s.shape
      group by %s.id""" % (t1, t1, t2, t1, t1, t2, t1)
    return db.execute(q, (r1,)).fetchall()

def check_agg_loc_join_3(db, t1, t2):
    # t1.shape, t2.shape
    q = """select %s.id, count(*) from %s, %s where
      %s.shape ~ %s.shape
      group by %s.id""" % (t1, t1, t2, t1, t2, t1)
    return db.execute(q).fetchall()


def get_numeric_columns(db, table, ignore=[]):
    data_types = ['smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision']
    args = ','.join(['%s'] * len(data_types))
    ignore_str = 'and not(column_name in (%s))' % ','.join(['%s']*len(ignore)) if ignore else ''
    q = """select column_name
    from information_schema.columns
    where table_name = %%s and column_name != 'latitude' and
          column_name != 'longitude' and data_type in (%s)
          %s""" % (args, ignore_str)
    cols = db.execute(q, tuple([table] + data_types + ignore)).fetchall()
    cols = [c[0] for c in cols]
    return cols
    

def aggregate_loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]
    if not sels:
        return []

    subq = """select %s.id, %s
    from %s, %s as t2
    where circle(%s._latlon, %%s) &&
          circle(%s._latlon, %%s) and
          %s._latlon is not null and
          %s._latlon is not null
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1, t2, t1, t2)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = db.execute(q, (r1, r2)).fetchall()
    return [dict(zip(sels, row)) for row in res]


def aggregate_loc_join_2(db, t1, t2, r1):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]
    if not sels:
        return []

    subq = """select %s.id, %s
    from %s, %s as t2
    where circle(%s._latlon, %%s) && t2.shape and
          %s._latlon is not null
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1, t1, t1)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = db.execute(q, (r1,)).fetchall()
    return [dict(zip(sels, row)) for row in res]


def aggregate_loc_join_3(db, t1, t2):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]
    if not sels:
        return []

    subq = """select %s.id, %s
    from %s, %s as t2
    where %s.shape && t2.shape and
    %s.shape is not null and t2.shape is not null
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1, t1, t1)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = db.execute(q).fetchall()
    return [dict(zip(sels, row)) for row in res]



def compute_radius(db, table):
    """Computing radius sizes for a table"""
    # ath.erf(0.063 / (2 ** 0.5))
    # 5% of distribution falls within 0.063 * stddev
    try:
        q = """select stddev(_latlon[0]), stddev(_latlon[1])
        from %s where _latlon is not null""" % table
        latstd, lonstd = db.execute(q).fetchone()
        #return 0.063 * (latstd + lonstd) / 2.
        return 0.03 * (latstd + lonstd) / 2.
    except:
        return None

def has_shape(db, table):
    try:
        q = "select count(*) from %s where shape is not null" % table
        return db.execute(q).fetchone()[0] > 0
    except:
        return False



if __name__ == '__main__':
    from sqlalchemy import *
    from sqlalchemy.orm import scoped_session, sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    from database import *
    import time
    #db = create_engine('postgresql://sirrice@localhost:5432/test')
    init_db()
    db.execute('delete from __dbtruck_corrpair__')

    recompute_correlations(db, db_session, 'underweight')
    exit()

    while True:
        res = get_correlations(db, db_session)
        print "sleeping"
        time.sleep(10)
    exit()

    t1, t2 = 'test2', 'test1'
    r1, r2 = 40, 5
    # print loc_join_1(db, t1, t2, r1, r2)
    # print loc_join_2(db, t1, t2, r1)
    # print loc_join_3(db, t1, t2)
    # print check_agg_loc_join_1(db, t1, t2, r1, r2)
    # print check_agg_loc_join_2(db, t1, t2, r1)
    # print check_agg_loc_join_3(db, t1, t2)
    # print aggregate_loc_join_1(db, t1, t2, r1, r2)
    # print aggregate_loc_join_2(db, t1, t2, r1)
    # print aggregate_loc_join_3(db, t1, t2)
    # print compute_radius(db, t1)
    # print compute_radius(db, t2)
