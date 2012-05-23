import csv
import os
import sys
import pdb
import time
import random
import pickle
import traceback
import locjoin.settings as settings
sys.path.extend(['..', '.', '../exporters'])

from operator import add, and_
from collections import defaultdict
from sqlalchemy import *
from locjoin.util import to_utf

from geocode import DBTruckGeocoder
from database import init_db, db_session
from location import possible_loc, re_badchar
from models import *




ALTERQUERY = """ALTER TABLE %s
add _latlon point null,
add _address text null,
add _city varchar(128) null,
add _state varchar(128) null,
add _country varchar(128) null,
add _query text null,
add _description text null,
add _shape polygon null,
add _zipcode varchar(128) null,
add _geocoded bool default false
"""

DROPSHADOW = """ALTER TABLE %s
drop _latlon,
drop _address,
drop _city,
drop _state,
drop _country,
drop _query,
drop _description,
drop _shape,
drop _zipcode,
drop _geocoded
"""



def reset_system(db, db_session):
    drop_shadows(db, db_session)
    delete_metadata(db, db_session)
    reset_metadata(db, db_session)
    
def drop_shadows(db, db_session):
    print "resetting system"
    meta = MetaData(db)
    meta.reflect()
    for tablename, schema in meta.tables.items():
        try:
            db.execute(DROPSHADOW % tablename)
            tablemd = Metadata.load_from_tablename(db_session, tablename)
            tablemd.state = 0
            db_session.add(tablemd)
            db_session.commit()
        except:
            pass
    meta.reflect()
    
    
def drop_metadata(db):
    db.execute("drop table %s cascade" % CorrelationPair.__tablename__)
    db.execute("drop table %s cascade" % Annotation.__tablename__)
    db.execute("drop table %s cascade" % Metadata.__tablename__)

def delete_metadata(db):
    db.execute("delete from %s cascade" % CorrelationPair.__tablename__)
    db.execute("delete from %s cascade" % Annotation.__tablename__)
    db.execute("delete from %s cascade" % Metadata.__tablename__)
    


def run_location_extractor(db, db_session, filter=None):
    """
    Entry point for analyzing existing tables in database and
    extracting locations
    """
    
    meta = MetaData(db)
    meta.reflect()

    results = []
    for tablename in meta.tables.keys():
        if not filter(tablename):
            continue
        if tablename.startswith('__dbtruck'):
            continue

        try:
            result = run_state_machine(db, db_session, tablename)
            results.append(result)
        except:
            raise

    states = results#[result.get() for result in results]
        
    return states
    


def run_state_machine(db, db_session, tablename):
    """
    Execute a state machine to analyze and extract location data from
    a single table
    """
    meta = MetaData(db)
    meta.reflect()
    schema = meta.tables[tablename]
    tablemd = Metadata.load_from_tablename(db_session, tablename)
    

    cols = schema.columns.keys()
    if tablemd.state == 0 or ('_latlon' not in cols and '_address' not in cols):
        # check if table exists in metadata table
        execute_state_0(db, tablemd, schema)

    if tablemd.state == 1:
        # check if shadow cols exist
        execute_state_1(db, tablemd, schema)        

    if tablemd.state == 2:
        # check if table needs to be analyzed for annotations
        execute_state_2(db, tablemd, schema)        

    if tablemd.state == 3 or tablemd.state == 4:
        # check if table has annotations and no geocode/shapes
        execute_state_3(db, tablemd, schema)
        
    if tablemd.state == 5:
        return tablemd.state

    return tablemd.state


def execute_state_0(db, tablemd, schema):
    print "state 0\t", tablemd.tablename
    cols = schema.columns.keys()    

    if '_latlon' not in cols and '_address' not in cols:
        # create shadow columns
        db.execute(ALTERQUERY % tablemd.tablename)

    tablemd.state = 1
    db_session.add(tablemd)
    db_session.commit()

def execute_state_1(db, tablemd, schema):
    "annotype -> [(column, extractorname)]"
    print "state 1\t", tablemd.tablename
    extractors = find_location_extractors(db, tablemd, schema)
    annos = []
    for anntype, pairs in extractors.iteritems():
        for colname, extractor in pairs:
            annos.append(Annotation(colname, anntype, extractor, tablemd))
    tablemd.hasloc = len(annos) > 0
    tablemd.state = 2
    db_session.add_all(annos)
    db_session.add(tablemd)
    db_session.commit()
    return tablemd.state


def execute_state_2(db, tablemd, schema):
    "fill in the shadow columns"
    print "state 2\t", tablemd.tablename
    needs_geocoding = populate_shadow_cols(db, tablemd, schema)
    tablemd.state = 3 if needs_geocoding else 5
    db_session.add(tablemd)
    db_session.commit()
    return tablemd.state


def execute_state_3(db, tablemd, schema):
    "geocode the table"

    print "state 3\t", tablemd.tablename
    maxid = geocode_table(db, tablemd)
    try:
        if maxid == db.execute('select max(id) from %s' % tablemd.tablename).fetchone()[0]:
            tablemd.state = 5
            db_session.add(tablemd)
            db_session.commit()
    except Exception as e:
        raise
    return tablemd.state


def geocode_table(db, tablemd):
    """
      check what most precise locations annotation is
      (latlon/address/city/county/zip/state/country)
      if address:
        add to geocode service queue
      otherwise, try to see if shape file exists
    """
    __geocoder__ = DBTruckGeocoder(settings.GEOCACHEFILE)
    
    tablename = tablemd.tablename
    q = """select * from %s where _latlon is null and
           _address is not null and _geocoded = false
           order by id asc""" % tablename
    resproxy = db.execute(q)
    cols = resproxy.keys()

    user_input = None
    for anno in tablemd.annotations:
        if anno.loctype == Annotation.USERINPUT:
            user_input = anno.name

    maxid = None
    idx = 0

    while True:
        rows = resproxy.fetchmany(500)
        if not rows:
            break

        rows = [dict(zip(cols, row)) for row in rows]
        addresses = [row.get('_address', '') for row in rows]
        restrictions = [construct_restriction(user_input, row)
                        for row in rows]
        rids = [row['id'] for row in rows]
        geocodes = __geocoder__.geocode_block(zip(addresses, restrictions))

        for idx, result in enumerate(geocodes):
            rid = rids[idx] 
            if not result:
                continue

            description, (lat, lon), query = result
            print "geocoded\t", query, lat, lon            

            q = """update %s set _latlon = point(%%s, %%s), _query = %%s,
                  _description = %%s, _geocoded = true 
                  where id = %%s""" % tablename
            db.execute(q, [lat, lon, query, description, rid])
            maxid = rid if not maxid or rid > maxid else maxid

        # XXX: this is a huge hack to ensure that location join
        # module will re-compute the correlations because we
        # have more location information
        q = """delete from __dbtruck_corrpair__ where table1 =
        %s or table2 = %s"""
        db.execute(q, [tablename, tablename])

    return maxid

def construct_restriction(user_input, data):
    city = data['_city']
    state = data['_state']
    zipcode = data['_zipcode']

    if user_input:
        restriction = user_input
    else:
        restriction = []
        if city:
            restriction.append(city)
        if state:
            restriction.append(state)
        if zipcode:
            restriction.append(zipcode)
        restriction = ' , '.join(restriction)

    return restriction

    

def populate_shadow_cols(db, tablemd, schema):
    tablename = tablemd.tablename
    colnames = schema.columns.keys()

    arg = ','.join(colnames)
    resproxy = db.execute("""select %s from %s order by id asc""" % (arg, tablename))

    annotations = defaultdict(list)
    for anno in tablemd.annotations:
        annotations[anno.name].append((anno.loctype, anno.extractor()))

    def annotate_shadow(shadow_data, loctype, vals):
        for sd, v in zip(shadow_data, vals):
            sd[loctype] = v

    while True:
        rows = resproxy.fetchmany(2000)
        if not rows:
            break
        
        coldatas = zip(*rows)
        ids = None
        shadow_data = [dict() for row in rows]
        for cn, cd in zip(colnames, coldatas):
            cd = [ re_badchar.sub(' ', to_utf(v)).lower().strip() for v in cd if v]
            annos = annotations[cn]

            for loctype, extractor in annos:
                extracted = map(extractor, cd)
                if loctype == 'latlon':
                    lats, lons = zip(*extracted)

                    annotate_shadow(shadow_data, 'latitude', lats)
                    annotate_shadow(shadow_data, 'longitude', lons)
                else:
                    annotate_shadow(shadow_data, loctype, extracted)

            ids = cd if cn == 'id' else ids

        print 'saving', len(rows)
        save_shadow(db, tablename, ids, shadow_data)


    loctypes = set([anno.loctype for anno in tablemd.annotations])
    if ('latlon' in loctypes or
        ('latitude' in loctypes and 'longitude' in loctypes)):
        return False
    return True
        
def save_shadow(db, tablename, ids, shadow_data):
    attrs = ['address', 'city', 'state', 'country', 'query', 'description', 'shape', 'geocoded']
    for rid, sd in zip(ids, shadow_data):
        try:
            setvars = []
            if 'latitude' in sd and 'longitude' in sd:
                setvars.append( ('_latlon', 'point(%s, %s)', [sd['latitude'], sd['longitude']]) )
            else:
                for attr in attrs:
                    if attr in sd:
                        setvars.append(('_%s' % attr, '%s', [sd[attr]]))
            if not setvars:
                continue
            
            cols, placeholders, vals = zip(*setvars)
            vals = reduce(add, vals) + [rid]
            setval = ', '.join(['%s = %s' % (a,p) for a, p in zip(cols, placeholders)])
            q = "update %s set %s where id = %%s" % (tablename, setval)
            db.execute(q, vals)
        except Exception as e:
            raise
    

def find_location_extractors(db, tablemd, schema):
    tablename = tablemd.tablename
    colnames = schema.columns.keys()
    colnames = filter(lambda c: not c.startswith('_'), colnames)

    arg = ','.join(colnames)
    rows = db.execute("""select %s from %s
    order by id asc limit 2000""" % (arg, tablename)).fetchall()
    coldatas = zip(*rows)

    extractors = defaultdict(list)
    for colname, coldata in zip(colnames, coldatas):
        for loc_type, extractorname in possible_loc(colname, coldata).items():
            extractors[loc_type].append( (colname, extractorname) )

    return extractors

def add_user_annotation(tablemd, loctype, colname):
    tablemd.tablename
    for anno in tablemd.annotations:
        if anno.loctype == loctype:
            db_session.delete(anno)
    
    db_session.add(Annotation(colname, loctype, 'parse_default', tablemd, user_set=True))
    db_session.commit()

def add_constant_annotation(tablemd, loctype, colname):
    db_session.add(Annotation(colname, loctype, 'parse_default', tablemd, user_set=True))

if __name__ == '__main__':
    from database import db, db_session
    #drop_shadows(db)
    #delete_metadata(db)
    #reset_system(db)    
    Base = init_db()

    f = lambda t: t.startswith('ny')
    f = lambda t: t not in ['crime', 'expends', 'income', 'lottery',
                            'mass', 'parking']
    sleeptime = 10
    states = None
    while True:
        new_states = run_location_extractor(db, db_session, filter=f)
        print new_states, states
        if states != None:
            changed = False
            for s, ns in zip(states, new_states):
                if s != ns:
                    changed = True
                    break
            if not changed:
                sleeptime = min(10, sleeptime + 1)
            else:
                sleeptime = 0.5
        states = new_states
        time.sleep(sleeptime)
