"""
Geocoder classes

Each geocoder class takes as input an iterable of well formed (see 1.) strings
and returns an iterable of location information or None.

The iterable returns elements of the form
  record_id, DATA

where DATA may be tuples of either
  (LATLON, (lat, lon))
  (SHAPE, shape_id)

1. well formed means the geocoder can blindly assume it is in the expected format and is allowed to
   return None if the format is incorrect.
   E.g., the county geocoder expects values of the form: "county name, state name"
   

"""
import re
import pickle
import time
from sqlalchemy import *
from sqlalchemy.orm import *
from geopy import geocoders

from locjoin.util import to_utf
from locjoin.settings import GEOCACHEFILE
from locjoin.geocoder.models import GeocoderTable
from locjoin.shapes.models import *


re_addr2 = re.compile(r'^\s*\d{1,6}(\s+[a-zA-Z\-]+){1,3}\s*$')

LATLON = 0
SHAPE = 1


class Geocoder(object):
    def __init__(self, session, **kwargs):
        self.session = session


    def close(self):
        pass

    def __call__(self, loc):
        """
        get the geocodertable record
        check to see if we are done materializing
        if not, start from max materialized

        @param loc location record
        @return iterator of (row_id, (loctype, latlon or shapeid))
        """

        try:
            georow = self.session.query(GeocoderTable).filter(GeocoderTable.locid == loc.id).one()
        except:
            # construt new record
            georow = GeocoderTable(loc=loc, max_materialized=0)
            #georow = self.session.merge(georow)
            self.session.add(georow)
            self.session.commit()

        if not georow.materialized:
            return self.geocode_all(georow)

    def geocode_all(self, georow):
        maxid = georow.max_materialized
        q = "select * from %s where id > %%s" % georow.loc.tname
        resproxy = self.session.bind.execute(q, (maxid,))
        cols = resproxy.keys()

        while True:
            rows = resproxy.fetchmany(100)
            if not rows:
                break
            
            rows = [dict(zip(cols, row)) for row in rows]
            addrs = map(georow.loc.extract, rows)
            rids = (row['id'] for row in rows)
            geocodes = self.geocode_batch(addrs)

            for rid, result in zip(rids, geocodes):
                if not result:
                    continue
                
                yield rid, result

                georow.max_materialized = rid
                self.session.add(georow)
                self.session.commit()

    def geocode_batch(self, addrs):
        """
        @return iterator of (loctype, latlon or shapeid)
        """
        raise
        



class AddressGeocoder(Geocoder):
    def __init__(self, *args, **kwargs):
        """
        @fname cache file name
        """
        Geocoder.__init__(self, *args, **kwargs)
        self.MINDELAY = 0.01
        
        fname = GEOCACHEFILE
        try:
            self.cache = bsddb3.hashopen(fname)
        except:
            self.cache = {}
        try:
            self.ncalls = int(self.cache.get('__ncalls__', '0'))
        except:
            self.ncalls = 0
    
    def close(self):
        try:
            self.cache.close()
        except:
            pass

    def geocode_batch(self, addresses):
        delay = self.MINDELAY
        for addr in addresses:
            ncallsprev = self.ncalls            

            try:
                locs = self.geocode(addr)
                if locs:
                    description, latlon = locs[0]
                    yield LATLON, latlon

                delay = max(self.MINDELAY, delay-0.0001)
            except Exception as e:
                e = str(e).lower()
                if 'limit' in e or 'rate' in e:
                    delay *= 1.1
                    yield None
                else:
                    raise

            ncallsmade = self.ncalls - ncallsprev
            if ncallsmade:
                time.sleep(delay)


    def get_initial_geocoder(self, address):
        if re_addr2.search(address):
            rand = random.random()

            if rand < 0.5:
                geocoder = geocoders.Yahoo(settings.YAHOO_APPID)
            else:
                geocoder = geocoders.Bing(settings.BING_APIKEY)

        else:
            geocoder = geocoders.GeoNames()

        return geocoder


        
    def geocode(self, address):
        """
        Try to pick the best geocoder for the address.  Google has a low
        daily limit so try to use other geocoders if possible

        If the address looks like a standard address (NUMBER WORDS+),
        then use yahoo, bing, geocoder.us

        Otherwise if it is a place name, try geonames

        If all else fails, use Google
        """
        query = to_utf(address).lower()

        if not query:
            return []

        if query in self.cache:
            try:
                ret = pickle.loads(self.cache[query])
                if ret and isinstance(ret[0], basestring):
                    return [ret]
                return ret
            except KeyboardInterrupt:
                raise
            except:
                pass


        
        geocoder = self.get_initial_geocoder(address)


        try:
            result = geocoder.geocode(query, exactly_one=False)
            self.ncalls += 1
            if not result:
               raise Exception('no result found for %s' % query)
        except Exception as e:
            
            try:
                geocoder = geocoders.Google()
                result = geocoder.geocode(query, exactly_one=False)
                self.ncalls += 1
            except Exception as e:
                print e
                result = []


        self.cache[query] = pickle.dumps(result)

        if result and isinstance(result[0], basestring):
            result = [result]
            
        return result


class ZipcodeGeoder(Geocoder):
    def geocode_batch(self, vals):
        """
        lookup in in-memory or bdb store for matching zip codes

        @return iterator of (loctype, latlon or shapeid)
        """

        for val in vals:
            try:
                zipcode = self.session.query(Zipcode).filter(Zipcode.fips == int(val)).one()
                yield SHAPE, zipcode.shapeptr_id
            except:
                yield None
        

class LatLonGeocoder(Geocoder):
    def geocode_batch(self, vals):
        """
        @return iterator of (loctype, latlon or shapeid)
        """

        for val in vals:
            try:
                yield None
                #yield LATLON, None
            except:
                yield None



class StateGeocoder(Geocoder):
    def geocode_batch(self, vals):
        """
        @return iterator of (loctype, latlon or shapeid)
        """
        Q = """select shapeptr_id
               from __dbtruck_state__ s,  __dbtruck_statename__ sn
               where s.fips = sn.fips and
               levenshtein(name, %s) < 1 + 0.2*char_length(name)
               order by levenshtein(name, %s) limit 1"""
        for v in vals:
            try:
                v = to_utf(v.lower())
                shapeids = self.session.bind.execute(Q, (v,v)).fetchone()
                yield SHAPE, shapeids[0]
            except Exception as e:
                print e
                yield None
        

class CountyGeocoder(Geocoder):
    def geocode_batch(self, vals):
        """
        @param vals an iterator of "county name, state name" strings
        @return iterator of (loctype, latlon or shapeid)
        """
        Q = """
        select c.shapeptr_id
        from __dbtruck_county__ as c, __dbtruck_state__ as s,
             __dbtruck_countyname__ as cn, __dbtruck_statename__ as sn
        where c.fips = cn.fips and s.fips = sn.fips and c.state_fips = s.fips and
              levenshtein(cn.name, %s) < 1 + 0.2*char_length(cn.name) and
              levenshtein(sn.name, %s) < 1 + 0.2*char_length(sn.name)
        order by levenshtein(cn.name, %s) + levenshtein(sn.name, %s) asc
        limit 1
        """

        for v in vals:
            try:
                v = to_utf(v.lower())
                idx = v.rindex(',')
                c, s =  v[:idx], v[idx:] # county, state

                args = (c, s, c, s)
                shapeids = self.session.bind.execute(Q, args).fetchone()
                if not shapeids:
                    yield None
                else:
                    yield SHAPE, shapeids[0]
            except Exception as e:
                print e
                yield None
            



if __name__ == '__main__':
    from locjoin.settings import DBURI
    
    db = create_engine(DBURI)
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=True,
                                          bind=db))

    geo = AddressGeocoder(session)
    for res in geo.geocode_batch(['32 vassar st, cambridge, MA',
                                  '1620 norvell st, El cerrito, CA',
                                  'sf, CA',
                                  'san francisco']):
        print res
        
    
    geo = StateGeocoder(session)
    for res in geo.geocode_batch(['massachusettes']):
        print res


    geo = CountyGeocoder(session)
    for res in geo.geocode_batch(['essex,MA']):
        print res
