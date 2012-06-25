
import locjoin.meta as meta
from geoalchemy import *

from sqlalchemy.sql.expression import asc

from locjoin.annos.models import *
from locjoin.metadata.models import *
from locjoin.metadata.models import LocationMetadata as LMD
from locjoin.geocoder.geocoder import *
from locjoin.tasks.models import Task



def geocoder_klass_from_loctype(lmd):
    loc_type = lmd.loc_type
    if loc_type == LocType.STATE:
        return StateGeocoder
    elif loc_type == LocType.COUNTY:
        return CountyGeocoder
    elif loc_type == LocType.ZIPCODE:
        return ZipcodeGeoder
    elif loc_type == LocType.LATLON:
        return LatLonGeocoder
    elif loc_type == LocType.ADDR:
        return AddressGeocoder
    return None

class LocExtractor(object):
    def __init__(self, session, lmd, annoklass):
        self.session = session
        self.lmd = lmd
        self.annoklass = annoklass

    def get_or_create(self, rid):
        try:
            q = self.session.query(self.annoklass)
            q = q.filter(self.annoklass.rid==rid,
                         self.annoklass.locid==self.lmd.id)
            return q.one()
        except:
            return self.annoklass(rid=rid, locid=self.lmd.id)
        finally:
            self.session.commit()
        

    def __call__(self):
        klass = geocoder_klass_from_loctype(self.lmd)
        if not klass:
            raise
        
        geocoder = klass(self.session)


        for rid, (loctype, data) in geocoder(self.lmd):
            anno = self.get_or_create(rid)
            
            if loctype == LATLON:
                pt = 'POINT(%f %f)' % tuple(data)
                anno.latlon = WKTSpatialElement(pt)

            elif loctype == SHAPE:
                anno.shape_id = data

            if anno:
                # should I wrap in try catch?
                self.session.add(anno)
                self.session.commit()


class Extractor(object):
    
    def __init__(self, session):
        self.session = session

    def __call__(self, tablename, lmd_ids=None):
        """
        look up all LocationMetaData objects relating to this table and
        call the appropriate geocoders for them
        """

        if lmd_ids is not None:
            lmds = self.session.query(LMD).filter(LMD.id.in_(lmd_ids)).all()
        else:
            q = LMD.current(meta.session)
            q = q.filter(LMD.tname == tablename)
            q = q.order_by(asc(LMD.loc_type))
            lmds = q.all()
            
        print "extracting from ", map(str,lmds)
        annoklass = get_table_annotation(tablename)
        
        for lmd in lmds:
            extractor = LocExtractor(self.session, lmd, annoklass)
            extractor()


if __name__ == '__main__':
    from locjoin.settings import DBURI
    import sys
    import pdb

    db = create_engine(DBURI)
    init_model(db)

    extractor = Extractor(meta.session)
    extractor('foobar')
