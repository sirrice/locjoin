from sqlalchemy import *
from sqlalchemy.orm import *
import locjoin.settings as settings


class LocalAddrGeocoder(object):
    ONEQ = '''select pprint_addy(g.addy), st_y(g.geomout) as lat, st_x(g.geomout) as lon, g.rating
            from geocode(:addr, 1) as g'''
    Q = '''select pprint_addy(g.addy), st_y(g.geomout) as lat, st_x(g.geomout) as lon, g.rating
            from geocode(:addr) as g'''


    
    def __init__(self):
        self.engine = create_engine(settings.GEOCODERDBURI)
        self.sm = sessionmaker(autoflush=True, bind=self.engine)
        self.session = self.sm()

    def geocode(self, addr, exactly_one=False):
        """
        return list of (description, latlon)
        """
        if exactly_one:
            q = LocalAddrGeocoder.ONEQ
        else:
            q = LocalAddrGeocoder.Q
            
        rows = self.session.execute(q, {'addr':addr}).fetchall()

        ret = []
        for descr, lat, lon, rating in rows:
            ret.append((descr, (lat,lon)))

        self.session.commit()

        return ret



if __name__ == '__main__':
    geocoder = LocalAddrGeocoder()

    for addr in ['529 Main Street, Boston MA, 02129',
                 '77 Massachusetts Avenue, Cambridge, MA 02139',
                 '25 Wizard of Oz, Walaford, KS 99912323',
                 '26 Capen Street, Medford, MA',
                 '124 Mount Auburn St, Cambridge, Massachusetts 02138',
                 '950 Main Street, Worcester, MA 01610']:
        print geocoder.geocode(addr)
