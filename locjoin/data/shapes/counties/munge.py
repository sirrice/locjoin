import shapefile
from sqlalchemy import *
from sqlalchemy.orm import *
from geoalchemy import *
import sys
sys.path.append('..')

from util import get_wkt
from locjoin.util import to_utf
from locjoin.settings import DBURI
from locjoin.shapes.models import *

r = shapefile.Reader('./tl_2008_us_county')

db = create_engine(DBURI)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=True,
                                         bind=db))

seen = set()

for idx in xrange(r.numRecords):
    recshape = r.shapeRecord(idx)

    rec, shape = recshape.record, recshape.shape

    statefp, countyfp, shortname, name = rec[0], rec[1], rec[4].lower(), rec[5].lower()
    countyfp = int(statefp + countyfp)
    statefp = int(statefp)
    shortname = to_utf(shortname)
    name = to_utf(name)
    print countyfp, '\t', name

    shp = get_wkt(shape)

    names = [CountyName(fips=statefp, name=name),
             CountyName(fips=statefp, name=shortname)]

    shapeobj = Shape(shp)
    county = County(fips=countyfp,
                    state_fips=statefp,
                    names=names,
                    county_shape=shapeobj)

    db_session.add_all(names)
    db_session.add_all([shapeobj, county])
    db_session.commit()



# states: fips, name, abbrv, shapesptr
# counties: fips, name, statefips, shapesptr
# zipcodes: 3 or 5 char fips, shapesptr


# multi: shapesptr -> shapeid
# shapeid -> shape


