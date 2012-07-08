import shapefile
from sqlalchemy import *
from sqlalchemy.orm import *
from geoalchemy import *
import sys
sys.path.append('..')

from util import get_wkt
from locjoin.settings import DBURI
from locjoin.shapes.models import *

r = shapefile.Reader('./tl_2008_us_zcta500')

db = create_engine(DBURI)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=True,
                                         bind=db))



    

print [f[0] for f in r.fields[1:]]
for idx in xrange(r.numRecords):
    recshape = r.shapeRecord(idx)
    rec, shape = recshape.record, recshape.shape
    fips = int(rec[0])
    print fips

    shp = get_wkt(shape)

    shapeobj = Shape(shp)
    zipcode = Zipcode(fips=fips,
                      zip_shape=shapeobj)
    
    db_session.add_all([shapeobj, zipcode])
    db_session.commit()

    if idx > 0 and idx % 500 == 0:
        db_session.commit()
db_session.commit()


