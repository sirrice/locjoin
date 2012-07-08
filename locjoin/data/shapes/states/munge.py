import shapefile
import sys
sys.path.append('..')

from sqlalchemy import *
from sqlalchemy.orm import *
from geoalchemy import *

from util import get_wkt

from locjoin.util import to_utf
from locjoin.settings import DBURI
from locjoin.shapes.models import *
from locjoin.shapes.util import clockwise

r = shapefile.Reader('./tl_2008_us_state')

db = create_engine(DBURI)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=True,
                                         bind=db))



for idx in xrange(r.numRecords):
    recshape = r.shapeRecord(idx)
    rec, shape = recshape.record, recshape.shape

    statefp, shortname, name = int(rec[0]), rec[2].lower(), rec[3].lower()
    shortname = to_utf(shortname)
    name = to_utf(name)
    print name

    shapeobj = Shape(get_wkt(shape))

    names = [StateName(fips=statefp, name=name),
             StateName(fips=statefp, name=shortname)]
        
    state = State(fips=statefp,
                   names=names,
                   state_shape=shapeobj)

    db_session.add_all(names)
    db_session.add_all([shapeobj, state])
    db_session.commit()

    if idx > 0 and idx % 50 == 0:
        db_session.commit()
db_session.commit()        



# states: fips, name, abbrv, shapesptr
# counties: fips, name, statefips, shapesptr
# zipcodes: 3 or 5 char fips, shapesptr


# multi: shapesptr -> shapeid
# shapeid -> shape


