from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy import *

from locjoin.settings import DBURI
from locjoin.shapes.models import *
from locjoin.geocoder.models import *
from locjoin.metadata.models import *
from locjoin.tests.models import *
from locjoin.tasks.models import *
import locjoin.meta as meta
from locjoin import init_model




if __name__ == '__main__':
    import sys
    import pdb

    reset = False
    if 'reset' in sys.argv:
        q = 'really reset database?  Y continues, everything else aborts > '
        reset = raw_input(q) == 'Y'
        
    klasses = [Shape,
               State, StateName,
               County, CountyName,
               Zipcode, GeocoderTable,
               LocationMetadata, TestModel,
               Task]

    
    db = create_engine(DBURI)
    init_model(db)

    metadata = meta.metadata
    Task.metadata.bind = db

    if reset:
        print "dropping tables"
        metadata.drop_all()
        Task.metadata.drop_all()
        
    print "creating tables"
    metadata.create_all()
    Task.metadata.create_all()
    
