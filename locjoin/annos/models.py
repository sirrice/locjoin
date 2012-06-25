import pdb

from sqlalchemy import *
from sqlalchemy.orm import *
from geoalchemy import *

from locjoin.metadata.models import LocationMetadata as LMD
from locjoin.shapes.models import Shape
from locjoin.settings import DBURI
import locjoin.meta as meta
from locjoin import init_model



def get_table_annotation(tablename):
    annoname = '%s__annotation__' % tablename
    
    if annoname in meta.table_cache:
        return meta.table_cache[annoname]


    if annoname in meta.metadata.tables:
        class TableAnnotation(meta.Base):
            __table__ = meta.metadata.tables[annoname]
        GeometryDDL(TableAnnotation.__table__)  

    else:

        class TableAnnotation(meta.Base):
            __tablename__ = annoname

            id = Column(Integer, primary_key=True)
            rid = Column(Integer) # id of row in base table
            locid = Column(Integer,
                           #ForeignKey('%s.id' % LMD.__tablename__),
                           index=True)
            # lmd = relationship('LocationMetadata',
            #                    primaryjoin='LocationMetadata.id==TableAnnotation.locid')
            def lmds(self, session):
                return session.query(LMD).get(self.locid)

            

            latlon = GeometryColumn(Point(2), nullable=True)
            shape_id = Column(Integer,
                              ForeignKey('%s.id' % Shape.__tablename__),
                              nullable=True)

            @property
            def shape(self):
                return object_session(self).query(Shape).get(self.shape_id)

                
                
        GeometryDDL(TableAnnotation.__table__)
        print "trying to create anno table\t%s" % tablename
        meta.session.close()
        TableAnnotation.__table__.create(bind=meta.metadata.bind, checkfirst=True)
        print "done"
        
    meta.table_cache[annoname] = TableAnnotation
    return TableAnnotation


    

if __name__ == '__main__':
    import sys


    reset = False
    if 'reset' in sys.argv:
        q = 'really reset database?  Y continues, everything else aborts > '
        reset = raw_input(q) == 'Y'
        
    db = create_engine(DBURI)
    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=True,
                                             bind=db))
    init_model(db)

    for i in xrange(10):
        print get_table_annotation('foobar')
    
    print meta.metadata.tables.keys()
    pdb.set_trace()
