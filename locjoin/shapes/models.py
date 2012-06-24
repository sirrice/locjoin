from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy import *

from locjoin.meta import Base

__all__ = ['Shape', 'ShapePtr', 'County', 'State', 'Zipcode', 'StateName', 'CountyName']



class ShapePtr(Base):
    __tablename__ = '__dbtruck_shapeptr__'

    id = Column(Integer, primary_key=True, index=True)
    shape_objs = relationship('Shape', backref="shape_ptr")
    states = relationship('State', backref="state_ptr")
    counties = relationship('County', backref='county_ptr')
    zipcodes = relationship('Zipcode', backref='zip_ptr')

    def __init__(self, shapes):
        self.shape_objs = [Shape(self, shape) for shape in shapes]

    @property
    def shapes(self):
        return [s.shape for s in self.shape_objs]

class Shape(Base):
    __tablename__ = '__dbtruck_shape__'

    id = Column(Integer, primary_key=True, index=True)
    ptrid = Column(Integer, ForeignKey('__dbtruck_shapeptr__.id'), index=True)
    shape = GeometryColumn(Polygon(2))

    def __init__(self, ptrobj, shape):
        self.ptrid = ptrobj.id
        self.shape = shape

GeometryDDL(Shape.__table__)    

class State(Base):
    __tablename__ = '__dbtruck_state__'

    fips = Column(Integer, primary_key=True, index=True)
    shapeptr_id = Column(Integer, ForeignKey('__dbtruck_shapeptr__.id'))
    counties = relationship('County', backref='state')
    names = relationship('StateName', backref='st')

class StateName(Base):
    __tablename__ = '__dbtruck_statename__'

    id = Column(Integer, primary_key=True, index=True)
    fips = Column(Integer, ForeignKey('__dbtruck_state__.fips'), index=True)
    name = Column(String)


class County(Base):
    __tablename__ = '__dbtruck_county__'

    # stores the 5 digit fips -- 3 char state + 2 char county
    fips = Column(Integer, primary_key=True)
    state_fips = Column(Integer, ForeignKey('__dbtruck_state__.fips'), nullable=True)
    shapeptr_id = Column(Integer, ForeignKey('__dbtruck_shapeptr__.id'))
    names = relationship('CountyName', backref='county')    

class CountyName(Base):
    __tablename__ = '__dbtruck_countyname__'

    id = Column(Integer, primary_key=True, index=True)
    fips = Column(Integer, ForeignKey('__dbtruck_county__.fips'), index=True)
    name = Column(String)

    

class Zipcode(Base):
    __tablename__ = '__dbtruck_zipcode__'

    # stores the 5 digit fips -- 3 char state + 2 char county
    fips = Column(Integer, primary_key=True)
    shapeptr_id = Column(Integer, ForeignKey('__dbtruck_shapeptr__.id'))

    

if __name__ == '__main__':
    db = create_engine('postgresql://localhost:5432/test')
    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=True,
                                             bind=db))

    klass = ShapePtr

    wkt_lake1 = "POLYGON((-81.3 37.2, -80.63 38.04, -80.02 37.49, -81.3 37.2))"
    lake1 = klass([WKTSpatialElement(wkt_lake1)])

    db_session.add(lake1)
    db_session.commit()








