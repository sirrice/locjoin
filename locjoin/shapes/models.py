from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy import *

from locjoin.meta import Base

__all__ = ['Shape', 'County', 'State', 'Zipcode', 'StateName', 'CountyName']



class Shape(Base):
    __tablename__ = '__dbtruck_shape__'

    id = Column(Integer, primary_key=True, index=True)
    shape = GeometryColumn(MultiPolygon(2))
    states = relationship('State', backref="state_shape")
    counties = relationship('County', backref='county_shape')
    zipcodes = relationship('Zipcode', backref='zip_shape')
    

    def __init__(self, shape):
        self.shape = shape

GeometryDDL(Shape.__table__)    

class State(Base):
    __tablename__ = '__dbtruck_state__'

    fips = Column(Integer, primary_key=True, index=True)
    shape_id = Column(Integer, ForeignKey('__dbtruck_shape__.id'))
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
    shape_id = Column(Integer, ForeignKey('__dbtruck_shape__.id'))
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
    shape_id = Column(Integer, ForeignKey('__dbtruck_shape__.id'))

    

if __name__ == '__main__':
    db = create_engine('postgresql://localhost:5432/test')
    db_session = scoped_session(sessionmaker(autocommit=False,
                                             autoflush=True,
                                             bind=db))

    klass = Shape

    wkt_lake1 = "MULTIPOLYGON(((-81.3 37.2, -80.63 38.04, -80.02 37.49, -81.3 37.2)))"
    lake1 = klass(WKTSpatialElement(wkt_lake1))

    db_session.add(lake1)
    db_session.commit()








