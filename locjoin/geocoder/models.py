from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy import *

from locjoin.metadata.models import *
from locjoin.meta import Base

__all__ = ['GeocoderTable']

class GeocoderTable(Base):
    __tablename__ = '__dbtruck_geocoder__'

    id = Column(Integer, primary_key=True, index=True)
    locid = Column(Integer,
                   ForeignKey('%s.id' %  LocationMetadata.__tablename__),
                   index=True)
    loc = relationship(LocationMetadata,
                       primaryjoin=locid == LocationMetadata.id,
                       backref=backref('geocoderows', order_by=id))

    # True if max_mat == max(locid.table.id)
    materialized = Column(Boolean, default=False)
    max_materialized = Column(Integer)

