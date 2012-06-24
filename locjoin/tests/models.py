from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy import *

from locjoin.meta import Base


class TestModel(Base):
    __tablename__ = '__dbtruck_test__'

    id = Column(Integer, primary_key=True)
    val = Column(Integer)
