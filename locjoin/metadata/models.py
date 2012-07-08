import re

from sqlalchemy import *
from sqlalchemy.orm import *


from locjoin.meta import Base


__refmt__ = re.compile('\{[\w\_]+\}')

class LocType(object):
    COUNTRY = 0
    STATE = 3
    ZIPCODE = 4
    COUNTY = 6
    CITY = 9
    DISTRICT = 12
    ADDR = 15
    LATLON = 18
    CUSTOM = 100

    @staticmethod
    def to_str(loctype):
        if loctype == LocType.COUNTRY:
            return 'Country'
        if loctype == LocType.STATE:
            return 'State'
        if loctype == LocType.ZIPCODE:
            return 'Zipcode'
        if loctype == LocType.COUNTY:
            return 'County'
        if loctype == LocType.CITY:
            return 'City'
        if loctype == LocType.DISTRICT:
            return 'District'
        if loctype == LocType.ADDR:
            return 'Address'
        if loctype == LocType.LATLON:
            return 'LatLon'
        return 'Custom'

class ExtractType(object):
    """
    whethr the extraction type is a user specified, parsable format, or
    a pickled python function callable
    """
    FMT = 0
    PY = 2


class LocSource(object):
    """
    keep track of whether this location metadata was autogenerated or specified by user
    (user takes precedance)
    """
    AUTO = 0
    USER = 10
    

class LocationMetadata(Base):
    __tablename__ = '__dbtruck_loc_metadata__'


    id = Column(Integer, primary_key=True)
    tname = Column(String, nullable=False)
    col_name = Column(String, nullable=False)
    
    loc_type = Column(Integer, nullable=False)
    extract_type = Column(Integer, nullable=False)
    
    fmt = Column(String)
    source = Column(Integer, default=LocSource.AUTO)
    deleted = Column(Boolean, default=False)  # let's never actually delete

    @property
    def is_shape(self):
        return self.loc_type not in [LocType.LATLON, LocType.ADDR]

    @property
    def is_point(self):
        return not self.is_shape

    def __str__(self):
        return self.col_name

    def parse_format(self):
        if self.extract_type != ExtractType.FMT:
            return None, None

        fmt_str = __refmt__.sub('%s', self.fmt)
        cols = [s.strip('{}') for s in __refmt__.findall(self.fmt)]
        return cols, fmt_str

    def extract(self, row_dict):
        """
        @param row_dict dictionary of column_name -> value
        """
        cols, fmt_str = self.parse_format()

        if self.extract_type == ExtractType.FMT:
            vals = []
            for col in cols:
                if col not in row_dict:
                    return None
                vals.append(row_dict[col])
            address = fmt_str % tuple(vals)
        else:
            raise

        return address

    @staticmethod
    def current(session):
        q = session.query(LocationMetadata)
        return q.filter(LocationMetadata.deleted == False)