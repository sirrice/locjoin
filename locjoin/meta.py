"""SQLAlchemy Metadata and Session object"""
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
 
__all__ = ['session', 'engine', 'metadata']
table_cache = {}
 
# SQLAlchemy database engine. Updated by model.init_model()
engine = None
 
# SQLAlchemy session manager. Updated by model.init_model()
sm = sessionmaker(autocommit=False,
                  autoflush=True)
session = sm()
 
# Global metadata. If you have multiple databases with overlapping table
# names, you'll need a metadata for each database
metadata = MetaData()
 
# declarative table definitions
Base = declarative_base(metadata=metadata)





def load_table_class(tablename):
    from locjoin.metadata.models import LocationMetadata as LMD
    from locjoin.shapes.models import ShapePtr 
    if tablename in table_cache:
        return table_cache

    class klass(Base):
        __table__ = Table(tablename,
                          metadata,
                          autoload=True)
        
    return klass
