from sqlalchemy import *
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import *
import locjoin.settings as settings

import psycopg2
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)



db = create_engine(settings.DBURI, isolation_level='READ COMMITTED')
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=True,
                                         bind=db))
Base = declarative_base()
Base.query = db_session.query_property()


def init_db():



    from locjoin.analyze.models import Metadata, Annotation, CorrelationPair
    Base.metadata.bind = db
    Base.metadata.create_all()
    Base.metadata.reflect()

    return Base


def new_db(autocommit=True):
    db = create_engine(settings.DBURI, isolation_level='READ COMMITTED')
    db_session = scoped_session(sessionmaker(autocommit=autocommit,
                                             autoflush=True,
                                             bind=db))
    Base = declarative_base()
    Base.query = db_session.query_property()

    from locjoin.analyze.models import Metadata, Annotation, CorrelationPair
    Base.metadata.bind = db
    Base.metadata.create_all()
    Base.metadata.reflect()

    return db, db_session



if __name__ == '__main__':
    init_db()
    from models import *    
    md = Metadata('testtable')
    db_session.add(md)
    db_session.commit()
    db_session.close_all()




    # examples of using engine directly (skip using db.py)
    print db.execute("select * from lottery limit 10").fetchall()
    print db.execute("insert into metadata(tablename, maxid) values(%s, %s)", ['t1', 99])
