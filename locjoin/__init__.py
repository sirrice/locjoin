import locjoin.meta

__version__ = '0.0.1'



def init_model(engine):
    try:
        meta.session.close()
    except:
        pass
    meta.sm.configure(bind=engine)
    meta.session = meta.sm()
    meta.engine = engine
    meta.metadata.bind = engine
