import time
import pickle

from dbtruck.dbtruck import import_datafiles
from dbtruck.exporters.pg import PGMethods

from locjoin.util import check_job_table
from locjoin.tasks import *
from locjoin.analyze.models import *
from locjoin.analyze.analyze import run_state_machine
from locjoin.analyze.fuzzyjoin import recompute_correlations, compute_pairwise_correlations



def main(db, session):
    while True:
        rows = session.execute('select * from __dbtruck_jobs__ limit 1').fetchall()
        for id, fname, argstxt, kwargstxt in rows:
            try:
                args = pickle.loads(str(argstxt))
            except Exception as e:
                args = []

            try:
                kwargs = pickle.loads(str(kwargstxt))
            except:
                kwargs = {}
            print fname
            print args
            print kwargs
            
            try:
                if fname == 'add_table':
                    _add_table(*args, **kwargs)
                elif fname == 'execute_state_machine':
                    _execute_state_machine(db, session, *args, **kwargs)
                elif fname == 'update_annotations':
                    _update_annotations(db, session, *args, **kwargs)
                elif fname == 'recompute_corr_pairs':
                    _recompute_corr_pairs(db, session, *args, **kwargs)

                print "deleting task", id
                session.execute('delete from __dbtruck_jobs__ where id = :id', {'id' : id})
                session.commit()
            except:
                import traceback
                traceback.print_exc()
                session.rollback()
                
        time.sleep(0.5)



def _add_table(url, name):
    try:
        import_datafiles([url], True, name, None,  PGMethods,
                         **settings.DBSETTINGS)
    except:
        import traceback
        traceback.print_exc()

def _execute_state_machine(db, db_session, tablename):
    run_state_machine(db, db_session, tablename)

    recompute_corr_pairs(db_session, tablename)

def _update_annotations(db, db_session, tablename, newannosargs):
    session = db_session
    tablemd = Metadata.load_from_tablename(db_session, tablename)

    newannos = []
    for name, ltype, extractor, annotype in newannosargs:
        anno = Annotation(name, ltype, extractor, tablemd,
                          annotype=annotype,
                          user_set=True)
        newannos.append(anno)

    tablemd.state = 0
    
    map(session.delete, tablemd.annotations)
    session.add_all(newannos)
    session.add(tablemd)
    session.commit()

    execute_state_machine(db_session, tablename)

def _recompute_corr_pairs(db, db_session, tablename):
    recompute_correlations(db, db_session, tablename)




if __name__ == '__main__':
    from locjoin.analyze.database import *
    init_db()
    check_job_table(db)
    main(db, db_session)
