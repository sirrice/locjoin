import time
import pickle

from dbtruck.dbtruck import import_datafiles
from dbtruck.exporters.pg import PGMethods

from locjoin.util import check_job_table
from locjoin.analyze.analyze import run_state_machine
from locjoin.analyze.fuzzyjoin import recompute_correlations, compute_pairwise_correlations



def main(db, session):
    while True:
        rows = session.execute('select * from __dbtruck_jobs__ limit 1').fetchall()
        for id, fname, argstxt, kwargstxt in rows:
            session.execute('delete from __dbtruck_jobs__ where id = :id', {'id' : id})
            session.commit()
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
                    add_table(*args, **kwargs)
                elif fname == 'execute_state_machine':
                    execute_state_machine(db, *args, **kwargs)
                elif fname == 'update_annotations':
                    update_annotations(db, session, *args, **kwargs)
                elif fname == 'recompute_corr_pairs':
                    recompute_corr_pairs(db, session, *args, **kwargs)
            except:
                import traceback
                traceback.print_exc()
                
        time.sleep(0.5)



def add_table(url, name):
    try:
        import_datafiles([url], True, name, None,  PGMethods,
                         **settings.DBSETTINGS)
    except:
        import traceback
        traceback.print_exc()

def execute_state_machine(db, tablename):
    run_state_machine(db, tablename)

    recompute_corr_pairs(db, tablename)

def update_annotations(db, db_session, tablename, newannotations):
    session = db_session
    tablemd = Metadata.load_from_tablename(db, tablename)

    tablemd.state = 0
    
    map(session.delete, tablemd.annotations)
    session.add_all(newannos)
    session.add(tablemd)
    session.commit()

    execute_state_machine(db, tablename)

def recompute_corr_pairs(db, db_session, tablename):
    recompute_correlations(db, db_session, tablename)




if __name__ == '__main__':
    from locjoin.analyze.database import *
    init_db()
    check_job_table(db)
    main(db, db_session)
