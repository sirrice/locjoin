import time
import pickle
import traceback

from multiprocessing import Process, Queue, Pool
from Queue import Empty
from dbtruck.dbtruck import import_datafiles
from dbtruck.exporters.pg import PGMethods

from locjoin.util import check_job_table
from locjoin.tasks import *
from locjoin.analyze.models import *
from locjoin.analyze.analyze import run_state_machine
from locjoin.analyze.fuzzyjoin import recompute_correlations, compute_pairwise_correlations



def main(db, session):
    npending = 0
    queue = Queue()
    timeout = 1
    process_limit = 5


    while True:

        # wait for results
        if npending > 0:
            try:
                success, task_id = queue.get(True, timeout)
                npending -= 1
                print "task completed", task_id, success
                if not success:
                    print "setting running = false"                    
                    session.execute('update __dbtruck_jobs__ set running = false where id = :id',
                                    {'id' : task_id})
                else:
                    print "setting done = true, running = false"
                    session.execute('update __dbtruck_jobs__ set done = true, running = false where id = :id',
                                    {'id' : task_id})
                session.commit()
            except Empty:
                time.sleep(0.5)
            except KeyboardInterrupt:
                raise
            except:
                import traceback
                traceback.print_exc()
        
        if npending > process_limit:
            continue

        q = """update __dbtruck_jobs__
               set running = true
               where id = (select min(dj2.id)
                           from __dbtruck_jobs__ as dj2
                           where dj2.running = false and done = false)
                returning id, fname, args, kwargs;"""
        rows = session.execute(q).fetchall()
        session.commit()
        for id, fname, args, kwargs in rows:
            try:
                args = pickle.loads(str(args))
            except Exception as e:
                args = []

            try:
                kwargs = pickle.loads(str(kwargs))
            except:
                kwargs = {}
            
            try:
                npending += 1
                p = Process(target=execute_function, args=(fname, args, kwargs, id, queue))
                p.start()
            except:
                import traceback
                traceback.print_exc()
                session.rollback()
                
        time.sleep(0.5)


def execute_function(fname, args, kwargs, task_id, queue):
    try:
        if fname == 'add_table':
            _add_table(*args, **kwargs)
        elif fname == 'execute_state_machine':
            _execute_state_machine(db, session, *args, **kwargs)
        elif fname == 'update_annotations':
            _update_annotations(db, session, *args, **kwargs)
        elif fname == 'recompute_corr_pairs':
            _recompute_corr_pairs(db, session, *args, **kwargs)
        elif fname == 'wait':
            print "wait", task_id
            time.sleep(5)
            print "wait done", task_id
        queue.put((True, task_id))
    except:
        queue.put((False, task_id))



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
