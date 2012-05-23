import time
import pickle
import traceback
import locjoin.settings as settings

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
    processes = {}    
    try:
        main_runner(db, session, processes)
    except KeyboardInterrupt:
        print "ctl-c detected.  killing processes"

        # for tid, p in processes.items():
        #     try:
        #         if p.is_alive():
        #             p.terminate()
        #     except:
        #         traceback.print_exc()
    except:
        traceback.print_exc()
    print "goodbye"
                
            

def main_runner(db, session, processes):
    npending = 0
    queue = Queue()
    timeout = 1
    process_limit = 5


    while True:

        # wait for results
        if npending > 0:
            try:
                success, task_id, err = queue.get(False)
                npending -= 1
                print "task completed", task_id, success
                del processes[task_id]
                if not success:
                    print err
                    print "setting running = false"                    
                    session.execute('update __dbtruck_jobs__ set running = false where id = :id',
                                    {'id' : task_id})
                else:
                    print "setting done = true, running = false"
                    session.execute('update __dbtruck_jobs__ set done = true, running = false where id = :id',
                                    {'id' : task_id})
                session.commit()
            except Empty:
                time.sleep(0.05)

        if npending > process_limit:
            continue

        try:
            q = """update __dbtruck_jobs__
                   set running = true,
                   id = (select max(dj3.id)+1 from __dbtruck_jobs__ as dj3)
                   where id = (select min(dj2.id)
                               from __dbtruck_jobs__ as dj2
                               where dj2.running = false and done = false)
                    returning id, fname, args, kwargs;"""
            rows = session.execute(q).fetchall()
            session.commit()
        except:
            time.sleep(0.1)
            continue

        for id, fname, args, kwargs in rows:
            try:
                args = pickle.loads(str(args))
            except KeyboardInterrupt:
                raise
            except Exception as e:
                args = []

            try:
                kwargs = pickle.loads(str(kwargs))
            except KeyboardInterrupt:
                raise
            except:
                kwargs = {}
            
            try:
                npending += 1
                processes[id] = id
                execute_function(db, session, fname, args, kwargs, id, queue)
                #p = Process(target=execute_function, args=(fname, args, kwargs, id, queue))
                #processes[id] = p
                #p.start()                
            except KeyboardInterrupt:
                raise
            except:
                traceback.print_exc()
                
        time.sleep(0.5)


def execute_function(db, session, fname, args, kwargs, task_id, queue):
    # from sqlalchemy import *
    # import locjoin.settings as settings
    # db = create_engine(settings.DBURI, isolation_level='serializable')
    # db_session = sessionmaker(autocommit=False,
    #                           autoflush=True,
    #                           bind=db)
    #from locjoin.analyze.database import new_db
    #db, session = new_db(False)
    try:
        if fname == 'add_table':
            _add_table(session, *args, **kwargs)
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
        queue.put((True, task_id, ''))
    except:
        import traceback
        err = traceback.format_exc()
        queue.put((False, task_id, err))



def _add_table(session, url, name):
    try:
        import_datafiles([url], True, name, None,  PGMethods,
                         **settings.DBSETTINGS)
    except:
        import traceback
        traceback.print_exc()

    execute_state_machine(db_session, name)

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

    execute_state_machine(db_session, tablename)

def _recompute_corr_pairs(db, db_session, tablename):
    recompute_correlations(db, db_session, tablename)




if __name__ == '__main__':
    
    from locjoin.analyze.database import *
    #db, db_session = new_db()
    init_db()
    check_job_table(db)

    db_session.execute('update __dbtruck_jobs__ set running = false where done = false')
    db_session.commit()
    
    main(db, db_session)
