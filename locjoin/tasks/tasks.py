"""
add tasks to task manager

task manager will keep track and ignore duplicate jobs
(based on job name and arguments)
"""
import pickle
from sqlalchemy import *

from locjoin.tasks.util import check_job_table



def add_job(session, jobname, args, kwargs={}):
    isolevel = session.bind.raw_connection().connection.isolation_level
    if isolevel != 3:
        raise RuntimeError("db connection has improper isolation level: %d" % isolevel)
    check_job_table(session.bind)


    args = pickle.dumps(args)
    kwargs = pickle.dumps(kwargs)
    qargs = {'a1':jobname, 'a2':args, 'a3':kwargs}

    q = """select count(*) from __dbtruck_jobs__
    where fname = :a1 and args = :a2 and kwargs = :a3 and
          (running = true or done = false)"""
    count = session.execute(q, qargs).fetchone()[0]
    if count:
        session.rollback()
        raise RuntimeError("duplicate job exists")

    q = """insert into __dbtruck_jobs__(fname, args, kwargs)
            values(:a1, :a2, :a3)"""
    session.execute(q, qargs)
    session.commit()

def cancel_tasks(session, table):
    add_job(session, 'cancel', [table])

def add_table(session, url, name):
    add_job(session, 'add_table', [url, name], {})

def run_pipeline(session, table):
    add_job(session, 'run_pipeline', [table])    

def run_detector(session, table):
    add_job(session, 'run_detector', [table])

def run_extractor(session, table):
    add_job(session, 'run_extractor', [table])

def recompute_corr_pairs(session, tablename):
    add_job(session, 'recompute_corr_pairs', [tablename], {})

def wait(session, secs=10):
    add_job(session, 'wait', [secs], {})

def crash(session, *args):
    add_job(session, 'crash', args, {})

def test_insert(session, v):
    add_job(session, 'test_insert', [v], {})

def test_inc(session, v):
    add_job(session, 'test_inc', [v], {})


if __name__ == '__main__':
    import locjoin.settings as settings
    from locjoin import init_model
    import locjoin.meta as meta

    db = create_engine(settings.DBURI, isolation_level='REPEATABLE READ')
    init_model(db)

    check_job_table(db)

    run_extractor(meta.session, 'realestate_small')
    #for i in xrange(11, 50, 1):
    #    test_inc(meta.session, i)
