import pickle
from sqlalchemy import *
from locjoin.util import check_job_table


def add_job(session, jobname, args, kwargs):
    q = """insert into __dbtruck_jobs__(fname, args, kwargs)
            values(:a1, :a2, :a3)"""
    args = pickle.dumps(args)
    kwargs = pickle.dumps(kwargs)
    session.execute(q, {'a1':jobname, 'a2':args, 'a3':kwargs})
    session.commit()

def add_table(session, url, name):
    add_job(session, 'add_table', [url, name], {})

def execute_state_machine(session, tablename):
    add_job(session, 'execute_state_machine', [tablename], {})

def update_annotations(session, tablename, newannosargs):
    add_job(session, 'update_annotations', [tablename, newannosargs], {})

def recompute_corr_pairs(session, tablename):
    add_job(session, 'recompute_corr_pairs', [tablename], {})

def wait(session):
    add_job(session, 'wait', [], {})

if __name__ == '__main__':
    from locjoin.analyze.database import *
    check_job_table(db)
    recompute_corr_pairs(db_session, 'urbanpop')
    #update_annotations(db_session, *args)
    #for i in xrange(10):
    #    wait(db_session)
