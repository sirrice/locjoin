import pickle
from sqlalchemy import *
from locjoin.runner import check_job_table


def add_job(db, jobname, args, kwargs):
    q = 'insert into __dbtruck_jobs__(fname, args, kwargs) values(%s, %s, %s)'
    args = pickle.dumps(args)
    kwargs = pickle.dumps(kwargs)
    db.execute(q, [jobname, args, kwargs])

def add_table(db, url, name):
    add_job(db, 'add_table', [url, name], {})

def execute_state_machine(db, tablename):
    add_job(db, 'execute_state_machine', [tablename], {})

def update_annotations(db, tablename, newannos):
    add_job(db, 'update_annotations', [tablename, newannos], {})

def recompute_corr_pairs(db, tablename):
    add_job(db, 'recompute_corr_pairs', [tablename], {})

if __name__ == '__main__':
    from locjoin.analyze.database import *
    check_job_table(db)
    #recompute_corr_pairs(db, 'underweight')
    add_table(db, 'http://www.health.ny.gov/statistics/chac/mortality/homici.htm', 'homicide')

