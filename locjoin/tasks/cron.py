import pdb
import time
import pickle
import traceback


from multiprocessing import Process, Queue, Pool
from Queue import Empty
from dbtruck.dbtruck import import_datafiles
from dbtruck.exporters.pg import PGMethods

from locjoin.tasks.models import Task

class Job(object):
    def __init__(self, row):
        id, fname, args, kwargs = row
        self.id = id
        self.fname = fname
        
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

        self.args = args
        self.kwargs = kwargs

        self.task_id = None
        self.queue = Queue()
        self.process = None
        self.running = False

    def get_task_id(self):
        while True:
            try:

                result = self.queue.get(False)
                return result

            except Empty:
                time.sleep(0.0005)
                

    def __call__(self, parallelize=True):
        try:

            if parallelize:
                args=(self.fname, self.args, self.kwargs, self.id, self.queue)
                print "running", args

                self.running = True                
                self.process = Process(target=execute_function, args=args)
                self.process.start()

                self.task_id = self.get_task_id()
                
            else:
                self.running = True
                print "running serial", self.args
                execute_function(self.fname, self.args, self.kwargs, self.id, self.queue)
                self.task_id = self.get_task_id()
            print "task id!", self.task_id

        except KeyboardInterrupt:
            raise
        except:
            traceback.print_exc()

    def kill(self):
        self.process.terminate()


class Cron(object):
    def __init__(self, session, **kwargs):
        self.db = session.bind
        self.session = session
        self.processes = {}
        self.queue = Queue()
        self.timeout = 1
        self.process_limit = 5
        self.parallelize = True
        self.__dict__.update(kwargs)

        check_job_table(self.db)

    def __call__(self):
        try:
            self.main_runner()
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


    def reset_job_status(self, task_id):
        print "setting running = false"                    
        self.session.execute('update __dbtruck_jobs__ set running = false where id = :id',
                             {'id' : task_id})
        self.session.commit()

    def set_job_complete_status(self, task_id):
        print "setting done = true, running = false"        
        self.session.execute('update __dbtruck_jobs__ set done = true, running = false where id = :id',
                             {'id' : task_id})
        self.session.commit()

    def get_next_jobs(self):
        try:
            q = """update __dbtruck_jobs__
                   set running = true,
                   lastrun = current_timestamp
                   where id = (select dj2.id
                               from __dbtruck_jobs__ as dj2
                               where dj2.running = false and done = false
                               order by lastrun asc limit 1)
                    returning id, fname, args, kwargs;"""
            rows = self.session.execute(q).fetchall()
            self.session.commit()
            return map(Job, rows)
        except Exception as e:
            self.session.rollback()
            print e
            time.sleep(0.1)
            return []

    def check_finished_jobs(self):
        if len(self.processes) == 0:
            return

        finished = []
        for jid, job in self.processes.iteritems():
            try:
                success, proc_id, err = job.queue.get(False)

                if not success:
                    self.reset_job_status(proc_id)
                    print err                
                else:
                    self.set_job_complete_status(proc_id)

                finished.append(proc_id)

            except Empty:
                continue


        ret = []
        for proc_id in finished:
            proc = self.processes[proc_id]
            ret.append(proc)
            del self.processes[proc_id]
            self.delete_task_row(proc.task_id)
        return ret

    def cancel_job(self, job):
        print "cancelling", job
        cancellable_jobs = ['run_extractor',
                            'run_detector',
                            'run_pipeline',
                            'wait']
        table = job.args[0]

        rmids = []
        for procid, proc in self.processes.iteritems():
            if proc.fname in cancellable_jobs:
                if proc.args[0] == table and proc.running:
                    rmids.append(procid)

        for rmid in rmids:
            try:
                proc = self.processes[rmid]
                proc.kill()
                del self.processes[rmid]
                self.delete_task_row(proc.task_id)
                self.set_job_complete_status(proc.id)
            except Exception as e:
                traceback.print_exc()

        self.set_job_complete_status(job.id)                

    def delete_task_row(self, task_id):
        if not task_id:
            return
        try:
            task = self.session.query(Task).get(task_id)
            if not task:
                return
            self.session.merge(task)
            self.session.delete(task)
            self.session.commit()
        except:
            traceback.print_exc()


    def main_runner(self):

        while True:
            finished = self.check_finished_jobs()
            if len(self.processes) >= self.process_limit:
                time.sleep(0.01)
                break
            

            jobs = self.get_next_jobs()
            nran = 0
            
            for job in jobs:
                if 'cancel' == job.fname:
                    self.cancel_job(job)
                    nran += 1                    
                    continue
                
                if len(self.processes) >= self.process_limit:
                    break

                self.processes[job.id] = job
                job(parallelize=self.parallelize)

                nran += 1

            
            if not nran:
                if not jobs:
                    time.sleep(2)
                else:
                    print "no slots left"
                    time.sleep(0.01)






def execute_function(fname, args, kwargs, job_id, queue):
    from sqlalchemy import create_engine

    import locjoin.settings as settings
    import locjoin.meta as meta
    from locjoin import init_model

    db = create_engine(settings.DBURI, isolation_level='REPEATABLE READ')
    init_model(db)

    try:
        strargs = pickle.dumps([args, kwargs])
        task = Task.check_and_add(session, fname, strargs)
        if not task:
            raise
    except:
        traceback.print_exc()
        queue.put(None)
        queue.put((False, job, traceback.format_exc()))
        return
        
    tid = task.id
    queue.put(tid)

    try:

        if fname == 'wait':
            print "wait", job_id
            time.sleep(args[0])
            print "wait done", job_id
        elif fname == 'crash':
            time.sleep(2)
            raise RuntimeError("crashed")
        else:
            cmd = '_%s(db, meta.session, *args, **kwargs)' % fname
            exec(cmd)

        queue.put((True, job_id, ''))

    except:
        err = traceback.format_exc()
        queue.put((False, job_id, err))

    finally:
        session.close()
        db.pool.dispose()


    Task.delete(session, tid)


def _add_table(session, url, name):
    try:
        import_datafiles([url], True, name, None,  PGMethods,
                         **settings.DBSETTINGS)
    except:
        traceback.print_exc()

    execute_state_machine(db_session, name)



def _run_pipeline(db, session, tablename):
    _run_detector(db, session, tablename)
    _run_extractor(db, session, tablename)


def _run_detector(db, session, tablename):
    from locjoin.metadata.metadata import DummyMetadataDetector, update_metadata    
    detector = DummyMetadataDetector(session)
    lmds = detector(tablename)
    update_metadata(session, tablename, lmds)


        
def _run_extractor(db, session, tablename):
    from locjoin.extractor.extractor import Extractor    
    extractor = Extractor(session)
    extractor(tablename)

    

def _recompute_corr_pairs(db, db_session, tablename):
    recompute_correlations(db, db_session, tablename)



def _test_insert(db, session, val):
    from locjoin.tests.models import TestModel    
    print "inserting", val
    session.add(TestModel(val=val))
    session.commit()


def _test_inc(db, session, inc_val):
    from locjoin.tests.models import TestModel    
    print "inc by ", inc_val
    models = session.query(TestModel).all()
    for m in models:
        m.val += inc_val
    session.add_all(models)
    session.commit()



def test_cron(session):
    import locjoin.tasks.tasks as tasks
    tasks.wait(session, 3)
    tasks.wait(session, 2)
    tasks.cancel_tasks(session,3)
    tasks.cancel_tasks(session,2)
    
    

if __name__ == '__main__':
    from sqlalchemy import *
    from sqlalchemy.orm import *
    from locjoin.tasks.util import check_job_table
    import locjoin.settings as settings
    db = create_engine(settings.DBURI, isolation_level='REPEATABLE READ')
    
    sm = sessionmaker(autocommit=False,
                      autoflush=True,
                      bind=db)
    session = sm()

    session.execute('''update __dbtruck_jobs__
                       set running=false
                       where running=true and done=false''')
    session.execute('delete from __dbtruck_tasks__')
    session.commit()

    
    cron = Cron(session, process_limit=10, parallelize=False)
    cron()
