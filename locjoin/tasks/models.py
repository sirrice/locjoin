import time

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy import *

Base = declarative_base()

class Task(Base):
    __tablename__ = '__dbtruck_tasks__'

    id = Column(Integer, primary_key=True)
    taskname = Column(String)
    args = Column(String)

    @staticmethod
    def exists(session, taskname, strargs):
        q = session.query(Task)
        q = q.filter(Task.taskname==taskname,
                     Task.args==strargs)
        return q.count() > 0

    @staticmethod
    def check_and_add(session, taskname, strargs):
        try:
            if Task.exists(session, taskname, strargs):
                raise

            task = Task(taskname=taskname,
                        args=strargs)
            session.add(task)
            session.commit()
            return task
        except:
            import traceback
            traceback.print_exc()
            session.rollback()
            return None


    @staticmethod
    def delete(session, taskid):
        while True:
            task = session.query(Task).get(taskid)
            try:
                session.delete(task)
                session.commit()
                return True
            except:
                import traceback
                traceback.print_exc()
                session.rollback()
                time.sleep(0.001)

        return False
        # sq = 'select * from %s where id = %d' % (self.__tablename__, self.id)
        # dq = 'delete from %s where id = %d' % (self.__tablename__, self.id)
        # while True:
        #     tasks = session.execute(sq).fetchall()
        #     try:
        #         if not len(tasks):
        #             session.commit()
        #             break
        #         session.execute(dq)
        #         session.commit()
        #         return True
        #     except:
        #         import traceback
        #         traceback.print_exc()
        #         session.rollback()
        #         time.sleep(0.001)
        # return False




