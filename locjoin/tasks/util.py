


def check_job_table(db):
    try:
        db.execute('select * from __dbtruck_jobs__ limit 1').fetchall()
    except:
        q = """
        create table __dbtruck_jobs__(
          id serial,
          fname varchar(128),
          args text null,
          kwargs text null,
          running bool default false,
          done bool default false,
          lastrun timestamp default current_timestamp
        )        
        """
        db.execute(q)
