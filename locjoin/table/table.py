import pdb
import traceback
import sqlalchemy as sa

from locjoin.annos.models import get_table_annotation
from locjoin.metadata.models import LocationMetadata as LMD
from locjoin.metadata.models import LocType
from locjoin.shapes.util import *
import locjoin.meta as meta






class Table(object):

    def __init__(self, session, tablename):
        self.session = session
        self.tablename = tablename
        self.tableklass = meta.load_table_class(tablename)
        self.annoklass = get_table_annotation(tablename)
        self.lmds = None

    def cols(self):
        return self.tableklass.__table__.columns.keys()

    def select_cols(self, cols=None):
        cols = cols or self.cols()
        return ','.join(['t.%s' % c for c in cols])

    def rows(self, offset=0, limit=10):
        q = 'select * from %s offset %d limit %d'
        q = q % (self.tablename, offset, limit)
        rows = self.session.execute(q).fetchall()
        cols = self.cols()
        ret = [dict(zip(cols, row)) for row in rows]
        self.session.commit()
        return ret

    def metadata(self):
        try:
            q = LMD.current(self.session)
            q = q.filter(LMD.tname==self.tablename)
            self.lmds = q.all()
            self.session.commit()
            return self.lmds
        except:
            traceback.print_exc()
            return []

    def annotations(self, offset=0, limit=10, lmds=None, point_only=False):
        try:
            lmds = lmds or self.metadata()

            
            ret = []
            for lmd in lmds:
                if point_only and not lmd.is_point:
                    continue
                
                q = self.session.query(self.annoklass)
                q = q.filter(self.annoklass.locid==lmd.id)
                q = q.offset(offset)
                if limit is not None:
                    q = q.limit(limit)

                objs = q.all()

                if lmd.is_point:
                    annos = [geom_to_point(obj.latlon) for obj in objs]
                else:
                    annos = [geom_to_polygons(obj.shape.shape) for obj in objs]
                ret.append(annos)
            return ret
        except:
            traceback.print_exc()
            return []

    def latlon_counts(self, lmd_id, ngrid=50, agg=None):
        try:
            if agg is None:
                agg = 'avg(t.av_total)'
            q = """select
                      (st_x(latlon)*%s)::int/%s. as lat,
                      (st_y(latlon)*%s)::int/%s. as lon,
                      %s as agg
               from %s as t, %s__annotation__ as a
               where a.locid = :loc_id and a.rid = t.id
               group by lat, lon
               having count(*) > 4 and %s is not null
               order by agg desc;""" 

            lmd = self.session.query(LMD).get(lmd_id)
            q = q % (ngrid, ngrid, ngrid, ngrid, agg, lmd.tname, lmd.tname, agg)
            print q
            rows = self.session.execute(q, {'loc_id' : lmd_id}).fetchall()
            rows = map(tuple, rows)
            return rows
        except:
            traceback.print_exc()
            return []


    def annotated_rows(self, lmd, offset=0, limit=10, cols=None):
        try:
            cols = cols or self.cols()
            
            is_point = lmd.is_point
            if is_point:
                q = """select %s, st_asewkb(a.latlon)
                from %s as t, %s as a
                where t.id = a.rid and a.locid = %d
                offset %d limit %d"""
            else:
                q = """select %s, st_asewkb(ds.shape)
                from %s as t, %s as a, __dbtruck_shape__ as ds
                where t.id = a.rid and a.locid = %d and ds.ptrid = a.shape_id
                order by t.id asc
                offset %d limit %d"""

            args = (self.select_cols(cols=cols),
                    self.tablename,
                    self.annoklass.__tablename__,
                    lmd.id,
                    offset,
                    limit)
            q = q % args

            locname = lmd.col_name
            rows = self.session.execute(q).fetchall()
            ret = []


            prev_d = None
            for row in rows:
                row = list(row)
                d = dict(zip(cols, row[:-1]))
                
                if prev_d is None or prev_d['id'] != d['id']:
                    d[lmd] = []
                    ret.append(d)

                if is_point:
                    d[lmd].append(geom_to_point(row[-1]))
                else:
                    polygon = geom_to_polygons(row[-1])
                    d[lmd].append(polygon)

                prev_d = d
                

            self.session.commit()
            return ret
        except:
            traceback.print_exc()
            return []

        
