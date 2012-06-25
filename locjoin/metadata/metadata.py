"""
Detects and manages the location metadata about what location information is contained
within a table.

The extractor module takes this metadata to geocode and populate a table's actual
annotations
"""



import locjoin.meta as meta
from locjoin.metadata.models import *
from locjoin.metadata.models import LocationMetadata as LMD
from locjoin import init_model



class BaseMetadataDetector(object):
    def __init__(self, session):
        self.session = session
        self.db = session.bind


    def __call__(self, tablename):
        return []

    def analyze_column(self, colname, col):
        pass

class DummyMetadataDetector(BaseMetadataDetector):
    def __call__(self, tablename):

        ret = []
        ret.append(LMD(tname = tablename,
                       loc_type = LocType.STATE,
                       extract_type=ExtractType.FMT,
                       fmt="{state}"))

        ret.append(LMD(tname = tablename,
                       loc_type = LocType.ADDR,
                       extract_type=ExtractType.FMT,
                       fmt="{addr}, {city}, {state}"))
        return ret



def create_metadata_from_user_inputs(session, tablename, user_inputs):
    lmds = []
    for user_input in user_inputs:
        if user_input.get('deleted', False):
            continue

        if 'id' in user_input:
            lmd = session.query(LMD).get(user_input['id'])
        else:
            lmd = LMD()

        lmd.tname = tablename
        lmd.col_name = user_input['col_name']
        lmd.loc_type = user_input['loc_type']
        lmd.extract_type = ExtractType.FMT
        lmd.fmt = user_input['format'].strip()
        lmd.source = LocSource.USER


        lmds.append(lmd)
    return lmds




def update_metadata(session, tablename, user_lmds, source=-1):
    """
    XXX: the wrong inputs could wipe out all of your metadata!!
    existing MDs without corresonding object in user_lmds is deleted
    MDs with ID values is updated
    MDs without ID value is added
    """
    # q = "select id, fmt, source from %s where tablename = :tn and deleted = false"
    # rows = self.session.execute(q, {'tn' : tablename}).fetchall()
    
    q = LMD.current(session).filter(LMD.tname == tablename)
    resproxy = q.all()
    existing_lmds = [res for res in resproxy]

    if not existing_lmds:
        session.add_all(user_lmds)
        session.commit()
        return []

    if not user_lmds:
        for lmd in existing_lmds:
            if lmd.source <= source:
                lmd.deleted = True
        session.add_all(existing_lmds)
        session.commit()
        return []

    if source == -1:
        source = max([lmd.source for lmd in user_lmds])

    if max([lmd.source for lmd in existing_lmds]) > source:
        return []


    existing_id_to_lmd = dict([(lmd.id, lmd) for lmd in existing_lmds])
    user_ids = set([lmd.id for lmd in user_lmds if lmd.id is not None])
    existing_ids = set([lmd.id for lmd in existing_lmds])
    rm_ids = existing_ids.difference(user_ids)
    updated_ids = existing_ids.intersection(user_ids)
    updated_ids.update(user_ids.difference(existing_ids))
    
    rm_lmds = filter(lambda lmd: lmd.id in rm_ids, existing_lmds)
    updated_lmds = [lmd for lmd in user_lmds if lmd.id in updated_ids]
    new_lmds = [lmd for lmd in user_lmds if lmd.id is None]

    # of the updated lmds, which need to be re-processed?
    reprocess = list(new_lmds)
    for lmd in updated_lmds:
        if existing_id_to_lmd[lmd.id].fmt != lmd.fmt:
            reprocess.append(lmd)


    for lmd in rm_lmds:
        lmd.deleted = True
    session.add_all(rm_lmds)
    session.add_all(user_lmds)
    session.commit()


    return reprocess

if __name__ == '__main__':
    from locjoin.settings import DBURI
    import sys
    import pdb

    db = create_engine(DBURI)
    init_model(db)

    mdd = DummyMetadataDetector('foobar')
    update_metadata(meta.session, 'foobar', mdd())
