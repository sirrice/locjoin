

def get_user_tables(session):
    import sqlalchemy as sa
    md = sa.MetaData(bind=session.bind)
    md.reflect()

    ret = []
    for tablename, schema in md.tables.iteritems():
        if not is_system_table(tablename):
            ret.append(tablename)
    return ret

def is_system_table(tablename):
    GEO_COLUMNS = ['spatial_ref_sys']
    if '__dbtruck' in tablename:
        return True
    if tablename.endswith('__annotation__'):
        return True
    if tablename in GEO_COLUMNS:
        return True
    return False
    


def to_utf(v):
    try:
        return v.strftime('%m/%d/%Y %H:%M')
    except:
        if isinstance(v, unicode):
            s = v.encode('utf-8', errors='ignore')
        elif isinstance(v, basestring):
            s = unicode(v, 'utf-8', errors='ignore').encode('utf-8', errors='ignore')
        else:
            s = str(v)
        return s

