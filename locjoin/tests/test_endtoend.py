import pdb
import time
import pickle
import traceback
import locjoin.settings as settings


from locjoin.tasks.tasks import *
from locjoin.tasks.util import *
from locjoin.tasks.models import *
from locjoin.tests.models import *
from locjoin.metadata.metadata import *




if __name__ == '__main__':

    import locjoin.settings as settings
    from locjoin import init_model
    import locjoin.meta as meta

    db = create_engine(settings.DBURI, isolation_level='REPEATABLE READ')
    init_model(db)

    from locjoin.table.table import Table

    table = Table(meta.session, 'realestate_small')
    lmd = table.metadata()[-1]
    print table.annotated_rows(lmd, limit=1, cols=['zipcode'])
    annos = table.annotations()
    pdb.set_trace()
