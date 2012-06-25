#!/usr/bin/env python
try:
    from setuptools import setup, find_packages
except ImportError:
    import ez_setup
    ez_setup.use_setuptools()
from setuptools import setup, find_packages
import locjoin

setup(name="locjoin",
      version=locjoin.__version__,
      description="Analyze and augment your database with location information",
      license="BSD",
      author="Eugene Wu",
      author_email="eugenewu@mit.edu",
      url="http://github.com/sirrice/locjoin",
      include_package_data = True,      
      packages = find_packages(),
      package_dir = {'locjoin' : 'locjoin'},
      scripts = ['bin/locjoin_analyze.py', 'bin/locjoin_correlate.py', 'bin/locjoin_runner.py'],
      package_data = { 'locjoin' : ['data/*'] },
      install_requires = ['bsddb3', 'xlrd', 'pyxl', 'argparse', 'DateUtils', 
                          'geopy', 'openpyxl', 'requests', 'pyquery', 'geoalchemy', 'sqlalchemy',
                          'shapely', 'pyshp'],
      keywords= "library db join location analyze")
