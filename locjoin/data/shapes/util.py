import sys
import shapefile
from shapely.geometry import MultiPolygon, Polygon


from sqlalchemy import *
from sqlalchemy.orm import *

from locjoin.shapes.util import clockwise

def get_wkt(shape):
    polygons = []
    parts = list(shape.parts)

    for start, end in zip(parts, parts[1:]+[len(shape.points)]):
        pts = list(map(tuple,shape.points[start:end]))
	is_not_hole = clockwise(pts + [pts[0]])

	if is_not_hole:
	    polygons.append( (pts, []) )
	else:
	    polygons[-1][1].append(pts)



    mp = MultiPolygon(polygons)
    return WKTSpatialElement(str(mp))

        
    s = ','.join(['((%s))' % ', '.join([' '.join(map(str, pt)) for pt in poly])
                  for poly in polygons])
    shp = WKTSpatialElement('MULTIPOLYGON(%s)' % s)
    return shp


