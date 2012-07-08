from operator import mul

import shapely
import shapely.geometry as sgeo
import shapely.wkt as swkt
import shapely.wkb as swkb


def geom_to_point(col):
    try:
        s = str(col.geom_wkb)
    except:
        s = str(col)

    pt = swkb.loads(s)
    return (pt.x, pt.y)

def geom_to_polygons(col):
    try:
        s = str(col.geom_wkb)
    except:
        s = str(col)

    mp = swkb.loads(s)
    ret = []
    for g in mp:
        coords = g.exterior.coords
        ret.append(zip(coords.xy[0], coords.xy[1]))
    return ret


def clockwise(points):
    """ 
    computes whether the polygon defined by points is
    clockwise
    """

    area = 0.
    for pt1, pt2 in zip(points, points[1:]):
	area += (pt2[0] - pt1[0]) * (pt2[1] + pt1[1])

    return area > 0


def counterclockwise(points):
    return not clockwise(points)


if __name__ == '__main__':
    
    print clockwise([[0,0], [1,0], [1,1], [0,1], [0,0]])
    print clockwise([[0,0], [0,1], [1,1], [1,0], [0,0]])
