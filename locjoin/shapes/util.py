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
