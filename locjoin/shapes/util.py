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

def geom_to_polygon(col):
    try:
        s = str(col.geom_wkb)
    except:
        s = str(col)

    pts = swkb.loads(s)
    coords = pts.exterior.coords
    return zip(coords.xy[0], coords.xy[1])
