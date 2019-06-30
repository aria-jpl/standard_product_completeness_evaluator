import os
from shapely.geometry import shape, Polygon, MultiPolygon, mapping
from shapely.ops import cascaded_union
from shapely.validation import explain_validity
import shapely.ops

def validate_geojson(coord):
    B=[]
    for C in  coord:
        if C is not None:
            C_updated = check_fix(C)
            print("new_length : {}".format(len(C_updated)))
            B.append(c_updated)
    return tuple(B)


def check_fix(C):
    n = len(C)
    print("orig_length :{}".format(n))
    i=1
    while(i<n):

        if C[i]==C[i-1]:
            print("List updated as two same consecutive points : {}".format(C[i]))
            return fix_tuple(C, i)
        i = i+1
    print("List unchanged")
    return C



def fix_tuple(A, i):
    B = list(A)
    C = B[i:]
    C.extend(B[:i])
    print("")
    #print(C)
    return tuple(C)

def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    print("get_area : coords : %s" %coords)
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        #print("i : %s j: %s, coords[i][1] : %s coords[j][0] : %s coords[j][1] : %s coords[i][0] : %s"  %(i, j, coords[i][1], coords[j][0], coords[j][1], coords[i][0]))
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    #area = abs(area) / 2.0
    return area / 2

def change_coordinate_direction(cord):
    print("change_coordinate_direction 1 cord: {}\n".format(cord))
    cord_area = get_area(cord)
    if not cord_area>0:
        print("change_coordinate_direction : coordinates are not clockwise, reversing it")
        cord = [cord[::-1]]
        print("change_coordinate_direction 2 : cord : {}".format(cord))
        try:
            cord_area = get_area(cord)
        except:
            cord = cord[0]
            print("change_coordinate_direction 3 : cord : {}".format(cord))
            cord_area = get_area(cord)
        if not cord_area>0:
            print("change_coordinate_direction. coordinates are STILL NOT  clockwise")
    else:
        print("change_coordinate_direction: coordinates are already clockwise")

    print("change_coordinate_direction 4 : cord : {}".format(cord))
    return cord

def validate_geojson2(geojson):
    '''validates the geojson and converts it into a shapely object. can accept strings, shapefiles & geojson dicts'''
    if isinstance(geojson, str):
        geojson = json.loads(geojson)
    if isinstance(geojson, shapely.geometry.polygon.Polygon):
        return geojson
    if isinstance(geojson, shapely.geometry.multipolygon.MultiPolygon):
        return geojson
    shp = shape(geojson)
    if shp.is_valid:
        return shp
    else:
        shp = shp.buffer(0)# handle self-intersection
        if shp.is_valid:
            return shp
        else:
            print(type(geojson))
            raise Exception('input geojson is not valid: {}'.format(explain_validity(shp)))


def change_union_coordinate_direction(union_geom):
    print("change_coordinate_direction")
    coordinates = union_geom["coordinates"]
    print("Type of union polygon : {} of len {}".format((type(coordinates), len(coordinates))))
    for i in range(len(coordinates)):
        cord = coordinates[i]
        cord_area = get_area(cord)
        if not cord_area>0:
            print("change_coordinate_direction : coordinates are not clockwise, reversing it")
            cord = [cord[::-1]]
            print(cord)
            cord_area = get_area(cord)
            if not cord_area>0:
                print("change_coordinate_direction. coordinates are STILL NOT  clockwise")
            union_geom["coordinates"][i] = cord
        else:
            print("change_coordinate_direction: coordinates are already clockwise")

    return union_geom


