'''
Tools work with geographical data
'''

import pysal as ps
import numpy as np
import pandas as pd

def clip_shp_val(shp_in, col_name, vals, shp_out=None):
    '''
    Clip out part of a shapefile based on the values of a column.

    This method is recommended when 'vals' is a short list, otherwise it may
    become pretty slow. Unlike clip_shp_key, vals may be repeated in col_name.
    ...

    Arguments
    =========
    shp_in      : str
                  Path to the shapefile to be clipped
    col_name    : str
                  Name of the column to subset
    vals        : list
                  Values of 'col' to be clipped out of shp_in and written into
                  shp_out.
    shp_out     : str
                  [Optional] Path to the shapefile to be created. If None,
                  writes the file with the same name plus '_clipped' appended.
    '''
    if not shp_out:
        shp_out = shp_in[:-4] + '_clipped.shp'
    return shp_out

def clip_shp_key(shp_in, col_name, keys, shp_out=None):
    '''
    Clip out part of a shapefile based on a subset of the dbf.

    Similar functionality as clip_shp_val but in this case keys must
    contain values that are unique in col_name. This is recommended when keys
    is a long list.
    ...

    Arguments
    =========
    shp_in      : str
                  Path to the shapefile to be clipped
    col_name    : str
                  Name of the column to subset
    keys        : list
                  Values of 'col' to be clipped out of shp_in and written into
                  shp_out. IMPORTANT: they must be unique on the column
    shp_out     : str
                  [Optional] Path to the shapefile to be created. If None,
                  writes the file with the same name plus '_clipped' appended.
    '''
    if not shp_out:
        shp_out = shp_in[:-4] + '_clipped.shp'
    dbi = ps.open(shp_in[:-3] + 'dbf')
    dbo = ps.open(shp_out[:-3] + 'dbf', 'w')
    dbo.header = dbi.header
    dbo.field_spec = dbi.field_spec
    shpi = ps.open(shp_in)
    shpo = ps.open(shp_out)
    col = np.array(dbi.by_col(col_name))
    full = pd.Series(np.array(dbi.by_col(col_name)))
    subset = pd.Series(np.array(vals))
    polys = list(shpi)
    to_clip
    for i in to_clip:
        dbo.write(dbi[i])
        shpo.write(polys[i])
    shpo.close()
    shpi.close()
    dbo.close()
    dbi.close()
    return shp_out

