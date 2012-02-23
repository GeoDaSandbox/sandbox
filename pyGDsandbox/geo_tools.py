'''
Tools work with geographical data
'''

import pysal as ps
import numpy as np
import pandas as pd

def clip_shp(shp_in, col_name, keys, shp_out=None):
    '''
    Clip out part of a shapefile based on a subset of one of the dbf columns.
    ...

    Arguments
    =========
    shp_in      : str
                  Path to the shapefile to be clipped
    col_name    : str
                  Name of the column to subset
    keys        : list
                  Values of 'col' to be clipped out of shp_in and written into
                  shp_out.
    shp_out     : str
                  [Optional] Path to the shapefile to be created. If None,
                  writes the file with the same name plus '_clipped' appended.
    Returns
    =======
    shp_out     : str
                  [Optional] Path to the shapefile to be created. If None,
                  writes the file with the same name plus '_clipped' appended.
    '''
    k = list(set(keys))
    if len(k) != len(keys):
        raise Exception, "Please don't pass duplicates in keys"
    keys = k
    if not shp_out:
        shp_out = shp_in[:-4] + '_clipped.shp'
    dbi = ps.open(shp_in[:-3] + 'dbf')
    dbo = ps.open(shp_out[:-3] + 'dbf', 'w')
    dbo.header = dbi.header
    dbo.field_spec = dbi.field_spec
    shpi = ps.open(shp_in)
    shpo = ps.open(shp_out, 'w')
    col = np.array(dbi.by_col(col_name))
    full = pd.DataFrame({'full': np.array(dbi.by_col(col_name))})
    subset = pd.DataFrame({'sub': keys}, index=keys)
    to_clip = full.join(subset, on='full').dropna().index.astype(int)
    polys = list(shpi)
    for i in to_clip:
        dbo.write(dbi[i][0])
        shpo.write(polys[i])
    shpo.close()
    shpi.close()
    dbo.close()
    dbi.close()
    return shp_out

