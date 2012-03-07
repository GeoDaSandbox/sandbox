'''
dataIO: module for code related to data files manipulation

Find here classes and functions to deal with DBFs, CSVs as well as Numpy arrays,
pandas DataFrames, etc.
'''

import pysal as ps
import numpy as np
import pandas

def df2dbf(df, dbf_path, my_specs=None):
    '''
    Convert a pandas.DataFrame into a dbf. 

    __author__  = "Dani Arribas-Bel <darribas@asu.edu> "
    ...

    Arguments
    ---------
    df          : DataFrame
                  Pandas dataframe object to be entirely written out to a dbf
    dbf_path    : str
                  Path to the output dbf. It is also returned by the function
    my_specs    : list
                  List with the field_specs to use for each column.
                  Defaults to None and applies the following scheme:
                    * int: ('N', 14, 0)
                    * float: ('N', 14, 14)
                    * str: ('C', 14, 0)
    '''
    if my_specs:
        specs = my_specs
    else:
        type2spec = {int: ('N', 20, 0),
                float: ('N', 36, 15),
                str: ('C', 14, 0)
                }
        types = [type(df[i][0]) for i in df.columns]
        specs = [type2spec[t] for t in types]
    db = ps.open(dbf_path, 'w')
    db.header = list(df.columns)
    db.field_spec = specs
    for i, row in df.T.iteritems():
        db.write(row)
    db.close()
    return dbf_path

def dbf2df(dbf_path, index=None, cols=False):
    '''
    Read a dbf file as a pandas.DataFrame, optionally selecting the index
    variable and which columns are to be loaded.

    __author__  = "Dani Arribas-Bel <darribas@asu.edu> "
    ...

    Arguments
    ---------
    dbf_path    : str
                  Path to the DBF file to be read
    index       : str
                  Name of the column to be used as the index of the DataFrame
    cols        : list
                  List with the names of the columns to be read into the
                  DataFrame. Defaults to False, which reads the whole dbf

    Returns
    -------
    df          : DataFrame
                  pandas.DataFrame object created
    '''
    db = ps.open(dbf_path)
    if cols:
        if index not in cols and index:
            cols.append(index)
        vars_to_read = cols
    else:
        vars_to_read = db.header
    data = dict([(var, db.by_col(var)) for var in vars_to_read])
    db.close()
    if index:
        return pandas.DataFrame(data, index=data[index])
    else:
        return pandas.DataFrame(data)

def appendcol2dbf(dbf_in,dbf_out,col_name,col_spec,col_data):
    """
    Function to append a column and the associated data to a DBF.

    __author__ = "Nicholas Malizia <nmalizia@asu.edu>"

    Arguments
    ---------
    dbf_in      : string
                  name of the dbf file to be updated, including extension.
    dbf_out     : string
                  name of the dbf file to be updated, including extension.
    col_name    : string
                  name of the field to be added to dbf.
    col_spec    : tuple
                  the format for the tuples is (type,len,precision).
                  valid types are 'C' for characters, 'L' for bool, 'D' for
                  data, 'N' or 'F' for number.
    col_data    : list
                  a list of values to be written in the column

    Example
    -------
    
    Just a simple example using the ubiquitous Columbus dataset. First, 
    specify the names of the input and output DBFs. 

    >>> dbf_in = 'columbus.dbf'
    >>> dbf_out = 'columbus_copy.dbf'

    Next, give the name of the new column. 

    >>> col_name = 'test'

    And, the specifications associated with it. See the documentation above
    for a further explanation of this requirement. Essentially it's a tuple
    with three parameters: type, length and precision. 

    >>> col_spec = ('N',9,0)

    Finally, we need to create some data to throw in the column. Ideally, this
    would be something that you'd already have handy (that's why you're adding
    a new column to the DBF right?). Here though we'll just create something
    simple like an integer ID. This could be a list of null values if the data
    aren't ready yet. 

    >>> db = ps.open(dbf_in)
    >>> n = db.n_records
    >>> col_data = range(n)

    We pull it all together with the function created here. 

    >>> appendcol2dbf(dbf_in,dbf_out,col_name,col_spec,col_data)

    This will output a second DBF that can then be used to replace the
    original DBF (this will often be the case when working with shapefiles). I
    figured it would be more prudent to create a second file which the user
    can then inspect and manually replace if they want rather than just
    blindly overwriting the original. This of course could be coded as well.
    I'll leave that up to the user though. I don't want emails complaining
    that I deleted your data ;) 

    """

    # open the original dbf and create a new one with the new field
    db = ps.open(dbf_in)
    db_new = ps.open(dbf_out,'w')
    db_new.header = db.header
    db_new.header.append(col_name)
    db_new.field_spec = db.field_spec
    db_new.field_spec.append(col_spec)

    # populate the dbf with the original and new data
    item = 0
    for rec in db:
        rec_new = rec
        rec_new.append(col_data[item])
        db_new.write(rec_new)
        item += 1

    # close the files 
    db_new.close()
    db.close()

