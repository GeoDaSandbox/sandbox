'''
dataIO: module for code related to data files manipulation

Find here classes and functions to deal with DBFs, CSVs as well as Numpy arrays,
pandas DataFrames, etc.
'''

import pysal as ps
import numpy as np
import pandas as pd
import os

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
                np.int64: ('N', 20, 0),
                float: ('N', 36, 15),
                np.float64: ('N', 36, 15),
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

def dbf2df(dbf_path, index=None, cols=False, incl_index=False):
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
    incl_index  : Boolean
                  If True index is included in the DataFrame as a
                  column too. Defaults to False

    Returns
    -------
    df          : DataFrame
                  pandas.DataFrame object created
    '''
    db = ps.open(dbf_path)
    if cols:
        if incl_index:
            cols.append(index)
        vars_to_read = cols
    else:
        vars_to_read = db.header
    data = dict([(var, db.by_col(var)) for var in vars_to_read])
    if index:
        index = db.by_col(index)
        db.close()
        return pd.DataFrame(data, index=index)
    else:
        db.close()
        return pd.DataFrame(data)

def appendcol2dbf(dbf_in,dbf_out,col_name,col_spec,col_data,replace=False):
    """
    Function to append a column and the associated data to a DBF.

    __author__ = "Nicholas Malizia <nmalizia@asu.edu>"

    Arguments
    ---------
    dbf_in      : string
                  name and path of the dbf file to be updated, including
                  extension.
    dbf_out     : string
                  name and path of the new file to be created, including
                  extension. 
    col_name    : string
                  name of the field to be added to dbf.
    col_spec    : tuple
                  the format for the tuples is (type,len,precision).
                  valid types are 'C' for characters, 'L' for bool, 'D' for
                  data, 'N' or 'F' for number.
    col_data    : list
                  a list of values to be written in the column, note the length
                  of this list should match the number of records in the original
                  dbf.
    replace     : boolean
                  if true, replace existing dbf file

    Example
    -------
    
    Just a simple example using the ubiquitous Columbus dataset. First, 
    specify the names of the input and output DBFs. 

    >>> dbf_in = 'columbus.dbf'
    >>> dbf_out = 'columbus_copy.dbf'

    Next, give the name of the column to be added. 

    >>> col_name = 'test'

    Also, provide the specifications associated with the new column. See the
    documentation above for a further explanation of this requirement.
    Essentially it's a tuple with three parameters: type, length and precision. 

    >>> col_spec = ('N',9,0)

    Finally, we need to provide the data to populate the column. Ideally, this
    would be something that you'd already have handy (that's why you're adding
    a new column to the DBF right?). Here though we'll just create something
    simple like an integer ID. This could be a list of null values if the data
    aren't ready yet and you want a placeholder. 

    >>> db = ps.open(dbf_in)
    >>> n = db.n_records
    >>> col_data = range(n)

    We pull it all together with the function created here. 

    >>> appendcol2dbf(dbf_in,dbf_out,col_name,col_spec,col_data)

    This will output a second DBF that can then be used to replace the
    original DBF (this will often be the case when working with shapefiles). I
    figured it would be more prudent to have the function by default create a 
    second file which the user can then inspect and manually replace if they
    want rather than just blindly overwriting the original. The latter is an
    option. Use at your own risk - I don't want complaints that I deleted your
    data ;) 

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

    # the following text will delete the old dbf and replace it with the new one
    # retaining the name of the original file. 
    if replace is True: 
        os.remove(dbf_in)
        os.rename(dbf_out, dbf_in)

def multi_model_tab(models, coefs2show=['betas', 'significance'], decs=4):
    '''
    Generate a pandas DataFrame with results from several models
    ...

    Arguments
    ---------
    models      : list
                  list-like object with all the pysal.spreg models
    coefs2show  : list
                  Attributes from a pysal.spreg model that will be pull out
                  into the table. Note that it will only work if the
                  attributes relate to the variables (e.g. betas, ses).
                  If 'significance' is included, a column per model with
                  significance level stars ('***' for 1%, '**' for 5% and
                  '*' for 10%) will be added.
    decs        : int
                  Decimals to which round the output

    Returns
    -------
    out         : DataFrame
                  Pandas DataFrame object with output
    '''
    out = []
    for c, model in enumerate(models):
        if 'significance' in coefs2show:
            model.significance = np.array([signify(t[1]) for t in model.t_stat])
        outm = pd.DataFrame({par: getattr(model, par).flatten() for par in coefs2show},\
                index=model.name_x).rename(columns={'significance': 'p'})
        coli = pd.MultiIndex.from_tuples([('M-%i'%(c+1), col) \
                for col in outm.columns.values])
        outm.columns = coli
        out.append(outm)
    out = pd.concat(out, axis=1)
    for col in out:
        try:
            out[col] = out[col].apply(lambda x: str(np.round(x, decimals=decs)))
        except:
            pass
    addons = pd.DataFrame({'': ['']*out.shape[1], \
            'R^2': np.array([[try_r2(m, decs=decs)]+['']*(len(coefs2show)-1) for m in models]).flatten(), \
            'N': np.array([[str(m.y.shape[0])]+['']*(len(coefs2show)-1) for m in models]).flatten(), \
            }, \
            index=out.columns).T
    out = pd.concat([out, addons], axis=0)
    out = out.fillna('').replace('nan', '')
    return out

def try_r2(model, decs=4):
    'Attempt to return R^2'
    try:
        return str(np.round(model.r2, decimals=decs))
    except:
        return ''

def signify(p):
    'Turn p-value into significance star(s)'
    if p <= 0.001:
        return '***'
    elif p > 0.01 and p <= 0.05:
        return '**'
    elif p > 0.005 and p <= 0.1:
        return '*'
    else:
        return ''

def updatelisashp(lm,shp,alpha=0.05,norm=False):
    """

    Updates the DBF of a shapefile to include the results of a LISA object from
    PySAL.     

    __author__ = "Nicholas Malizia <nmalizia@asu.edu>"

    Arguments
    ---------
    lm      : pysal local moran object
              output from pysal local moran analysis
    shp     : string
              path and name (excluding extension) of the relevant shapefile
    alpha   : float
              nominal significance level
    norm    : boolean
              use the standard normal approximation data to identify significance
              

    Example
    -------

    First, we need to create the local moran object using PySAL. Again, we'll be
    using the columbus dataset. Here, we'll be looking for clustering of
    criminal activity. See the PySAL documentation for a full explanation of how 
    to run a local moran and how to interpret the results. 

    >>> import pysal as ps
    >>> import numpy as np
    >>> np.random.seed(10)
    >>> w = ps.open(pysal.examples.get_path("columbus.gal")).read()
    >>> f = ps.open(pysal.examples.get_path("columbus.dbf"))
    >>> y = np.array(f.by_col['CRIME'])
    >>> lm = ps.esda.moran.Moran_Local(y,w,transformation="r",permutations=99)

    Next we define the shapefile information and specify the alpha level that we
    will use to define significant clusters for output to our shapefile. Note
    that we're leaving the default value for the "norm" parameter, meaning that
    we're using the pseudo p-values determined through the simulation rather
    than those based on a standard normal approximation. We could use the latter
    by changing the "norm" parameter to True. Also, it should be obvious, but
    it's worth stating, the shapefile name specified here has to correspond to
    the shapefile that was used in the creation of the local Moran object. 

    >>> shp = "columbus"
    >>> alpha = 0.05

    And finally we run the function created here inputting our recently defined
    parameters.

    >>> updatelisashp(lm, shp, alpha)

    The updated shapefile can then be opened in your favorite GIS software! 

    """
    # count the observations
    n = len(lm.y)

    # identify which observations are significant using the appropriate p-values
    if norm is True:
        sig = lm.p_z_sim<alpha
    else:
        sig = lm.p_sim<alpha

    sig_ids = np.array(range(lm.n))[sig]

    # create a list of p-values to add to the dbf
    if norm is True:
        data_p = lm.p_z_sim
    else:
        data_p = lm.p_sim
    
    # create a list of lisa categories to add to the dbf
    data_q = [0]*n
    for i in sig_ids:
        data_q[i] = lm.q[i]

    # prep parameters for changing the dbf
    dbf_in = shp + ".dbf"
    dbf_out = shp + "_copy.dbf"
    col_name = ['quadrat', 'pvalue']    
    col_spec = [('N',9,0), ('F',10,8)]
    col_data = [data_q, data_p]

    # add the quadrat and pvalue columns to the data
    for i in range(2):
        appendcol2dbf(dbf_in,dbf_out,col_name[i],col_spec[i],col_data[i],\
        replace=True)
