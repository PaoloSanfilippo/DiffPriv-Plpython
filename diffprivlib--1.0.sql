-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION diffprivlib" to load this file. \quit

/*
Type of object containing the array and parameters for the computation.
arr: real[]
    array for the computation
epsi: real
    value of the epsilon parameter
bounds: real[]
    array containing computation boundaries that can be used to calibrate sensitivity
quant: real
    quantile or percentile used for percentile or quantile functions.
bin: int
    number of bin used for histogram functions.
*/
DROP TYPE IF EXISTS diffpriv_object CASCADE;
CREATE  TYPE diffpriv_object AS (
  arr   real[],
  epsi  real,
  bounds real[],
  quant real,
  bin int
);

/*
Set epsilon as session parameter
Parameters
    epsi : real
       value of epsi
*/
CREATE OR REPLACE FUNCTION set_epsi (epsi real) RETURNS text AS '
    BEGIN
    PERFORM set_config(''diffpriv.epsi'',$1::text, false);
    RETURN ''Parameter epsilon setted''; 
    END;  
' LANGUAGE 'plpgsql';

/*
Set the parameters to calibrate the sensitivity as session parameters
Parameters
    bounds_min : real
        value of the lower limit
    bounds_max : real
        value of the upper limit
*/
CREATE OR REPLACE FUNCTION set_bounds (bounds_min real,bounds_max real) 
RETURNS text AS '
    BEGIN
    PERFORM set_config(''diffpriv.bounds_min'',$1::text, false);
    PERFORM set_config(''diffpriv.bounds_max'',$1::text, false);
    RETURN ''Parameter bounds setted''; 
    END;
' LANGUAGE 'plpgsql';


/*
PL/pgSQL function which prepares object of type diffpriv_object for computation of Pl/Python functions
Function used as a transition function of the following aggregate functions: dv_avg, dv_avg_nan,dp_sum,dp_sum_nan,dp_count_nonzero,dp_var_pop,dp_var_pop_nan,dp_std_pop,dp_std_pop_nan,dp_median.
Parameters
    $1 : diffpriv_object
        object to be prepared.
    $2 : anyelement
        single element of expression.
*/
CREATE OR REPLACE FUNCTION diffpriv_object_builder_general_utilities (diffpriv_object,anyelement) 
RETURNS diffpriv_object AS '
    DECLARE
     obj diffpriv_object;
    BEGIN
        IF pg_typeof($2)::text!=''smallint'' and 
        pg_typeof($2)::text!=''integer'' and pg_typeof($2)::text!=''bigint'' 
        and  pg_typeof($2)::text!=''real'' 
        and pg_typeof($2)::text!=''double precision'' and pg_typeof($2)::text!=''numeric'' THEN
            RAISE EXCEPTION ''Type must be numeric'';
        END IF;
        obj.arr=array_append($1.arr,$2::real);
        obj.epsi=0;
        RETURN obj;
    END;
' LANGUAGE 'plpgsql';


/*
PL/pgSQL function which prepares object of type diffpriv_object for computation of Pl/Python functions.
Function used as a transition function of the following aggregate functions: dv_avg, dv_avg_nan,dp_sum,dp_sum_nan,dp_count_nonzero,dp_var_pop,dp_var_pop_nan,dp_std_pop,dp_std_pop_nan,dp_median
Function
Parameters
   $1 : diffpriv_object
        object to be prepared.
    $2 : anyelement
        single element of expression.
    $3: variadic real[]
        variable number of parameters containing:
            -epsilon
            -epsilon+bounds
*/   
CREATE OR REPLACE FUNCTION diffpriv_object_builder_general_utilities (diffpriv_object,anyelement,variadic real[]) 
RETURNS diffpriv_object AS '
    DECLARE
     obj diffpriv_object;
     length integer=array_length($3,1);
    BEGIN
        IF pg_typeof($2)::text!=''smallint'' and 
        pg_typeof($2)::text!=''integer'' and pg_typeof($2)::text!=''bigint'' and  
        pg_typeof($2)::text!=''real'' and 
        pg_typeof($2)::text!=''double precision'' and pg_typeof($2)::text!=''numeric'' THEN
            RAISE EXCEPTION ''Type must be numeric'';
        END IF;
        obj.arr=array_append($1.arr,$2::real);
        IF length=2 or length>3 THEN
            RAISE EXCEPTION ''Wrong number of parameters'';
        END IF;
        obj.epsi=$3[1];
        IF length>1 THEN
            obj.bounds=array_append(obj.bounds,$3[2]);
            obj.bounds=array_append(obj.bounds,$3[3]);
        END IF;
        RETURN obj;
    END;
' LANGUAGE 'plpgsql';

/*
Returns the average of the array elements with differential privacy. 
Noise is added using Laplace Mechanism to satisfy differential privacy.
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_mean(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import utils
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.mean(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1]))
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return utils.mean(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))    
return utils.mean(obj['arr'],obj['epsi'])
$BODY$;


/*
Returns the average of the array elements with differential privacy,gnoring NaNs. 
Noise is added using Laplace Mechanism to satisfy differential privacy.
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_nanmean(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import utils
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.nanmean(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1]))
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return utils.nanmean(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))    
return utils.nanmean(obj['arr'],obj['epsi'])
$BODY$;

/*   
Sum of array elements with differential privacy.   
Parameters   
    obj : diffpriv_object   
        object containing the array and parameters for the computat   ion.
*/   
CREATE OR REPLACE FUNCTION public.diffpriv_sum(obj diffpriv_object)   
    RETURNS real   
    LANGUAGE 'plpython3u'   
    VOLATILE   
    PARALLEL UNSAFE   
    COST 100     
AS $BODY$   
from diffprivlib.tools import utils   
if obj['epsi']==0:   
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','   t');")[0]['current_setting']
    if epsi_par!=None:   
        obj['epsi']=float(epsi_par)   
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.sum(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1])) 
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return utils.sum(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return utils.sum(obj['arr'],obj['epsi'])
$BODY$;


/*
Sum of array elements with differential privacy, ignoring NaNs.
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_nansum(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import utils
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.sum(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1])) 
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return utils.nansum(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return utils.nansum(obj['arr'],obj['epsi'])
$BODY$;

/*
Counts the number of non-zero values in the array with differential privacy
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_count_nonzero(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import utils
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.count_nonzero(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1]))
return utils.count_nonzero(obj['arr'],obj['epsi'])
$BODY$;

/*
Returns the variance of the array elements, a measure of the spread of a distribution, with differential privacy.
Noise is added 'LaplaceBoundedDomain'
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_var(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import utils
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.var(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1]))  
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return utils.var(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return utils.var(obj['arr'],obj['epsi'])
$BODY$;

/*
Returns the variance of the array elements, a measure of the spread of a distribution, with differential privacy, ignoring NaNs.
Noise is added 'LaplaceBoundedDomain'
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_nanvar(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import utils
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.nanvar(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1]))  
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return utils.nanvar(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return utils.nanvar(obj['arr'],obj['epsi'])
$BODY$;

/*
Returns the standard deviation of the array elements, a measure of the spread of a distribution, with differential privacy.
Noise is added 'LaplaceBoundedDomain'
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_std(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import utils
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.std(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1])) 
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return utils.std(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return utils.std(obj['arr'],obj['epsi'])
$BODY$;

/*
Returns the standard deviation of the array elements, a measure of the spread of a distribution, with differential privacy, ignoring NaNs.
Noise is added 'LaplaceBoundedDomain'
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_nanstd(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import utils
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return utils.nanstd(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1])) 
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return utils.nanstd(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return utils.nanstd(obj['arr'],obj['epsi'])
$BODY$;


/*
Compute the differentially private median of the array.
Noise is added 'LaplaceBoundedDomain'
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_median(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import quantiles
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return quantiles.median(obj['arr'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1])) 
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return quantiles.median(obj['arr'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return quantiles.median(obj['arr'],obj['epsi'])
$BODY$;

/*
Aggregate function that calculates the differentially private average of a set of values of an expression
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_avg (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_mean
);

/*
aggregate function that calculates the differentially private average of a set of values of an expression
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_avg (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_mean
);

/*
aggregate function that calculates the differentially private average of a set of values of an expression, ignoring NaNs.
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_avg_nan (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nanmean
);

/*
aggregate function that calculates the differentially private average of a set of values of an expression, ignoring NaNs.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_avg_nan (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nanmean

);

/*
aggregate function that calculates the differentially private sum of a set of values of an expression. 
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_sum (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_sum
);
/*
aggregate function that calculates the differentially private sum of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_sum (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_sum
);

/*
aggregate function that calculates the differentially private sum of a set of values of an expression, ignoring NaNs.
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_sum_nan (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nansum
);
/*
aggregate function that calculates the differentially private sum of a set of values of an expression, ignoring NaNs.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_sum_nan (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nansum
);
/*
aggregate function that counts the number of non-zero values of an expression with differential privacy.
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_count_nonzero (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_count_nonzero
);
/*
aggregate function that counts the number of non-zero values of an expression with differential privacy.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_count_nonzero (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_count_nonzero
);

/*
aggregate function that calculates the differentially private variance of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_var_pop  (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_var
);
/*
aggregate function that calculates the differentially private variance of a set of values of an expression.
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_var_pop (anyelement,variadic real[])
( 
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_var
);

/*
aggregate function that calculates the differentially private variance of a set of values of an expression, ignoring NaNs.
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_var_pop_nan  (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nanvar
);
/*
aggregate function that calculates the differentially private variance of a set of values of an expression, ignoring NaNs.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_var_pop_nan (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nanvar
);

/*
aggregate function that calculates the differentially private standard deviation of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_std_pop  (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_std
);
/*
aggregate function that calculates the differentially private standard deviation of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/

CREATE OR REPLACE AGGREGATE dp_std_pop (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nanstd
);
/*
aggregate function that calculates the differentially private standard deviation of a set of values of an expression, ignoring NaNs.
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_std_pop_nan  (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nanstd
);
/*
aggregate function that calculates the differentially private standard deviation of a set of values of an expression, ignoring NaNs.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_std_pop_nan (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_nanstd
);

/*
aggregate function that calculates the differentially private standard median of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
*/
CREATE OR REPLACE AGGREGATE dp_median  (anyelement)
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_median
);
/*
aggregate function that calculates the differentially private median of a set of values of an expression.  
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_median (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_general_utilities,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_median
);

/*
Return the differentially private quantile of the array.
Differential privacy is achieved with the Exponentia mechanism
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_quantile(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import quantiles
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return quantiles.quantile(obj['arr'],obj['quant'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1])) 
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return quantiles.quantile(obj['arr'],obj['quant'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return quantiles.quantile(obj['arr'],obj['quant'],obj['epsi'])
$BODY$;

/*
Return the differentially private percentile of the array.
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_percentile(obj diffpriv_object)
    RETURNS real
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import quantiles
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return quantiles.percentile(obj['arr'],obj['quant'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1])) 
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return quantiles.percentile(obj['arr'],obj['quant'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return quantiles.percentile(obj['arr'],obj['quant'],obj['epsi'])
$BODY$;

/*
PL/pgSQL function which prepares object of type diffpriv_object for computation of Pl/Python functions
Function used as a transition function of the following aggregate functions: dv_quantile,dp_percentile.
Parameters
    $1 : diffpriv_object
        object to be prepared.
    $2 : anyelement
        single element of expression.
    $3: variadic real[]
        variable number of parameters containing:
            -epsilon
            -epsilon+bounds
*/
CREATE OR REPLACE FUNCTION diffpriv_object_builder_quantile (diffpriv_object,anyelement,variadic real[]) RETURNS diffpriv_object AS '
    DECLARE
     obj diffpriv_object;
     length integer=array_length($3,1);
    BEGIN
        IF pg_typeof($2)::text!=''smallint'' and pg_typeof($2)::text!=''integer'' and pg_typeof($2)::text!=''bigint'' and  pg_typeof($2)::text!=''real'' and
        pg_typeof($2)::text!=''double precision'' and pg_typeof($2)::text!=''numeric'' THEN
            RAISE EXCEPTION ''Type must be'';
        END IF;
        obj.arr=array_append($1.arr,$2::real);
        obj.quant=$3[1];
        IF length=3 or length>4 THEN
            RAISE EXCEPTION ''Wrong number of parameters'';
        END IF;
        IF length=1 THEN
        obj.epsi=0;
        ELSE
        obj.epsi=$3[2];
        END IF;
        IF length>2 THEN
        obj.bounds=array_append(obj.bounds,$3[3]);
        obj.bounds=array_append(obj.bounds,$3[4]);
        END IF;
        RETURN obj;
    END;
' LANGUAGE 'plpgsql';


/*
aggregate function that calculates the  private quantile of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -quantile
        -quantile+epsilon
        -quantile+epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_quantile (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_quantile,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_quantile
);
/*
aggregate function that calculates the  private percentile of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
$2: variadic real[]
    variable number of parameters containing:
        -percentile
        -percentile+epsilon
        -percentile+epsilon+bounds
*/
CREATE OR REPLACE AGGREGATE dp_percentile (anyelement,variadic real[])
(
    sfunc = diffpriv_object_builder_quantile,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_percentile
);
/*
Return differentially private histogram of a set of data.
noise is added using GeometricTruncate.
Parameters
    obj : diffpriv_object
        object containing the array and parameters for the computation.
*/
CREATE OR REPLACE FUNCTION public.diffpriv_histogram(obj diffpriv_object)
    RETURNS real[]
    LANGUAGE 'plpython3u'
    VOLATILE
    PARALLEL UNSAFE
    COST 100  
AS $BODY$
from diffprivlib.tools import histograms
if obj['epsi']==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        obj['epsi']=float(epsi_par)
    else:
        obj['epsi']=1
if obj['bounds']!=None:
    return histograms.histogram(obj['arr'],obj['bin'],obj['epsi'],(obj['bounds'][0],obj['bounds'][1])) 
bounds_min_par=plpy.execute("SELECT current_setting('diffpriv.bounds_min','t');")[0]['current_setting']
bounds_max_par=plpy.execute("SELECT current_setting('diffpriv.bounds_max','t');")[0]['current_setting']
if bounds_min_par!=None and bounds_max_par!=None:
    return histograms.histogram(obj['arr'],obj['bin'],obj['epsi'],(bounds_min_par,bounds_max_par))     
return histograms.histogram(obj['arr'],obj['bin'],obj['epsi'])
$BODY$;

/*
PL/pgSQL function which prepares object of type diffpriv_object for computation of Pl/Python functions
Function used as a transition function of the following aggregate functions: dv_histogram.
Parameters
    $1 : diffpriv_object
        object to be prepared.
    $2 : anyelement
        single element of expression.
    $3: int
        bin for histogram
*/
CREATE OR REPLACE FUNCTION diffpriv_object_builder_histogram (diffpriv_object,anyelement,int) RETURNS diffpriv_object AS '
    DECLARE
     obj diffpriv_object;
    BEGIN
        IF pg_typeof($2)::text!=''smallint'' and pg_typeof($2)::text!=''integer'' and pg_typeof($2)::text!=''bigint'' and  pg_typeof($2)::text!=''real'' and
        pg_typeof($2)::text!=''double precision'' and pg_typeof($2)::text!=''numeric'' THEN
            RAISE EXCEPTION ''Type must be numeric'';
        END IF;
        obj.arr=array_append($1.arr,$2::real);
        obj.bin=$3;
        obj.epsi=0;
        RETURN obj;
    END;
' LANGUAGE 'plpgsql';
/*
PL/pgSQL function which prepares object of type diffpriv_object for computation of Pl/Python functions
Function used as a transition function of the following aggregate functions: dv_histogram.
Parameters
    $1 : diffpriv_object
        object to be prepared.
    $2 : anyelement
        single element of expression.
    $3: int
        number of bin for histogram
    $4: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds    
*/
CREATE OR REPLACE FUNCTION diffpriv_object_builder_histogram (diffpriv_object,anyelement,int,variadic real[]) RETURNS diffpriv_object AS '
    DECLARE
     obj diffpriv_object;
     length integer=array_length($4,1);
    BEGIN
        IF pg_typeof($2)::text!=''smallint'' and pg_typeof($2)::text!=''integer'' and pg_typeof($2)::text!=''bigint'' and  pg_typeof($2)::text!=''real'' and
        pg_typeof($2)::text!=''double precision'' and pg_typeof($2)::text!=''numeric'' THEN
            RAISE EXCEPTION ''Type must be numeric'';
        END IF;
        obj.arr=array_append($1.arr,$2::real);
        obj.bin=$3;
        IF length=2 or length>3 THEN
            RAISE EXCEPTION ''Wrong number of parameters'';
        END IF;
        obj.epsi=$4[1];
        IF length>1 THEN
            obj.bounds=array_append(obj.bounds,$5);
            obj.bounds=array_append(obj.bounds,$6);
        END IF;
        RETURN obj;
    END;
' LANGUAGE 'plpgsql';
/*
aggregate function that calculates the differentially private standard median of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
$2: int
    number of bin for histogram
*/
CREATE OR REPLACE AGGREGATE dp_histogram (anyelement,int)
(
    sfunc = diffpriv_object_builder_histogram,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_histogram
);
/*
aggregate function that calculates the differentially private standard median of a set of values of an expression.
Parameters
$1: anyelement
    single value of the expression
$2: int
    number of bin for histogram
$3: variadic real[]
    variable number of parameters containing:
        -epsilon
        -epsilon+bounds    
*/
CREATE OR REPLACE AGGREGATE dp_histogram (anyelement,int,variadic real[])
(
    sfunc = diffpriv_object_builder_histogram,
    stype = diffpriv_object,
    initcond = '({},0,{},0,0)',
    finalfunc=diffpriv_histogram
);

/*
Default table for storing machine learning models
*/
DROP TABLE IF EXISTS models;
CREATE  TABLE models (
model_name varchar PRIMARY KEY,
model BYTEA NOT NULL,
model_name_type varchar
);

/*
Return a KMeans private model encoded in a sequence of bytes storable in the database
Parameters
number_cluster : int
    number of cluster 
table_name : text
    name of the table from which to extract the training data
columns: text[]
    the names of the columns of the table table_name from which to extract the training data
epsi : real
    value of the parameter epsilon
bound_min, bound_max : real
    values of the parameters used to calibrate sensitivity
*/
CREATE OR REPLACE FUNCTION KMeans(number_cluster int,table_name text,
columns text[] Default NULL,epsi real DEFAULT 0,bound_min real DEFAULT NULL,bound_max real DEFAULT NULL) 
RETURNS bytea AS
$$

from pandas import DataFrame
from diffprivlib.models import KMeans
from pickle import dumps

if columns==None:
    all_columns="*"
else:
    all_columns=",".join(columns)

query=plpy.execute('SELECT %s FROM %s;' % (all_columns, plpy.quote_ident(table_name)))

data=[]
for i in query:
    data.append(i)
epsilon=epsi
if epsilon==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        epsilon=float(epsi_par)
    else:
        epsilon=1
df = DataFrame(data)
if bound_min!=None and bound_max!=None:
    kmeans = KMeans(epsilon=epsilon,bounds=(bound_min,bound_max),n_clusters=number_cluster).fit(df._get_numeric_data())
else:
    kmeans = KMeans(epsilon=epsilon,n_clusters=number_cluster).fit(df._get_numeric_data())  

return dumps(kmeans)
 
$$ LANGUAGE plpython3u;

/*
Return a GaussianNB private model encoded in a sequence of bytes storable in the database
Parameters
number_cluster : int
    number of cluster 
table_name_x : text
    name of the table from which to extract the training data
column_y: text
    name of the column from which to extract the labels
columns: text[]
    the names of the columns of the table table_name from which to extract the training data
table_name_y : text
    name of the table from which to extract the labels. If not specified it's equal to table_name_x.
epsi : real
    value of the parameter epsilon
bound_min, bound_max : real
    values of the parameters used to calibrate sensitivity
*/
CREATE OR REPLACE FUNCTION GaussianNB(table_name_x text,column_y text,
    columns text[] default NULL,table_name_y text DEFAULT NULL,epsi real DEFAULT 0,
    bound_min real DEFAULT NULL,bound_max real DEFAULT NULL) RETURNS bytea AS
$$

from pandas import DataFrame
from diffprivlib.models import GaussianNB
from pickle import dumps

if columns==None:
    all_columns="*"
else:
    all_columns=",".join(columns)

query=plpy.execute('SELECT %s FROM %s;' % (all_columns, plpy.quote_ident(table_name_x)))

data=[]
for i in query:
    data.append(i)
if table_name_y==None:
    y_table=table_name_x

query=plpy.execute('SELECT %s FROM %s;' % (column_y, plpy.quote_ident(y_table)))
y=[]
for i in query:
    y.append(i)

epsilon=epsi
if epsilon==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        epsilon=float(epsi_par)
    else:
        epsilon=1
df = DataFrame(data)
df2= DataFrame(y)
if bound_min!=None and bound_max!=None:
    gaussian = GaussianNB(epsilon=epsilon,bounds=(bound_min,bound_max)).fit(df._get_numeric_data(),df2)
else:
    gaussian = GaussianNB(epsilon=epsilon).fit(df._get_numeric_data(),df2)  

return dumps(gaussian)
$$ LANGUAGE plpython3u;



/*
Return a LogisticRegression private model encoded in a sequence of bytes storable in the database
Parameters
number_cluster : int
    number of cluster 
table_name_x : text
    name of the table from which to extract the training data
column_y: text
    name of the column from which to extract the labels
columns: text[]
    the names of the columns of the table table_name from which to extract the training data
table_name_y : text
    name of the table from which to extract the labels. If not specified it's equal to table_name_x.
epsi : real
    value of the parameter epsilon
bound_min, bound_max : real
    values of the parameters used to calibrate sensitivity
*/
CREATE OR REPLACE FUNCTION LogisticRegression(table_name_x text,column_y text,columns text[] default NULL,table_name_y text DEFAULT NULL,epsi real DEFAULT 0,bound_min real DEFAULT NULL,bound_max real DEFAULT NULL) RETURNS bytea AS
$$

from pandas import DataFrame
from diffprivlib.models import LogisticRegression
from pickle import dumps

if columns==None:
    all_columns="*"
else:
    all_columns=",".join(columns)

query=plpy.execute('SELECT %s FROM %s;' % (all_columns, plpy.quote_ident(table_name_x)))

data=[]
for i in query:
    data.append(i)
if table_name_y==None:
    y_table=table_name_x

query=plpy.execute('SELECT %s FROM %s;' % (column_y, plpy.quote_ident(y_table)))
y=[]
for i in query:
    y.append(i)

epsilon=epsi
if epsilon==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        epsilon=float(epsi_par)
    else:
        epsilon=1
df = DataFrame(data)
df2= DataFrame(y)
if bound_min!=None and bound_max!=None:
    gaussian = LogisticRegression(epsilon=epsilon,bounds=(bound_min,bound_max)).fit(df._get_numeric_data(),df2)
else:
    gaussian = LogisticRegression(epsilon=epsilon).fit(df._get_numeric_data(),df2)  

return dumps(gaussian)
$$ LANGUAGE plpython3u;

/*
Return a LinearRegression private model encoded in a sequence of bytes storable in the database
Parameters
number_cluster : int
    number of cluster 
table_name_x : text
    name of the table from which to extract the training data
column_y: text
    name of the column from which to extract the labels
columns: text[]
    the names of the columns of the table table_name from which to extract the training data
table_name_y : text
    name of the table from which to extract the labels. If not specified it's equal to table_name_x.
epsi : real
    value of the parameter epsilon
bound_min, bound_max : real
    values of the parameters used to calibrate sensitivity
*/
CREATE OR REPLACE FUNCTION LinearRegression(table_name_x text,column_y text,columns text[] default NULL,table_name_y text DEFAULT NULL,epsi real DEFAULT 0,bound_min real DEFAULT NULL,bound_max real DEFAULT NULL) RETURNS bytea AS
$$

from pandas import DataFrame
from diffprivlib.models import LinearRegression
from pickle import dumps

if columns==None:
    all_columns="*"
else:
    all_columns=",".join(columns)

query=plpy.execute('SELECT %s FROM %s;' % (all_columns, plpy.quote_ident(table_name_x)))

data=[]
for i in query:
    data.append(i)
if table_name_y==None:
    y_table=table_name_x

query=plpy.execute('SELECT %s FROM %s;' % (column_y, plpy.quote_ident(y_table)))
y=[]
for i in query:
    y.append(i)

epsilon=epsi
if epsilon==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        epsilon=float(epsi_par)
    else:
        epsilon=1
df = DataFrame(data)
df2= DataFrame(y)
if bound_min!=None and bound_max!=None:
    Linear = LinearRegression(epsilon=epsilon,bounds=(bound_min,bound_max)).fit(df._get_numeric_data(),df2)
else:
    Linear = LinearRegression(epsilon=epsilon).fit(df._get_numeric_data(),df2)  

return dumps(Linear)

$$ LANGUAGE plpython3u;
 

/*
Return an array containing the centroids of a given KMeans model
*/
CREATE OR replace FUNCTION get_kmeans_centroids(model bytea) RETURNS real[] AS
 
$$
 
from pandas import DataFrame
from pickle import loads
 
model_Py = loads(model)
ret = map(list, model_Py.cluster_centers_)
return ret
 
$$ LANGUAGE plpython3u;


/*
Return predictions of a model on an array
Parameters
model: bytea
    model to be used to make predictions 
input_value : float[]
    array containing feature values 
*/
CREATE OR replace FUNCTION predict(model bytea, input_values float[]) RETURNS text AS
$$
 
from pickle import loads
model_py = loads(model)
ret = model_py.predict(input_values)
return str(ret)

$$ LANGUAGE plpython3u;


/*
Return predictions of a model on a set of data extracted from database
Parameters
model: bytea
    model to be used to make predictions 
table_name_x : text
    name of the table from which to extract the data 
columns_x : text[]
    the names of the columns of the table table_name from which to extract the data
*/
CREATE OR replace FUNCTION predict(model bytea,table_name_x text,
    columns_x text[] default NULL) 
RETURNS text AS
$$
 
from pandas import DataFrame
from pickle import loads
model_py = loads(model)
if columns_x==None:
    all_columns="*"
else:
    all_columns=",".join(columns)

query=plpy.execute('SELECT %s FROM %s;' % (all_columns, plpy.quote_ident(table_name_x)))

data=[]
for i in query:
    data.append(i)
df = DataFrame(data)

ret = model_py.predict(df._get_numeric_data())
return str(ret)

$$ LANGUAGE plpython3u;


/*
Return the accuracy of a given model on a set of data extracted from database
Parameters
model : bytea
    model on which to calculate the accuracy
table_name_x : text
    name of the table from which to extract the data 
column_y : test
    name of the column from which to extract the labels
columns_x : text[]
    the names of the columns of the table table_name from which to extract the data
table_name_y : text
    name of the table from which to extract the labels. If not specified it's equal to table_name_x.
*/
CREATE OR replace FUNCTION score(model bytea,table_name_x text,column_y text,
    columns_x text[] default NULL,table_name_y text DEFAULT NULL) returns real AS

$$
from pickle import loads
from pandas import DataFrame
model_Py = loads(model)

if columns_x==None:
    all_columns="*"
else:
    all_columns=",".join(columns)

query=plpy.execute('SELECT %s FROM %s;' % (all_columns, plpy.quote_ident(table_name_x)))

data=[]
for i in query:
    data.append(i)
df = DataFrame(data)

if table_name_y==None:
    y_table=table_name_x
query=plpy.execute('SELECT %s FROM %s;' % (column_y, plpy.quote_ident(y_table)))
y=[]
for i in query:
    y.append(i)
df2= DataFrame(y)
return model_Py.score(df._get_numeric_data(),df2)

$$ LANGUAGE plpython3u;


/*
Create, using Differential Privacy, a new table which is the result of the reduction of dimensionality of a given table
Parameters
n_components: int
    number of features to be obtained after the transformation. If this parameter is greater than or equal to the number of features in the input table, an exception is raised.
table_name_input: text
    name of the table from which to extrapolate the data set on which you want to make the reduction in size.
table_name_output: text
    name of the table where to store the transformed data.
columns: text[]
    array of strings specifying table column names, from which to extract data. If a value is not passed for this parameter, all numeric columns of the table will be used by default.
epsi: real
    value of the epsilon parameter. If not set, the same priority order already described in the extension statistics module will be used.
bound_min7 bound_max. real
    value for the limits of computation.
*/
CREATE OR REPLACE FUNCTION PCA(n_components int,table_name_input text,table_name_output text,
    columns text[] default NULL,epsi real DEFAULT 0,bound_min real DEFAULT NULL,bound_max real DEFAULT NULL) 
RETURNS text AS
$$
import pandas as pd
from pandas import DataFrame
from diffprivlib.models import PCA

string="select count(*) from information_schema.columns  where table_name='"+table_name_input+"';"
query=plpy.execute(string)
numberColumnsTable=query[0]['count']
if n_components>=numberColumnsTable:
    raise Exception("Number of component must be inferior of number of features")

if columns==None:
    all_columns="*"
else:
    all_columns=",".join(columns)

query=plpy.execute('SELECT %s FROM %s;' % (all_columns, plpy.quote_ident(table_name_input)))
data=[]
for i in query:
    data.append(i)

epsilon=epsi
if epsilon==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        epsilon=float(epsi_par)
    else:
        epsilon=1

df=DataFrame(data)
if bound_min!=None and bound_max!=None:
    pca = PCA(n_components=n_components,epsilon=epsilon,bounds=(bound_min,bound_max)).fit_transform(df._get_numeric_data())
else:
    pca = PCA(n_components=n_components,epsilon=epsilon).fit_transform(df._get_numeric_data()) 

index=1
string="CREATE table "+table_name_output+" ("
while index<=n_components:
    string+='component'+str(index)+' real'
    if index!=n_components:
        string+=','
    index+=1
string+=');'
query=plpy.execute(string)

for row in pca:
    queryString='INSERT INTO '+table_name_output+' VALUES('
    for value in row:
        queryString+=str(value)+','
    queryString = queryString[:-1]+');'
    query=plpy.execute(queryString)
return 'Table '+table_name_output+' created!'
$$ LANGUAGE plpython3u;


/*
Create, using Differential Privacy, a new table which is the result of standardization of features by removing the mean and scaling the variance to the unit.
Parameters
table_name_input: text
    name of the table from which to extrapolate the data set on which you want to make the reduction in size.
table_name_output: text
    name of the table where to store the transformed data.
columns: text[]
    array of strings specifying table column names, from which to extract data. If a value is not passed for this parameter, all numeric columns of the table will be used by default.
epsi: real
    value of the epsilon parameter. If not set, the same priority order already described in the extension statistics module will be used.
bound_min7 bound_max. real
    value for the limits of computation.
*/
CREATE OR REPLACE FUNCTION StandardScaler(table_name_input text,table_name_output text,columns text[] default NULL,epsi real DEFAULT 0,bound_min real DEFAULT NULL,bound_max real DEFAULT NULL) RETURNS text AS
$$

import pandas as pd
from pandas import DataFrame
from diffprivlib.models import StandardScaler

if columns==None:
    all_columns="*"
else:
    all_columns=",".join(columns)

query=plpy.execute('SELECT %s FROM %s;' % (all_columns, plpy.quote_ident(table_name_input)))

data=[]
for i in query:
    data.append(i)


epsilon=epsi
if epsilon==0:
    epsi_par=plpy.execute("SELECT current_setting('diffpriv.epsi','t');")[0]['current_setting']
    if epsi_par!=None:
        epsilon=float(epsi_par)
    else:
        epsilon=1

df=DataFrame(data)

if bound_min!=None and bound_max!=None:
    standardScaler = StandardScaler(epsilon=epsilon,bounds=(bound_min,bound_max)).fit_transform(df._get_numeric_data())
else:
    standardScaler = StandardScaler(epsilon=epsilon).fit_transform(df._get_numeric_data()) 
string="select count(*) from information_schema.columns  where table_name='"+table_name_input+"';"
query=plpy.execute(string)

numberColumnsTable=query[0]['count']


index=1
string="CREATE table "+table_name_output+" ("
while index<=numberColumnsTable:
    string+='component'+str(index)+' real'
    if index!=n_components:
        string+=','
    index+=1
string+=');'
query=plpy.execute(string)



for row in standardScaler:
    queryString='INSERT INTO '+table_name_output+' VALUES('
    for value in row:
        queryString+=str(value)+','
    queryString = queryString[:-1]+');'
    query=plpy.execute(queryString)


return 'Table '+table_name_output+' created!'

$$ LANGUAGE plpython3u;