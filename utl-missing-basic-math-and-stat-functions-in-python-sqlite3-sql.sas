%let pgm=utl-missing-basic-math-and-stat-functions-in-python-sqllite3-sql;

Missing basic math and stat functions in python sqlite3 sql

     1 Python sqllite3 error (calculate standard deviation of numeric zipcode)
       PandaSQLException: (sqlite3.OperationalError) no such function: STDEV
     2 Python fix

  github
  https://tinyurl.com/y3x9v5mt
  https://github.com/rogerjdeangelis/utl-missing-basic-math-and-stat-functions-in-python-sqlite3-sql

  math and stat C extensions
  https://www.sqlite.org/contrib/download/extension-functions.c/download/extension-functions.c

  also
  https://github.com/rogerjdeangelis/utl-macros-used-in-many-of-rogerjdeangelis-repositories

Python's sqllite3 operates directly on panda dataframes, no server needed.
Python sqllite3 is very slow, much slower than sqllite3 in R.
In addition Python SQL is missing many math and stat functions, however R
has them built in.
WPS/SAS proc sql is much faster than either R or Python sqllite3.

SOAPBOX ON

  This criticism does not apply to the many excellent packages in R and Python.
  One can create great packages in assembler.

  R appears to be more mature than Python, SAS(1974 memeo manual) is more mature then R or Python.
  R is based on the S language developed by Bell Labs in 1975
  Python was developed in 1988?
  Like SAS, R has gone throgh much more scrutiny, enhancements and bug fixes over the years?

  I suspect at some point math and stat(even SQRT?) will become part of Python sqllite3
  implementation, Fixing issues like performance and enhancements take time.

SOAPBOX OFF


/*               _   _                             _ _ _ _       _____
/ |  _ __  _   _| |_| |__   ___  _ __    ___  __ _| | (_) |_ ___|___ /    ___ _ __ _ __ ___  _ __
| | | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | | | | __/ _ \ |_ \   / _ \ `__| `__/ _ \| `__|
| | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | | | | ||  __/___) | |  __/ |  | | | (_) | |
|_| | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|_|_|\__\___|____/   \___|_|  |_|  \___/|_|
    |_|    |___/                                |_|

*/

%utl_submit_wps64x("
proc python;
export data=sashelp.zipcode python=zipcode;
submit;
from pandasql import sqldf;
import pandas as pd;
zipLog=sqldf('''
   select
     stdev(zip) as stdevZip
   from
     zipcode
   ''');
print(zipLog);
endsubmit;
");

NOTE: (sqlite3.OperationalError) no such function: stdev

/*___                _   _                             _ _ _ _       _____   __ _
|___ \   _ __  _   _| |_| |__   ___  _ __    ___  __ _| | (_) |_ ___|___ /  / _(_)_  __
  __) | | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | | | | __/ _ \ |_ \ | |_| \ \/ /
 / __/  | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | | | | ||  __/___) ||  _| |>  <
|_____| | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|_|_|\__\___|____/ |_| |_/_/\_\
        |_|    |___/                                |_|

*/

/*---- I think all  of this ugly stuff is needed to fix python sqllite3  ----*/

%utl_submit_wps64x("
proc python;
export data=sashelp.zipcode python=zipcode;
submit;
from os import path;
import pandas as pd;
import numpy as np;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
import pandas as pd;
zipLog=pdsql('''
   select
     stdev(zip) as StdevZip
   from
     zipcode
     limit 10;
   ''');
print(zipLog);
endsubmit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* The PYTHON Procedure                                                                                                   */
/*                                                                                                                        */
/*           StdevZip                                                                                                     */
/*                                                                                                                        */
/* 0       28191.621359                                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/





























%utl_submit_wps64x("
options validvarname=any lrecl=32756;
libname sd1 'd:/sd1';
proc sql;select max(cnt) into :_cnt from (select count(nam) as cnt from sd1.have group by nam);quit;
%array(_unq,values=1-&_cnt);
proc python;
export data=sd1.have python=have;
submit;
print(have);
from os import path;
import pandas as pd;
import numpy as np;
import pandas as pd;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
want = pdsql('''
   select
      nam
     ,%do_over(_unq,phrase=%str(max(case when partition=? then score else NULL end) as score?),between=comma)
   from
      (select nam, score, row_number() OVER (PARTITION BY nam) as partition from have )
   group
     by nam
''');
print(want);
endsubmit;
run;quit;
"));



%utl_submit_wps64x("
proc python;
export data=sashelp.zipcode python=zipcode;
submit;
from pandasql import sqldf;
import pandas as pd;
zipLog=sqldf('''
   select
     stdev(zip) as logZip
   from
     zipcode
     limit 10;
   ''');
print(zipLog);
endsubmit;
");

PandaSQLException: (sqlite3.OperationalError) no such function: STDEV


%utl_submit_wps64x("
proc python;
export data=sashelp.zipcode python=zipcode;
submit;
from os import path;
import pandas as pd;
import numpy as np;
import pandas as pd;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
import pandas as pd;
zipLog=pdsql('''
   select
     stdev(zip) as StdevZip
   from
     zipcode
     limit 10;
   ''');
print(zipLog);
endsubmit;
");



from os import path;
import pandas as pd;
import numpy as np;
import pandas as pd;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
