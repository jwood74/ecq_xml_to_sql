## Example Options file
## Put your own options here

## this is a place you can set up the options of the parsing

## location
$location = '/home/user'


## database
$database = 'results'
## this is the database where all the resuls will be


## election prefix
$elec = 'elec'
## used to prefix each table
## e.g qld2015_ or fed2016_


## do you want to use a MySQL table to determine if it should loop or not?
$mysql_run = 'TRUE'
## it can just loop endlessly, or you could connect to MySQL table to check for RUN condition
## options are TRUE or FALSE


## XML location
$xml_url = 'http://results.ecq.qld.gov.au/elections/state/state2015/results/public.zip'
## the location of the XML file
## only used if you are downloading XML files


## reporting level
$report_level = 10
## used for debugging, this will print different logs throughout the code
## 0 == none	5 == medium		10 == lots


## MySQL username
$mysql_user = 'user'


## MySQL password
$mysql_pass = 'pass'