# THIS PROJECT NEEDS A LOT OF REFACTORING -bryan

Main two files are:
MainHive.scala for Hive and Spark SQL
MainStreaming.scala for Spark Streaming

Note this project is cross built to allow D3.js on Scala.js, so instead of running with `sbt run` run with `sbt oiJVM/run` to run the main class for the jvm.
not satisfied with the current state of my P1, but I understand I have to share it right now still. still planning on adding improvements but current focus has been leading ptwo.

# Data Processing Layer
## = Spark ==
- `Dataframe` = `Dataset[row]`
- `spark.sql()` returns a `Dataframe`
## = Spark Streaming =
- Realtime ~= High Data **V**elocity
- `DStream` ~= a discrete stream aka sustained series of RDDs
  - `DStream` is Spark Streaming's core data type aka abstraction
  - `DStream` is created with `StreamingContext`

# Data Query Layer
## = Hive =
- `spark.sql()` returns a `Dataframe`

# Analytics Layer
## == MLlib ==
`Vector`

# Visualization Layer
## == D3.js ==
- Realtime ~= High Data **V**elocity
- Crossbuild 

===================================


![](https://i.imgur.com/3wzWB9o.png)













































































== More Advanced next step after MVP ==

Create MVP on CLI first then attemp cross-build:
  attempt cross-building Scala-js and Scala JVM to use Scala-js and the rich ecosystem of javascript libraries for front end UI 
  https://www.scala-js.org/doc/project/cross-build.html#:~:text=It%20is%20often%20desirable%20to,with%20the%20shared%20source%20code.
  back end in Scala JVM
  front end in Scala.js 
    (attempt D3.js if time allows as added feature after MVP)

Consider allowing my curiousity to take ahold of me and explore Spark MLlib
for the analytics, even if I have to generate random parameterized data just to practice its use.

==================================

P.S. I'm excited about the capabilities of Spark's StreamingContext (Spark Streaming) and its capabilities we will be diving into further later in training, enough so to where I'm planning on making Scala and Spark apart of my long term career path due to the implications of realtime data streaming and processing for reinforcement learning. 


===================================





# Are you aware of how you can visually present your scenario? (input and output : data and answer)

# CHALLENGE BEGINNINGS IN UNDERSTANDING THE DATA
"challenge is in the data,"
"understand your data before you start this,"
"the data is common amongst all of you"
  is there a primary key? are there any foreign keys?
  branchX is the foreign key

 *look at growth/acceleration, transactions declining prune, transactions inclining grow
# Future Query ~ [PREDICTOR] Query ~ [FORECAST] Query
#  ? IS THERE ANYTHING YOU UNDERSTAND ABOUT THE FUTURE OF THIS DATA ?
  ? after 2024 will Branch9 still have Triple_cappuccino?
  ? in the future should certain Branches be removed?
  ? is any one branch better than any two combined branches in the future"
  ? what all is needed so the company doesnt go into a loss ?

can add columns of data, last batch added the CITY column

graded dependent upon how you would enhance your project than everyone else
what addon?
  VISUALIZATION scala.js realtime
  "future query" THE 6th ONE
  "you have the same dataset, its up to you how you want to do the future query the 6th one"
  for the future prediction i did take additional columns

"in hive remove is a challenge"-m

SEND PROPOSAL ON FUTURE QUERY
actual proposal for p1 is what is your future query
"keep future query as challenge you want to implement"

Future Query
  ? after 2024 will Branch9 still have Triple_cappuccino?
  ? in the future should certain Branches be removed?
  ? is any one branch better than any two combined branches in the future"
 *look at growth/acceleration, transactions declining prune, transactions inclining grow
  if you feel the given data isnt enough, then you need to manipulate it
  a query about the future
  "DATE COMES INTO PLAY" "USE DATE AS A COLUMN"
  "DATE AS A COLUMN"

