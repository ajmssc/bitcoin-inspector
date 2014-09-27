Lab : calculate customer total
----

Objective :  Use mapreduce to calculate billing invoices

JDK API docs : http://docs.oracle.com/javase/7/docs/api/
Hadoop API docs : http://hadoop.apache.org/docs/stable/api/

project dir : mr-billing
you can also open the project in eclipse
(use  'mvn eclipse:eclipse'  to create eclipse project files)

== STEP 1)
    $  cd mr-billing
edit the file : src/main/java/hi/mr/BillingTotal.java
complete the TODO items

(Instructor : answer in  /labs-solutions/hadoop/mr/mr-billing)


== STEP 2) compile the code:
  $ cd mr-billing
  $ ../compile.sh
this should create a jar file called 'a.jar'

(alternatively  use 'mvn package' command to build the project too)


== STEP 3)
Now it is time to copy the sample input into HDFS
    $ hdfs dfs -mkdir -p  <your name>/billing/in
    $ hdfs dfs -put  ../../data/billing-data/sample.txt   <your name>/billing/in/


== STEP 4)
we will run this jar file
  $ hadoop jar a.jar  hi.mr.BillingTotal   <your name>/billing/in/sample.txt   <your name>/billing/out


== STEP 5)
Once the mr job is done, inspect the output file:
  $ hdfs  dfs -cat <your name>/billing/out/part-r-00000
or
Browse HDFS file system.  Navigate to '/user/<login name>/<your user name>/billing/out' dir


== STEP 6)
Once the sample data is working, lets try this on more data.

Generate more (random) sample data
    $  python ../../data/billing-data/gen-billing-data.py
This would generate a bunch of *.log files

Inspect a log file
    $  head  billing-2012-01-01.log

Quiz : How many records have we generated?

Now lets copy the newly generated log files into HDFS
    $  hdfs  dfs -put   *.log    <your name>/billing/in/

Verify the files are there
    $  hdfs  dfs -ls <your name>/billing/in


== STEP 7)
run mr again on this new data
  $ hadoop jar a.jar  hi.mr.BillingTotal   <your name>/billing/in   <your name>/billing/out2
note 1 : we are supplying an input dir (not a single file)
note 2 : specified a different output dir


== STEP 8)
inspect the output from JobTracker UI


== STEP 9)
examine the job stats from job tracker UI
Find the job under 'completed jobs' section
Click on it
inspect the stats
