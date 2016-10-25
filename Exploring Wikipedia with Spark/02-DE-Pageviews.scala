// Databricks notebook source exported at Tue, 25 Oct 2016 09:40:22 UTC
// MAGIC %md #![Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark.png)
// MAGIC 
// MAGIC **Objective:**
// MAGIC Analyze Desktop vs Mobile traffic to English Wikipedia
// MAGIC 
// MAGIC **Time to Complete:**
// MAGIC 30 mins
// MAGIC 
// MAGIC **Data Source:**
// MAGIC pageviews_by_second (<a href="http://datahub.io/en/dataset/english-wikipedia-pageviews-by-second" target="_blank">255 MB</a>)
// MAGIC 
// MAGIC **Business Questions:**
// MAGIC * Question # 1) How many rows in the table refer to mobile vs desktop?
// MAGIC 
// MAGIC **Engineering Questions:**
// MAGIC * How is the data partitioned? Why is it partitioned the way it is?
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Upload a file to Databricks using the Tables UI (optional)
// MAGIC - Learn how Actions kick off Jobs + Stages
// MAGIC - Understand how DataFrame partitions relate to compute tasks
// MAGIC - Use Spark UI to monitor details of Job execution (input read, Shuffle, Storage UI, SQL visualization)
// MAGIC - Cache a DataFrame to memory (and learn how to unpersist it)
// MAGIC - Use the following transformations: `orderBy()`, `filter()`
// MAGIC - Catalyst Optimizer: How DataFrame queries are converted from a Logical plan -> Physical plan
// MAGIC - Configuration Option: `spark.sql.shuffle.partitions`

// COMMAND ----------

// MAGIC %md Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

// COMMAND ----------

// MAGIC %md ####![Wikipedia Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_wikipedia_tiny.png) **Introduction: Pageviews By Second**

// COMMAND ----------

// MAGIC %md Wikipedia.com is the 7th most popular website (measured by page views and unique visitors):

// COMMAND ----------

// MAGIC %md #![Top Ten Global Websites2](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/top_ten_websites.png)
// MAGIC 
// MAGIC Source: Alexa/Aug 2015: <a href="https://en.wikipedia.org/wiki/List_of_most_popular_websites" target="_blank">List_of_most_popular_websites</a>

// COMMAND ----------

// MAGIC %md In this notebook, we will analyze the traffic patterns to the desktop vs. mobile editions of English Wikipedia.
// MAGIC 
// MAGIC The Wikimedia Foundation has released 41 days of pageviews data starting March 16, 2015 at midnight. Two rows are collected every second:
// MAGIC - Desktop requests
// MAGIC - Mobile requests

// COMMAND ----------

// MAGIC %md ####![Databricks Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_databricks_tiny.png) **Pageviews By Second Table**
// MAGIC Before we do anything, let's see if our table "pageviews_by_second" even exists: run the following cell and we should get a **NoSuchTableException**.

// COMMAND ----------

val pageviewsDF = sqlContext.read.table("pageviews_by_second")

// COMMAND ----------

// MAGIC %md And just to make sure, let's take a look at the Databrick's Tables UI...

// COMMAND ----------

// MAGIC %md ####![Databricks Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_databricks_tiny.png) **Option 1: Upload Pageviews file from your PC to a Databricks Table**
// MAGIC 
// MAGIC One option is to "load" data directly with the Databrick's UI

// COMMAND ----------

// MAGIC %md ##### Step 1) Download the 46 MB .gz compressed pageviews file to your computer
// MAGIC Normally we would simply download the 46 MB .gz compressed pageviews file from <a href="https://datahub.io/en/dataset/english-wikipedia-pageviews-by-second/resource/6447ff71-bc5c-4c0a-b724-34e117151ee1" target="_blank">datahub.io</a>
// MAGIC 
// MAGIC **However**, so that we don't overwhelem the WiFi ware are instead going to use a smaller version of the same file: `pageviews_by_second_example.tsv`

// COMMAND ----------

// MAGIC %md ##### Step 2) Uncompress the pageviews file on your computer
// MAGIC Uncompress the pageviews file on your computer, which will result in a 255 MB CSV file
// MAGIC 
// MAGIC ![Pageviews file](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/pageviews_file.png)
// MAGIC 
// MAGIC **However**, our test file is already decompressed so we can skip this step.

// COMMAND ----------

// MAGIC %md ##### Step 3) Upload the CSV file to a Databricks table
// MAGIC 
// MAGIC To upload the uncompressed file to Databricks, click the **Tables** button, then **Create Table:**
// MAGIC 
// MAGIC ![Create Table](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/create_table.png)

// COMMAND ----------

// MAGIC %md Before moving forward, let's take a look at the various import options:
// MAGIC * File
// MAGIC * S3
// MAGIC * DBFS
// MAGIC * JDBC

// COMMAND ----------

// MAGIC %md On the "Data Import" page, make sure the Data Source is set to **File** and upload the file:
// MAGIC 
// MAGIC ![Data Import](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/data_import.png)

// COMMAND ----------

// MAGIC %md The upload should take 2-5 minutes, depending on your internet connection.
// MAGIC 
// MAGIC **However**, it should only take a couple of seconds with our example file, `pageviews_by_second_example.tsv`.

// COMMAND ----------

// MAGIC %md Once the file is uploaded, a "Preview Table" button will appear. Click **Preview Table**:
// MAGIC 
// MAGIC ![Preview Table](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/preview_table.png)

// COMMAND ----------

// MAGIC %md The "Table Details" will appear. There are 4 steps to do next:
// MAGIC 
// MAGIC 1) Enter the Table Name: **pageviews_by_second_example**
// MAGIC 
// MAGIC 2) Place a check next to **First row is header**
// MAGIC 
// MAGIC 3) Change the **requests** column data type from a STRING to an **INT**
// MAGIC 
// MAGIC 4) Click **Create Table**
// MAGIC 
// MAGIC ![Table Details](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/table_details.png)

// COMMAND ----------

// MAGIC %md Finally, you should see the new "pageviews_by_second_example" table, with its **Schema** and some **Sample Data**:
// MAGIC 
// MAGIC ![Table GUI](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/table_gui.png)

// COMMAND ----------

// MAGIC %md Using `sqlContext`, create a DataFrame named `exampleDF` by reading the table you uploaded:

// COMMAND ----------

val exampleDF = sqlContext.read.table("pageviews_by_second_example")

// COMMAND ----------

// MAGIC %md Look at the first 5 records in the DataFrame:

// COMMAND ----------

// Shows the first 5 records in ASCII print
exampleDF.show(5)

// COMMAND ----------

// MAGIC %md ####![Databricks Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_databricks_tiny.png) **Option 2: Use a premounted version of the data (from S3)**
// MAGIC Let's start by taking another look at what is on our file system: run the following cell and we should see all the datasets from Amazon S3 mounted into our shard:

// COMMAND ----------

// MAGIC %fs ls /mnt/wikipedia-readonly/

// COMMAND ----------

// MAGIC %md Next, let's take a look in our *pageviews* folder:

// COMMAND ----------

// MAGIC %fs ls /mnt/wikipedia-readonly/pageviews

// COMMAND ----------

// MAGIC %md We can import this file directly with the following command:

// COMMAND ----------

val tempDf = sqlContext.read
   .format("com.databricks.spark.csv")
   .option("header", "true")        // Use first line of all files as header
   .option("inferSchema", "true")   // Automatically infer data types
   .option("delimiter", "\t")       // Use tab delimiter (default is comma-separator)
   .load("/mnt/wikipedia-readonly/pageviews/pageviews_by_second.tsv")

tempDf.registerTempTable("pageviews_by_second")

// COMMAND ----------

// MAGIC %md Lastly, we can verify that the table was created by using `sqlContext` to create the `pageviewsDF` from the "temp" table "pageviews_by_second"

// COMMAND ----------

val pageviewsDF = sqlContext.read.table("pageviews_by_second")

// COMMAND ----------

// MAGIC %md And then we can take a look at the first 10 records.

// COMMAND ----------

pageviewsDF.show(10)

// COMMAND ----------

// MAGIC %md Next, take note that the timestamps and/or sites are out of order, we will dig into this more later.

// COMMAND ----------

// MAGIC %md Click the down arrows in the cell above to see that the `show()` action kicked off 1 job and 1 stage.
// MAGIC 
// MAGIC We will learn more about Jobs and Stages later in this lab. 

// COMMAND ----------

// MAGIC %md `printSchema()` prints out the schema for the table, the data types for each column and whether a column can be null:

// COMMAND ----------

pageviewsDF.printSchema

// COMMAND ----------

// MAGIC %md Notice above that the first 2 columns are typed as `string`, while the requests column holds an `integer`. 

// COMMAND ----------

// MAGIC %md ####![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Partitions and Tasks**

// COMMAND ----------

// MAGIC %md DataFrames are made of one or more partitions.  To see the number of partitions a DataFrame is made of:

// COMMAND ----------

pageviewsDF.rdd.partitions.size

// COMMAND ----------

// MAGIC %md The above cell first converts the DataFrame to an RDD, then calls the partitions method followed by the size method on the RDD. We will learn more about RDDs in a future lab. For now, just remember this handy trick to figure out the number of partitions in a DataFrame.

// COMMAND ----------

// MAGIC %md Here is what the DataFrame looks like:

// COMMAND ----------

// MAGIC %md ![4 partitions](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/4_partitions_dashed.png)

// COMMAND ----------

// MAGIC %md The dashed lines for the borders indicates that the DataFrame is still on disk and has not been cached into memory.

// COMMAND ----------

// MAGIC %md Count the number of records (rows) in the DataFrame:

// COMMAND ----------

pageviewsDF.count

// COMMAND ----------

// MAGIC %md Let's understand how Spark is actually computing the result of 7.2 million for the count action. It is important to understand the relationship between the number of partitions in a DataFrame and the number of tasks required to process a DataFrame.

// COMMAND ----------

// MAGIC %md In the cell above, where you ran the count, expand the Spark Jobs and Stages:
// MAGIC 
// MAGIC ![Expand Jobs and Stages](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/count_jobs_stages_tasks.png)

// COMMAND ----------

// MAGIC %md Each Spark action (like count) kicks off one or more Jobs. Above we see that one job (Job #2) was launched. *(your specific job # may be different)*
// MAGIC 
// MAGIC Each job is comprised of one or more Stages. Above we see that two stages (Stage #2 and #3) were launched to compute the result.

// COMMAND ----------

// MAGIC %md To learn more details about the Job and Stages, open the Spark UI in a new tab:

// COMMAND ----------

// MAGIC %md ![Open Spark UI](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/view_spark_ui.png)

// COMMAND ----------

// MAGIC %md When you go to the new "Spark UI" tab, you should see the Jobs page, with a few completed jobs. Click on the link under Description for the Job # used to run the count action:

// COMMAND ----------

// MAGIC %md ![Two Completed Jobs](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/three_completed_jobs.png)

// COMMAND ----------

// MAGIC %md On the "Details for Job #" page, you can now see several metrics about the count Job:

// COMMAND ----------

// MAGIC %md ![Two Stages Colored](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/two_stages_colored.png)

// COMMAND ----------

// MAGIC %md In the screenshot above, we can see (in purple) that Stage 2 (the first Stage for the count job) took 21 seconds to run, while Stage 3 only took 0.2 seconds.
// MAGIC 
// MAGIC Under the "Input" column, (in green) notice that Stage 2 read about 250 MB of data and (in orange) wrote 168 Bytes of shuffle data.
// MAGIC 
// MAGIC Under the "Shuffle Read" column, we can also see that Stage 3, (in orange) read the 168 Bytes of data that Stage 2 had written.
// MAGIC 
// MAGIC To learn more about the details of Stage 2, click the link (in red) under the Description column for Stage 2:

// COMMAND ----------

// MAGIC %md On the "Details for Stage 2" page, scroll all the way to the bottom till you see the 4 Tasks:

// COMMAND ----------

// MAGIC %md ![Stage-1, 4 tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/stageone_4tasks.png)

// COMMAND ----------

// MAGIC %md In the Tasks screenshot above, we can see (in green) that the first 3 tasks read 64 MB of the file, while the last task read 58 MB of the file. Also notice (in green) that the each of the 64 MB buffers that the first 3 tasks read was comprised of about 1.8 million records, but the last task that read 58 MB only read about 1.6 million records.
// MAGIC 
// MAGIC We can also see (in purple) that each task emitted a single 42 Byte record as the Shuffle Write.

// COMMAND ----------

// MAGIC %md When Spark reads CSV files from S3, the input split is 64 MB. That means that Spark will launch a new task/thread to read each 64 MB split of the file. 
// MAGIC 
// MAGIC In this case, after reading the first three input splits, only 58 MB remain, so the last task reads 58 MB:

// COMMAND ----------

// MAGIC %md ![64 MB input split](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/input_split.png)

// COMMAND ----------

// MAGIC %md Click back in your browser to return to the "Details of Job #" page, then click on the link under Description to see the details of the next Stage (Stage #3):

// COMMAND ----------

// MAGIC %md ![Click Stage 2](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/two_stages_clickstagetwo.png)

// COMMAND ----------

// MAGIC  %md Once again, scroll all the way to the bottom till you see the 1 Task for Stage 3:

// COMMAND ----------

// MAGIC %md ![Stage two, 1 task](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/stage2_onetask.png)

// COMMAND ----------

// MAGIC %md Notice in the screenshot above, that the single task in Stage 3 read 168 Bytes of data (4 records.)

// COMMAND ----------

// MAGIC %md The diagram below explains what's going on. The count Job is kicking off two stages. 
// MAGIC 
// MAGIC Stage 2 (the first stage of the job) has 4 tasks and each task reads between 1.6 million to 1.8 million records.
// MAGIC 
// MAGIC Each task in Stage 2 emits one record with the aggregated count that it saw in its local partition of data.
// MAGIC 
// MAGIC Then all four tasks in Stage 2 complete.
// MAGIC 
// MAGIC Stage 3 (the second stage of the job) starts with only one task. The task reads the 4 records from Stage 2 and performs a final aggregation and emits the number 7.2 million back to our Notebook cell as the final result of the computation!

// COMMAND ----------

// MAGIC %md ![Count, Physical Model](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/count_physicalmodelwjob.png)

// COMMAND ----------

// MAGIC %md ####![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Transformation: `orderBy()`**

// COMMAND ----------

// MAGIC %md The `orderBy()` transformation can be used to order the table by the timestamp column:

// COMMAND ----------

pageviewsDF
  .orderBy($"timestamp")  // transformation
  .show(10)               // action

// COMMAND ----------

// MAGIC %md The first 2 rows show data from March 16, 2015 at **00:00:00** (midnight). 
// MAGIC 
// MAGIC The 3rd and 4th rows show data from a second after midnight, **00:00:01**.
// MAGIC 
// MAGIC The DataFrame contains 2 rows for every second, one for desktop and one for mobile.

// COMMAND ----------

// MAGIC %md Did you notice that the first 6 rows in the DataFrame are ordered by `desktop`, then `mobile` traffic, but the last 4 rows are ordered by `mobile`, then `desktop`:

// COMMAND ----------

// MAGIC %md ![Out of Order](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/out_of_order.png)

// COMMAND ----------

// MAGIC %md The following command orders the rows by first the timestamp (ascending), then the site (descending) and then shows the first 10 rows again:

// COMMAND ----------

pageviewsDF.orderBy($"timestamp", $"site".desc).show(10)

// COMMAND ----------

// MAGIC %md The `orderBy()` transformation takes about 20 seconds to run against the 255 MB pageviews file on S3.

// COMMAND ----------

// MAGIC %md ####![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Reading from Disk vs Memory**

// COMMAND ----------

// MAGIC %md The 255 MB pageviews file is currently on S3, which means each time you scan through it, your Spark cluster has to read the 255 MB of data remotely over the network.

// COMMAND ----------

// MAGIC %md Once again, use the `count()` action to scan the entire 255 MB file from disk and count how many total records (rows) there are:

// COMMAND ----------

pageviewsDF.count

// COMMAND ----------

// MAGIC %md As we saw earlier, the pageviews DataFrame contains 7.2 million rows.

// COMMAND ----------

// MAGIC %md Hmm, that took about at least 20 seconds. Let's cache the DataFrame into memory to speed it up.

// COMMAND ----------

// This command finishes in less than half a second, because it doesn't actually do the caching yet

sqlContext.cacheTable("pageviews_by_second")

// COMMAND ----------

// MAGIC %md
// MAGIC Caching is a lazy operation (meaning it doesn't take effect until you call an action that needs to read all of the data). So let's call the `count()` action again:

// COMMAND ----------

// During this count() action, the data is read from S3 and counted, and also cached
// Note that when the count action has to also cache data, it takes longer than simply a count (like above)

pageviewsDF.count

// COMMAND ----------

// MAGIC %md
// MAGIC The DataFrame should now be cached, let's run another `count()` to see the speed increase:

// COMMAND ----------

pageviewsDF.count

// COMMAND ----------

// MAGIC %md Notice that scanning the DataFrame takes significantly faster!

// COMMAND ----------

// MAGIC %md Now that the pageviews DataFrame is cached in memory, if you go to the Spark UI tab and click on "Storage" (1 in image below) you'll see the "pageviews_by_second" DataFrame in memory:

// COMMAND ----------

// MAGIC %md ![Storage UI](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/storage_ui.png)

// COMMAND ----------

// MAGIC %md Notice above that the DataFrame is made of 4 partitions totaling 192 MB in size. 
// MAGIC 
// MAGIC The Storage Level for DataFrames is actually the new Tungsten Binary format.
// MAGIC 
// MAGIC Click on the DataFrame name link under the RDD Name column (2 in image above) to see more details about the DataFrame.

// COMMAND ----------

// MAGIC %md ![Storage UI](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/storage_ui_details.png)

// COMMAND ----------

// MAGIC %md Although the first 3 input splits read from S3 were 64 MB, when they got cached in memory using the Tungsten binary format, they became 49 MB each. The last 58 MB input split became a 44 MB partition in memory:

// COMMAND ----------

// MAGIC %md ![df in memory](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/df_in_mem.png)

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-1) How many rows in the table refer to mobile vs desktop?

// COMMAND ----------

// MAGIC %md Use the `filter()` transformation to keep only the rows where the site column is equal to mobile:

// COMMAND ----------

// How many rows refer to mobile?
pageviewsDF.filter($"site" === "mobile").count

// COMMAND ----------

// MAGIC %md Expand the Spark Jobs above and notice that even though we added a `filter()` transformation, the Job still requires 2 Stages with 4 tasks in the first Stage and 1 task in the second Stage:

// COMMAND ----------

// MAGIC %md ![Filter Count Expand](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/filter_count_expand.png)

// COMMAND ----------

// MAGIC %md **Challenge 1:** How many rows refer to desktop?

// COMMAND ----------

pageviewsDF.filter($"site" === "desktop").count

// COMMAND ----------

// MAGIC %md So, 3.6 million rows refer to the mobile page views and 3.6 million rows refer to desktop page views.

// COMMAND ----------

// MAGIC %md Let's compare the above `filter()` + `count()` from a Logical Model vs Physical Model perspective.
// MAGIC 
// MAGIC Reading a DataFrame from a Databricks table and running a filter() on it are both lazy operations, so technically no work is done yet:

// COMMAND ----------

// MAGIC %md ![Logical Model: Filter](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/filter_lazy.png)

// COMMAND ----------

// MAGIC %md However, when you call the count() action, it triggers the read from S3, and the filter() and count() to run:

// COMMAND ----------

// MAGIC %md ![Logical Model: Filter and Count](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/filter_count_run.png)

// COMMAND ----------

// MAGIC %md The Physical Model looks different. The filter() + count() job kicks off 2 Stages:

// COMMAND ----------

// MAGIC %md ![Physical Model: Filter and Count](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/filter_physical_model.png)

// COMMAND ----------

// MAGIC %md Each of the four tasks in the 1st Stage are actually doing 4 things:
// MAGIC - Read input split from S3
// MAGIC - Filter for just mobile or desktop traffic
// MAGIC - Do a local aggregation on the input split partition
// MAGIC - Write a single record to local SSD with the count # seen in the partition

// COMMAND ----------

// MAGIC %md ![Pipelining](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/pipelining.png)

// COMMAND ----------

// MAGIC %md ####![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) ** SQL Query Plan Visualization & the Catalyst Optimizer**

// COMMAND ----------

// MAGIC %md Recall that the last command we just ran above was:

// COMMAND ----------

//pageviewsDF.filter($"site" === "desktop").count

// COMMAND ----------

// MAGIC %md To see the SQL Query Plan for the `filter()` + `count()` query, click on the SQL tab in the Spark UI:

// COMMAND ----------

// MAGIC %md ![SQL viz](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/sql_viz.png)

// COMMAND ----------

// MAGIC %md In the diagram above, you can see that 7.2 million records are read from memory (green) to create the DataFrame, then filtered for just desktop (or mobile) traffic. The 3.6 million rows that pass the filter are projected out to an aggregator, which outputs 4 records to the Shuffle.
// MAGIC 
// MAGIC Everything above the TungstenExchange shuffle (in purple) is part of the 1st Stage. After the shuffle, in the 2nd stage, an aggregation is done on 4 input rows to emit 1 output row.

// COMMAND ----------

// MAGIC %md You can expand the "Details" in the SQL visualization UI to see the logical and physical plans:

// COMMAND ----------

// MAGIC %md ![SQL details](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/sql_details.png)

// COMMAND ----------

// MAGIC %md At the core of Spark SQL is the Catalyst optimizer, which all DataFrame, SQL and Dataset queries flow through to generate a physical plan that gets executed using RDDs:

// COMMAND ----------

// MAGIC %md ![Catalyst Optimizer](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/catalyst.png)

// COMMAND ----------

// MAGIC %md Catalyst is one of the newest and most technically involved components of Spark. It leverages advanced programming language features (e.g. Scala?s pattern matching and quasiquotes) in a novel way to build an extensible query optimizer. 
// MAGIC 
// MAGIC The main data type in Catalyst is a tree composed of zero or more child node objects. Trees can be manipulated using rules (functions that turn a tree into a new tree). You can read more about how Catalyst works in this Databricks blog post: <a href="https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html" target="_blank">April 2015: Deep Dive into Spark SQL?s Catalyst Optimizer</a>

// COMMAND ----------

// MAGIC %md ####![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Memory persistence and Shuffle Partitions **

// COMMAND ----------

// MAGIC %md Recall from the first notebook that your Spark local mode cluster is running with 3 slots:

// COMMAND ----------

// MAGIC %md ![Notebook + Micro Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/notebook_microcluster.png)

// COMMAND ----------

// MAGIC %md For best performance, we should cache DataFrames into memory with a number of partitions that is a multiple of the number of slots (3 or 6 or 9, etc). 
// MAGIC 
// MAGIC For example, here is a DataFrame in memory (orange) with 3 partitions:

// COMMAND ----------

// MAGIC %md ![Arch 3 slots](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/arch_3slots.png)

// COMMAND ----------

// MAGIC %md When running transformations on the DataFrame, all 3 partitions can be analyzed in parallel:

// COMMAND ----------

// MAGIC %md ![Arch 3 tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/arch_3tasks.png)

// COMMAND ----------

// MAGIC %md Three seems to be a more ideal number of partitions than four. 
// MAGIC 
// MAGIC First, unpersist the original base DataFrame, `pageviewsDF`. Then re-read the 255 MB file from S3, order it by the timestamp column, and re-cache it with 3 partitions:

// COMMAND ----------

pageviewsDF.unpersist

// COMMAND ----------

// MAGIC %md The Storage UI will now be empty:

// COMMAND ----------

// MAGIC %md ![Storage UI empty](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/storage_empty.png)

// COMMAND ----------

// MAGIC %md **Challenge 2:** Reload the table from S3, order it by the timestamp and site column (like above) and cache it:
// MAGIC 
// MAGIC Hint: Name the new DataFrame `pageviewsOrderedDF`

// COMMAND ----------

val pageviewsOrderedDF = sqlContext.read
   .format("com.databricks.spark.csv")
   .option("header", "true")        // Use first line of all files as header
   .option("inferSchema", "true")   // Automatically infer data types
   .option("delimiter", "\t")       // Use tab delimiter (default is comma-separator)
   .load("/mnt/wikipedia-readonly/pageviews/pageviews_by_second.tsv")
   .orderBy($"timestamp", $"site".desc)



// COMMAND ----------

pageviewsOrderedDF.cache()

// COMMAND ----------

// Materialize the cache
pageviewsOrderedDF.count

// COMMAND ----------

// MAGIC %md How many partitions are in the new DataFrame?

// COMMAND ----------

pageviewsOrderedDF.rdd.partitions.size

// COMMAND ----------

// MAGIC %md #### **200 Partitions!**

// COMMAND ----------

// MAGIC %md What could have happened? Expand the Job details in the `count()` command above by clicking the two down arrows:

// COMMAND ----------

// MAGIC %md ![200 tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/200tasks.png)

// COMMAND ----------

// MAGIC %md The first stage (Stage 17 above) is reading the 4 input splits from S3. The next Stage seems to be using 200 tasks to do the `orderBy()` transformation (in purple). This is when the DataFrame is being snapshotted and cached into memory. The final stage (Stage 19 above) is doing the final aggregation for the count.

// COMMAND ----------

// MAGIC %md By clicking on the Jobs tab in the Spark UI and then clicking into the details for the last job, you can see the same 3 stages:

// COMMAND ----------

// MAGIC %md ![3stages_200partitions.png](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/3stages_200partitions.png)

// COMMAND ----------

// MAGIC %md Notice that the first stage read 250 MB of input data (in green) and wrote 110 MB of shuffle data (in purple).
// MAGIC 
// MAGIC The second stage read the 110 MB of shuffle data from the earlier stage and wrote 8.2 KB of shuffle data (in orange).
// MAGIC 
// MAGIC The third stage read the 8.2 KB of shuffle data from the middle stage.

// COMMAND ----------

// MAGIC %md The trick to understanding what's going on lies in the following Spark SQL configuration option:

// COMMAND ----------

sqlContext.getConf("spark.sql.shuffle.partitions")

// COMMAND ----------

// MAGIC %md The option is set to 200. This configures the number of partitions to use when shuffling data for joins or aggregations.

// COMMAND ----------

// MAGIC %md Change the setting to 3:

// COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "3")

// COMMAND ----------

// MAGIC %md Verify the change:

// COMMAND ----------

sqlContext.getConf("spark.sql.shuffle.partitions")

// COMMAND ----------

// MAGIC %md Unpersist the DataFrame and re-run the read/orderBy/cache/count to store the DataFrame in memory with 3 partitions:

// COMMAND ----------

pageviewsOrderedDF.unpersist

// COMMAND ----------

val pageviewsOrderedDF = sqlContext.read.table("pageviews_by_second").orderBy($"timestamp", $"site".desc).cache

// COMMAND ----------

// Materialize the cache
pageviewsOrderedDF.count

// COMMAND ----------

// MAGIC %md Expand the Spark Jobs and Job # details in the cell above and note that this time the middle stage only used 3 tasks:

// COMMAND ----------

// MAGIC %md ![Middle Stage, 3 Tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/middlestage_3tasks.png)

// COMMAND ----------

// MAGIC %md Check the size of the DataFrame now:

// COMMAND ----------

pageviewsOrderedDF.rdd.partitions.size

// COMMAND ----------

// MAGIC %md If you drill into the details of the Spark UI's Storage tab and click on the RDD name, you will now see the DataFrame in memory with 3 partitions:

// COMMAND ----------

// MAGIC %md ![Storage UI, 3 tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/storage_3partitions.png)

// COMMAND ----------

// MAGIC %md You can learn more about the different configuration options in Spark SQL in the <a href="https://spark.apache.org/docs/latest/sql-programming-guide.html#other-configuration-options" target="_blank">Apache Spark Docs</a>.
