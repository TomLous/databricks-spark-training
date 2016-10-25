// Databricks notebook source exported at Tue, 25 Oct 2016 11:46:28 UTC
// MAGIC %md
// MAGIC 
// MAGIC ![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC ### Analyzing the Wikipedia PageCounts with RDDs and Datasets
// MAGIC #### Time to complete: 20 minutes
// MAGIC 
// MAGIC #### Business questions:
// MAGIC * Question # 1) How many unique articles in English Wikipedia were requested in the past hour?
// MAGIC * Question # 2) How many requests total did English Wikipedia get in the past hour?
// MAGIC * Question # 3) How many requests total did each Wikipedia project get total during this hour?
// MAGIC 
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Understand the difference between Dataframes, RDDs and Datasets
// MAGIC * Learn how to use the following RDD actions: `count`, `take`, `takeSample`, `collect`
// MAGIC * Learn the following RDD transformations: `filter`, `map`, `groupByKey`, `reduceByKey`, `sortBy`
// MAGIC * Learn how to convert your RDD code to Datasets
// MAGIC * Learn how to cache an RDD or Dataset and view its number of partitions and total size in memory
// MAGIC * Learn advanced persistence options like MEMORY_AND_DISK
// MAGIC * Learn how to define a case class to organize data in an RDD or Dataset into objects

// COMMAND ----------

// MAGIC %md Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

// COMMAND ----------

// MAGIC %fs ls /mnt/wikipedia-readonly/pagecounts/staging/

// COMMAND ----------

// MAGIC %md Notice that the file name has the date and time of when the file was created by the Wikimedia Foundation. This file contains recent web traffic data to Wikipedia, that is less than 1 hour old. It captures 1 hour of page counts to all of Wikipedia languages and projects.

// COMMAND ----------

// MAGIC %md
// MAGIC ### RDDs
// MAGIC RDDs can be created by using the Spark Context object's `textFile()` method.

// COMMAND ----------

// In Databricks, the SparkContext is already created for you as the variable sc
sc

// COMMAND ----------

// MAGIC %md Create an RDD from the recent pagecounts file:

// COMMAND ----------

// Notice that this returns a RDD of Strings
val pagecountsRDD = sc.textFile("dbfs:/mnt/wikipedia-readonly/pagecounts/staging/")

// COMMAND ----------

// MAGIC %md The `count` action counts how many items (lines) total are in the RDD (this requires a full scan of the file):

// COMMAND ----------

pagecountsRDD.count()

// COMMAND ----------

// MAGIC %md The Spark UI will show that just one task read the entire file and the Input column should match the size of the file. For example, if the file were 72.4 MB, you would see:
// MAGIC #![1 task](http://i.imgur.com/Xu9LjbU.png)

// COMMAND ----------

// MAGIC %md So the count shows that there are about 5 - 9 million lines in the file. Notice that the `count()` action took 3 - 25 seconds to run b/c it had to read the entire file remotely from S3.

// COMMAND ----------

// MAGIC %md ** Challenge 1:**  Why is only one task being used to read this file? If the S3 input split is 64 MB, then why aren't two tasks being used? 

// COMMAND ----------

// Speculate upon your answer here

// COMMAND ----------

// MAGIC %md You can use the take action to get the first 10 records:

// COMMAND ----------

pagecountsRDD.take(10)

// COMMAND ----------

// MAGIC %md The take command is much faster because it does not have read the entire file, it only reads 10 lines:
// MAGIC 
// MAGIC #![1 task](http://i.imgur.com/MpYvzeA.png)

// COMMAND ----------

// MAGIC %md Unfortunately results returned by `.take(10)` are not very readable because `take()` returns an array and Scala simply prints the array with each element separated by a comma. 
// MAGIC 
// MAGIC We can make the output prettier by traversing the array to print each record on its own line *(the .foreach() here is NOT a Spark operation, it's a local Scala operator)*:

// COMMAND ----------

pagecountsRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md Notice that each line in the file actually contains 2 strings and 2 numbers, but our RDD is treating each line as a long string. We'll fix this typing issue shortly by using a custom parsing function.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Datasets
// MAGIC Datasets can be created by using the SQL Context object's `read.text()` method:

// COMMAND ----------

// Notice that this returns a Dataset of Strings
val pagecountsDS = sqlContext.read.text("dbfs:/mnt/wikipedia-readonly/pagecounts/staging/").as[String]

// COMMAND ----------

// Notice that you get an array of Strings back
pagecountsDS.take(10)

// COMMAND ----------

pagecountsDS.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Caching RDDs vs Datasets in memory
// MAGIC Next, let's cache both the `pagecountsRDD` and `pagecountsDS` into memory and see how much space they take.

// COMMAND ----------

import org.apache.spark.storage.StorageLevel._

// COMMAND ----------

pagecountsRDD.setName("pagecountsRDD").persist(MEMORY_AND_DISK).count

// COMMAND ----------

pagecountsDS.persist(MEMORY_AND_DISK).count

// COMMAND ----------

// MAGIC %md The Spark UI's Storage tab now shows both in memory. Notice that the Dataset is compressed in memory by default, so it takes up much less space *(your exact size numbers will vary depending how the last hours's file size)*:
// MAGIC 
// MAGIC #![DS vs RDD](http://i.imgur.com/RsDpcD8.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pagecount Parsing Function
// MAGIC 
// MAGIC Storing each line in the file as a String item in the RDD or Dataset is not the most effective solution, since each line actually has 4 fields in it. 
// MAGIC 
// MAGIC Let's define a function, `parse`, to parse out the 4 fields on each line. Then we'll run the parse function on each item in the RDD or Dataset and create a new RDDs and Datasets with the correct types for each item.

// COMMAND ----------

// Define a parsing function that takes in a line string and returns the 4 fields on each line, correctly typed
def parse(line:String) = {
  val fields = line.split(' ') //Split the original line with 4 fields according to spaces
  (fields(0), fields(1), fields(2).toInt, fields(3).toLong) // return the 4 fields with their correct data types
}

// COMMAND ----------

// Now we get back a RDD with the correct types, each line has 2 strings and 2 numbers
val pagecountsParsedRDD = pagecountsRDD.map(parse)

// COMMAND ----------

// Here we get back a Dataset with the correct types, each line has 2 strings and 2 numbers
val pagecountsParsedDS = pagecountsDS.map(parse)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Revisiting caching RDDs vs Datasets in memory
// MAGIC Next, let's cache both the new `pagecountsParsedRDD` and `pagecountsParsedDS` into memory and see how much space they take.

// COMMAND ----------

pagecountsRDD

// COMMAND ----------

pagecountsRDD.unpersist()

// COMMAND ----------

//90 seconds to run
pagecountsParsedRDD.setName("pagecountsParsedRDD").persist(MEMORY_AND_DISK).count

// Note that this will go to disk in Spark UI b/c it doesn't fit in memory!

// COMMAND ----------

pagecountsDS

// COMMAND ----------

pagecountsDS.unpersist()

// COMMAND ----------

//50 sec to run
pagecountsParsedDS.persist(MEMORY_AND_DISK).count

// 485.5 MB fits in memory

// COMMAND ----------

// MAGIC %md Notice that the Parsed RDD is more costly in storage (compared to the first RDD) but  the Parsed Dataset is cheaper to store in memory (compared to the first Dataset).
// MAGIC 
// MAGIC This is because of the way Java objects are represented normally in memory. When using RDDs, Java objects are many times larger than their underlying fields, with a bunch of data structures and pointers floating around. 
// MAGIC 
// MAGIC Consider the fact that a 4 byte string with UTF-8 encoding in Java actually ends up taking 48 bytes of memory in the JVM.
// MAGIC 
// MAGIC However, Project Tungsten's UnsafeRow format is far more efficient and operates directly on binary data rather than Java objects by using `sun.misc.Unsafe`. Learn more about Project Tungsten via <a href="https://www.youtube.com/watch?v=5ajs8EIPWGI" target="_blank">Josh Rosen's YouTube video</a> and the Reynold and Josh's <a href="https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html" target="_blank">Databricks blog post</a>.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #1: 
// MAGIC ** How many unique articles in English Wikipedia were requested in the past hour?**

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

// MAGIC %md Let's filter out just the lines referring to English Wikipedia:

// COMMAND ----------

// Note: _._1 is just scala syntax for yanking out the first element from each line
val enPagecountsRDD = pagecountsParsedRDD.filter(_._1 == "en")

// COMMAND ----------

// MAGIC %md Note that the above line is lazy and doesn't actually run the filter. We have to trigger the filter transformation to run by calling an action:

// COMMAND ----------

enPagecountsRDD.count()

// COMMAND ----------

// MAGIC %md Around 2 million lines refer to the English Wikipedia project. So about half of the 5 million articles in English Wikipedia get requested per hour. Let's take a look at 10 random lines:

// COMMAND ----------

// 83 seconds to run
enPagecountsRDD.takeSample(true, 10).foreach(println)

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

// MAGIC %md Running a filter and count on a Dataset looks very similar:

// COMMAND ----------

val enPagecountsDS = pagecountsParsedDS.filter(_._1 == "en")

// COMMAND ----------

enPagecountsDS.count()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #2:
// MAGIC ** How many requests total did English Wikipedia get in the past hour?**

// COMMAND ----------

// MAGIC %md ##### RDD answer:

// COMMAND ----------

// MAGIC %md Start with the `enPagecountsRDD`:

// COMMAND ----------

//45 secs to run
enPagecountsRDD.take(5)

// COMMAND ----------

// MAGIC %md ** Challenge 2:** Can you figure out how to yank out just the requests column and then sum all of the requests?

// COMMAND ----------

enPagecountsRDD.map(_._3).sum

// COMMAND ----------

// Type your answer here... Then build upon that by summing up all of the requests

// COMMAND ----------

// MAGIC %md We can see that there were between 5 - 10 million requests to English Wikipedia in the past hour.

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

// MAGIC %md Let's re-write the same query using Datasets:

// COMMAND ----------

// The map() operation looks the same as the RDD version
enPagecountsDS.map(x => x._3).take(5)

// COMMAND ----------

// MAGIC %md Note that there is no available `.sum()` method on Datasets:

// COMMAND ----------

// This will return an error in 90 sec
enPagecountsDS.map(_._3).reduce(_ + _)

// COMMAND ----------

// MAGIC %md The Datasets API in Spark 1.6 is still experimental, so full functionality is not available yet.

// COMMAND ----------

// MAGIC %md #### Strategy #1) Collect on Driver and sum locally

// COMMAND ----------

// MAGIC %md Instead, if the data is small enough, we can collect it on the Driver and sum it locally.
// MAGIC 
// MAGIC ** Challenge 3:** Implement this new strategy of collecting the data on the Driver for the summation.

// COMMAND ----------

enPagecountsDS.map(x => x._3).collect.sum

// COMMAND ----------

// MAGIC %md Performance here may appear fast in a local mode cluster because no network transfer has to take place. Also, collecting data at the driver to perform a sum won't scale if the data set is too large to fit on one machine (which could cause an Out of Memory condition).

// COMMAND ----------

// MAGIC %md #### Strategy #2) Convert DS to a DF for the sum

// COMMAND ----------

// MAGIC %md Another strategy is to convert the Dataset to a Dataframe just to perform the sum.

// COMMAND ----------

// MAGIC %md ** Challenge 4:** See if you can start with the `enPagecountsDS` Dataset, run a map on it like above, then convert it to a Dataframe and sum the `value` column.

// COMMAND ----------

// Type your answer here...
import org.apache.spark.sql.functions._

// COMMAND ----------

enPagecountsDS 
  .map(x => x._3)
  .toDF
  .select(sum($"value"))
  .show()

// COMMAND ----------

// MAGIC %md #### Strategy #3) Implement a custom Aggregator for sum

// COMMAND ----------

// MAGIC %md In the final strategy, we construct a simple Aggregator that sums up a collection of `Int`s.

// COMMAND ----------

// MAGIC %md Aggregators provide a mechanism for adding up all of the elements in a Dataset, returning a single result. An Aggregator is similar to a User Defined Aggregate Function (UDAF), but the interface is expressed in terms of JVM objects instead of as a Row.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.TypedColumn

// COMMAND ----------

val simpleSum = new Aggregator[Int, Int, Int] with Serializable {
  def zero: Int = 0                     // The initial value.
  def reduce(b: Int, a: Int) = b + a    // Add an element to the running total
  def merge(b1: Int, b2: Int) = b1 + b2 // Merge intermediate values.
  def finish(b: Int) = b                // Return the final result.
}.toColumn

// COMMAND ----------

// Why is this so slow? This cell takes about 70 seconds to complete! We will optimize this next.
enPagecountsDS.map(x => x._3).select(simpleSum).collect

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Performance Optimization: Understanding the relationship between # of partitions and # of tasks

// COMMAND ----------

// MAGIC %md The slow Spark job above launches two stages and one task in each stage. Recall that each local mode cluster in Databricks has 4 slots, so 4 tasks can be run simultaneously.

// COMMAND ----------

// MAGIC %md Let's repartition the Dataset from 1 partition to 4 partitions so that we can run 4 tasks in parallel when analyzing it:

// COMMAND ----------

val pagecounts3PartitionsDS = pagecountsParsedDS.repartition(3).cache

// COMMAND ----------

pagecounts3PartitionsDS.count // Materialize the cache

// COMMAND ----------

// The same operations now complete in about 25 seconds, when reading from 3 partitions in memory
pagecounts3PartitionsDS.filter(_._1 == "en" ).map(x => x._3).select(simpleSum).collect

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #3:
// MAGIC ** How many requests total did each Wikipedia project get total during this hour?**

// COMMAND ----------

// MAGIC %md Recall that our data file contains requests to all of the Wikimedia projects, including Wikibooks, Wiktionary, Wikinews, Wikiquote... and all of the 200+ languages.

// COMMAND ----------

// MAGIC %md ##### Dataset answer:

// COMMAND ----------

// MAGIC %md Start by creating key/value pairs from the project prefix and the number of requests:

// COMMAND ----------

pagecounts3PartitionsDS.map(line => (line._1, line._3)).take(5)

// COMMAND ----------

pagecounts3PartitionsDS.map(line => (line._1, line._3)).groupBy(_._1).count().take(5)

// COMMAND ----------

// MAGIC %md Since Datasets are still an experimental API and aggregations/sorting are not yet fully supported, let's switch the Dataset to a Dataframe for an aggregation:

// COMMAND ----------

pagecounts3PartitionsDS
  .map(line => (line._1, line._3))     // yank out k/v pairs of the project and # of requests
  .toDF()                              // Convert to DataFrame to perform aggregation / sorting
  .groupBy($"_1")                      // Group the k/v pairs by the key (project name)
  .agg(sum($"_2") as "sumOccurances")   // Sum up how many occurrances there are of each project
  .orderBy($"sumOccurances".desc)      // Order in descening order
  .take(10)
  .foreach(println)
