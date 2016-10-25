// Databricks notebook source exported at Tue, 25 Oct 2016 11:11:35 UTC
// MAGIC %md #![Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark.png)
// MAGIC 
// MAGIC **Objective:**
// MAGIC Analyze Desktop vs Mobile traffic to English Wikipedia (continued)
// MAGIC 
// MAGIC **Time to Complete:**
// MAGIC 20 mins
// MAGIC 
// MAGIC **Data Source:**
// MAGIC pageviews_by_second (<a href="http://datahub.io/en/dataset/english-wikipedia-pageviews-by-second" target="_blank">255 MB</a>)
// MAGIC 
// MAGIC **Business Questions:**
// MAGIC 
// MAGIC * Question # 1) How many total incoming requests were to the mobile site vs the desktop site?
// MAGIC * Question # 2) What is the start and end range of time for the pageviews data? How many days of data is in the DataFrame?
// MAGIC * Question # 3) What is the avg/min/max for the number of requests received for Mobile and Desktop views?
// MAGIC * Question # 4) Which day of the week does Wikipedia get the most traffic?
// MAGIC * Question # 5) Can you visualize both the mobile and desktop site requests in a line chart to compare traffic between both sites by day of the week?
// MAGIC * Question # 6) Why is there so much more traffic on Monday vs. other days of the week?
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Give a DataFrame a human-readable name when caching
// MAGIC - Cast a String col type into a Timestamp col type
// MAGIC - Browse the Spark SQL API docs
// MAGIC - Learn how to use "Date time functions"
// MAGIC - Create and use a User Defined Function (UDF)
// MAGIC - Make a Databricks bar chart visualization
// MAGIC - Join 2 DataFrames
// MAGIC - Make a Matplotlib visualization

// COMMAND ----------

// MAGIC %md Attach to, and then restart your cluster first to clear out old memory caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC ![Restart Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/restart_cluster.png)

// COMMAND ----------

// MAGIC %md ####![Wikipedia Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_wikipedia_tiny.png) ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Continue Exploring Pageviews By Second**

// COMMAND ----------

// MAGIC %md In this notebook, we will continue exploring the Wikipedia pageviews by second data.

// COMMAND ----------

// MAGIC %md First, change the shuffle.partitions option to 3:

// COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "3")

// COMMAND ----------

// MAGIC %md We're going to read the raw pageviews-by-second data. This data is stored in a TSV (tab-separated value), so we can read it using 
// MAGIC the add-on [Spark CSV](https://spark-packages.org/package/databricks/spark-csv) package. (Note: In Spark 2.0, this package is no longer an add-on; it's built-in.)

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/wikipedia-datasets/data-001/pageviews/raw

// COMMAND ----------

// MAGIC %md 
// MAGIC However, we don't want to let Spark CSV infer the schema, because that requires two passes over the data file. The file isn't huge, but it's more than 200 MB, so two passes is a little slow, especially for a class. So, we're going to specify the schema ourselves. Let's take a quick look at the first couple lines of the file:

// COMMAND ----------

display(
  sqlContext.read.text("dbfs:/databricks-datasets/wikipedia-datasets/data-001/pageviews/raw/pageviews_by_second.tsv")
)

// COMMAND ----------

// MAGIC %md Okay, so the fields appear to be:
// MAGIC 
// MAGIC * The timestamp, in ISO 8601 format. Unfortunately, Spark SQL can't parse this automatically, so we'll have to treat it as a string and do something ugly (and tricky) to make it into a timestamp.
// MAGIC * The site, which is just "mobile" or "desktop". So that's obviously a string.
// MAGIC * The request count, which is an integer.
// MAGIC 
// MAGIC We can use this information to craft our own schema, as shown below.

// COMMAND ----------

import org.apache.spark.sql.types._
val schema = StructType(
  List(
    StructField("timestamp", StringType, true),
    StructField("site", StringType, true),
    StructField("requests", IntegerType, true)
  )
)


// COMMAND ----------

// MAGIC %md As noted above, Spark SQL cannot parse ISO 8601 timestamps. _We_ can, using a `java.text.SimpleDateFormat` format string, but there's no `date_parse` SQL function in the Spark SQL API.
// MAGIC However, there _is_ a way we can parse it in this case:
// MAGIC 
// MAGIC 1. First, we can use the Spark SQL `unix_timestamp` function, along with
// MAGIC a [SimpleDateFormat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html), to parse the ISO 8601 timestamp
// MAGIC into a Unix timestamp (a long integer representing the number of seconds since January 1, 1970).
// MAGIC 
// MAGIC 2. Second, we can cast the Unix timestamp to a timestamp. (This is a conversion that Spark understands.)

// COMMAND ----------

import org.apache.spark.sql.functions._
val df = sqlContext.read
                   .format("com.databricks.spark.csv")
                   .option("delimiter", "\t")
                   .option("header", "true")
                   .schema(schema)
                   .load("dbfs:/databricks-datasets/wikipedia-datasets/data-001/pageviews/raw/pageviews_by_second.tsv")
                   .select($"site", $"requests", unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp").as("timestamp"))
df.show()

// COMMAND ----------

df.show(truncate=false)

// COMMAND ----------

// MAGIC %md Now, let's put the data in a more useful order.

// COMMAND ----------

val pageviewsDF = df.orderBy($"timestamp", $"site".desc)

// COMMAND ----------

// MAGIC %md Register the Temporary Table and use sqlContext's `cacheTable()` method to give the DataFrame a human-readable name in the Storage UI:

// COMMAND ----------

pageviewsDF.registerTempTable("pageviews_by_second_ordered")
sqlContext.cacheTable("pageviews_by_second_ordered")

// COMMAND ----------

// MAGIC %md Materialize the cache with a `count()` action:

// COMMAND ----------

pageviewsDF.count // materialize the cache

// COMMAND ----------

// MAGIC %md You should now see the DataFrame in the Storage UI:

// COMMAND ----------

// MAGIC %md ![Clean Name and 3 partitions](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/pageviews_cleanname_3partitions.png)

// COMMAND ----------

// MAGIC %md Look at the first 6 rows:

// COMMAND ----------

pageviewsDF.show(6)

// COMMAND ----------

// MAGIC %md Verify that the DataFrame is indeed in memory by running a count again:

// COMMAND ----------

// This should run in less than a second
pageviewsDF.count

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-1) How many total incoming requests were to the mobile site vs the desktop site?

// COMMAND ----------

// MAGIC %md First, let's sum up the `requests` column to see how many total requests are in the dataset. We've already imported the SQL functions package, which includes statistical functions like `sum`, `max`, `min`, `avg`, etc.

// COMMAND ----------

pageviewsDF.select(sum($"requests")).show()

// COMMAND ----------

// MAGIC %md So, there are about 13.3 billion requests total.

// COMMAND ----------

// MAGIC %md But how many of the requests were for the mobile site?

// COMMAND ----------

// MAGIC %md **Challenge 1:** Using just the commands we learned so far, can you figure out how to filter the DataFrame for just **mobile** traffic and then sum the requests column?

// COMMAND ----------

pageviewsDF.filter($"site" === "mobile").select(sum($"requests")).show()

// COMMAND ----------

// MAGIC %md **Challenge 2:** What about the **desktop** site? How many requests did it get?

// COMMAND ----------

pageviewsDF.filter($"site" === "desktop").select(sum($"requests")).show()

// COMMAND ----------

// MAGIC %md So, about twice as many were for the desktop site.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-2) What is the start and end range of time for the pageviews data? How many days of data is in the DataFrame?

// COMMAND ----------

// MAGIC %md To answer this, we should first convert the `timestamp` column from a `String` type to a `Timestamp` type. Currently the first column of `pageviewsDF` is typed as a string:

// COMMAND ----------

pageviewsDF.printSchema()

// COMMAND ----------

// MAGIC %md Create a new DataFrame, `pageviewsOrderedByTimestampDF`, that changes the timestamp column from a `string` data type to a `timestamp` data type:

// COMMAND ----------

val pageviewsTimestampDF = pageviewsDF.select($"timestamp".cast("timestamp"), $"site", $"requests")

// COMMAND ----------

pageviewsTimestampDF.printSchema()

// COMMAND ----------

pageviewsTimestampDF.show(10)

// COMMAND ----------

// MAGIC %md How many different years is the data from?

// COMMAND ----------

// MAGIC %md For the next command, we'll use `year()`, one of the date time function available in Spark. You can review which functions are available for DataFrames in the <a href="https://spark.apache.org/docs/1.6.2/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">Spark API doc's SQL functions</a>, under "Date time functions".

// COMMAND ----------

pageviewsTimestampDF.select(year($"timestamp")).distinct().show()

// COMMAND ----------

// MAGIC %md The data only spans 2015. But which months?

// COMMAND ----------

// MAGIC %md **Challenge 3:** Can you figure out how to check which months of 2015 the data covers (using the <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">Spark API docs</a>)?

// COMMAND ----------

pageviewsTimestampDF.select(year($"timestamp"), month($"timestamp"), d).distinct().show()

// COMMAND ----------

// MAGIC %md The data covers the months you see above.

// COMMAND ----------

// MAGIC %md Similarly, you can discover how many weeks of timestamps are in the data and how many days of data there is:

// COMMAND ----------

// How many weeks of data is there?
pageviewsTimestampDF.select(weekofyear($"timestamp")).distinct().show()

// COMMAND ----------

// How many days of data is there?
pageviewsTimestampDF.select(dayofyear($"timestamp")).distinct().count

// COMMAND ----------

// MAGIC %md There is 41 days of data.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-3) What is the avg/min/max for the number of requests received for Mobile and Desktop views?

// COMMAND ----------

// MAGIC %md To understand our data better, let's look at the average, minimum and maximum number of requests received for mobile, then desktop page views over every 1 second interval:

// COMMAND ----------

// Look at mobile statistics
pageviewsTimestampDF.filter("site = 'mobile'").select(avg($"requests"), min($"requests"), max($"requests")).show()

// COMMAND ----------

// Look at desktop statistics
pageviewsTimestampDF.filter("site = 'desktop'").select(avg($"requests"), min($"requests"), max($"requests")).show()

// COMMAND ----------

// MAGIC %md There certainly appears to be more requests for the desktop site.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-4) Which day of the week does Wikipedia get the most traffic?

// COMMAND ----------

// MAGIC %md Think about how we can accomplish this. We need to pull out the day of the week (like Mon, Tues, etc) from each row, and then sum up all of the requests by day.

// COMMAND ----------

// MAGIC %md First, use the `date_format` function to extract out the day of the week from the timestamp and rename the column as "Day of week".
// MAGIC 
// MAGIC Then we'll sum up all of the requests for each day and show the results.

// COMMAND ----------

// Notice the use of alias() to rename the new column
// "E" is a pattern in the SimpleDataFormat class in Java that extracts out the "Day in Week""

// Create a new DataFrame named pageviewsByDayOfWeekDF and cache it
val pageviewsByDayOfWeekDF = pageviewsTimestampDF.groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum()

// Cache the DataFrame with a human-readable name
pageviewsByDayOfWeekDF.registerTempTable("pageviews_by_DOW")
sqlContext.cacheTable("pageviews_by_DOW")

// Show what is in the new DataFrame
pageviewsByDayOfWeekDF.show()

// COMMAND ----------

// MAGIC %md You can learn more about date/time patterns, like "E", in the <a href="https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html" target="_blank">Java SimpleDateFormat</a> documentation.

// COMMAND ----------

// MAGIC %md It would help to visualize the results:

// COMMAND ----------

// Use orderBy() to sort by day of week
display(pageviewsByDayOfWeekDF.orderBy($"Day of week"))

// COMMAND ----------

// MAGIC %md Click on the Bar chart icon above to convert the table into a bar chart:
// MAGIC 
// MAGIC #![Bar Chart](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/barchart_icon.png)

// COMMAND ----------

// MAGIC %md Under the "Plot Options" button above, you might also need to set the Keys as "Day of week" and the values as "sum(requests)".

// COMMAND ----------

// MAGIC %md Hmm, the ordering of the days of the week is off, because the `orderBy()` operation is ordering the days of the week alphabetically. Instead of that, let's start with Monday and end with Sunday. To accomplish this, we'll write a short User Defined Function (UDF) to prepend each `Day of week` with a number.

// COMMAND ----------

// MAGIC %md ####![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **User Defined Functions (UDFs)**

// COMMAND ----------

// MAGIC %md A UDF lets you code your own logic for processing column values during a DataFrame query. 
// MAGIC 
// MAGIC First, let's create a Scala match expression for pattern matching:

// COMMAND ----------

def matchDayOfWeek(day:String): String = {
  day match {
    case "Mon" => "1-Mon"
    case "Tue" => "2-Tue"
    case "Wed" => "3-Wed"
    case "Thu" => "4-Thu"
    case "Fri" => "5-Fri"
    case "Sat" => "6-Sat"
    case "Sun" => "7-Sun"
    case _ => "UNKNOWN"
  }
}

// COMMAND ----------

// MAGIC %md Test the match expression:

// COMMAND ----------

matchDayOfWeek("Tue")

// COMMAND ----------

// MAGIC %md Great, it works! Now define a UDF named `prependNumber`:

// COMMAND ----------

val prependNumberUDF = sqlContext.udf.register("prependNumber", (s: String) => matchDayOfWeek(s))

// COMMAND ----------

// MAGIC %md Test the UDF to prepend the `Day of Week` column in the DataFrame with a number:

// COMMAND ----------

pageviewsByDayOfWeekDF.select(prependNumberUDF($"Day of week")).show(7)

// COMMAND ----------

// MAGIC %md Our UDF looks like it's working. Next, let's apply the UDF and also order the x axis from Mon -> Sun:

// COMMAND ----------

display((pageviewsByDayOfWeekDF.withColumnRenamed("sum(requests)", "total requests")
  .select(prependNumberUDF($"Day of week"), $"total requests")
  .orderBy("UDF(Day of week)")))

// COMMAND ----------

// See if there are outliers in dates
pageviewsTimestampDF.groupBy(date_format(($"timestamp"), "dd.MM.yyyy").alias("Day")).sum().orderBy($"sum(requests)".desc).show()

// COMMAND ----------

// The 20'th of april has almost double the request. Group by hour to see if there are outliers
pageviewsTimestampDF.filter(date_format(($"timestamp"), "dd.MM.yyyy") === "20.04.2015").groupBy(date_format(($"timestamp"), "dd.MM.yyyy HH").alias("hour")).sum().orderBy($"sum(requests)".desc).show()

// COMMAND ----------

// Apparently not, see if there are doubles (visually)
pageviewsTimestampDF.filter(date_format(($"timestamp"), "dd.MM.yyyy") === "20.04.2015").orderBy($"timestamp".desc).show(10)

// COMMAND ----------

// Yes, this must be the issue
pageviewsTimestampDF.select($"timestamp", $"site").filter(date_format(($"timestamp"), "dd.MM.yyyy") === "20.04.2015").groupBy($"timestamp", $"site").count().filter($"count" === 2).count()

// COMMAND ----------

// MAGIC %md Click on the bar chart icon again to convert the above table into a Bar Chart. Also, under the Plot Options, you may need to set the Keys as "UDF(Day of week)" and the values as "total requests".

// COMMAND ----------

// MAGIC %md Wikipedia seems to get significantly more traffic on Mondays than other days of the week. Hmm...

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-5) Can you visualize both the mobile and desktop site requests in a line chart to compare traffic between both sites by day of the week?

// COMMAND ----------

// MAGIC %md First, graph the mobile site requests:

// COMMAND ----------

val mobileViewsByDayOfWeekDF = pageviewsTimestampDF.filter("site = 'mobile'").groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum().withColumnRenamed("sum(requests)", "total requests").select(prependNumberUDF($"Day of week"), $"total requests").orderBy("UDF(Day of week)").toDF("DOW", "mobile_requests")

// COMMAND ----------

// MAGIC %md Before we go any further, that is a lot of code strung together... let's break it down just a little bit...

// COMMAND ----------

val mobileDF = pageviewsTimestampDF.filter("site = 'mobile'")
val groupedData = mobileDF.groupBy(date_format(($"timestamp"), "E").alias("Day of week"))
val sumDF = groupedData.sum().withColumnRenamed("sum(requests)", "total requests")
val selectDF = sumDF.select(prependNumberUDF($"Day of week"), $"total requests")
val orderedDF = selectDF.orderBy("UDF(Day of week)")
val mobileViewsByDayOfWeekDF = orderedDF.toDF("DOW", "mobile_requests")

// Cache this DataFrame
mobileViewsByDayOfWeekDF.cache()

display(mobileViewsByDayOfWeekDF)

// COMMAND ----------

// MAGIC %md Click on the bar chart icon again to convert the above table into a Bar Chart. 
// MAGIC 
// MAGIC Also, under the Plot Options, you may need to set the Keys as "DOW" and the values as "mobile requests".

// COMMAND ----------

// MAGIC %md With a DataFrame for mobile views, let's create one more for desktops:

// COMMAND ----------

val desktopViewsByDayOfWeekDF = pageviewsTimestampDF
  .filter("site = 'desktop'")
  .groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum().withColumnRenamed("sum(requests)", "total requests")
  .select(prependNumberUDF($"Day of week"), $"total requests")
  .orderBy("UDF(Day of week)")
  .toDF("DOW", "desktop_requests")

// Cache this DataFrame
desktopViewsByDayOfWeekDF.cache()

display(desktopViewsByDayOfWeekDF)

// COMMAND ----------

// MAGIC %md We now have two DataFrames: 
// MAGIC * **desktopViewsByDayOfWeekDF**
// MAGIC * **mobileViewsByDayOfWeekDF**
// MAGIC 
// MAGIC We can then perform a join on the two DataFrames to create a thrid DataFrame, **allViewsByDayOfWeekDF**

// COMMAND ----------

val allViewsByDayOfWeekDF = mobileViewsByDayOfWeekDF
  .join(desktopViewsByDayOfWeekDF, 
        mobileViewsByDayOfWeekDF("DOW") === desktopViewsByDayOfWeekDF("DOW"))

// COMMAND ----------

// MAGIC %md And lastly, we can create a line chart to visualize mobile vs. desktop page views:

// COMMAND ----------

display(allViewsByDayOfWeekDF)

// COMMAND ----------

// MAGIC %md Click on the line chart icon above to convert the table into a line chart:
// MAGIC 
// MAGIC #![Line Chart 1](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/linechart_1.png)

// COMMAND ----------

// MAGIC %md Then click on Plot Options:
// MAGIC 
// MAGIC #![Line Chart 2](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/linechart_2.png)

// COMMAND ----------

// MAGIC %md Finally customize the plot as seen below and click Apply:
// MAGIC 
// MAGIC *(You will have to drag and drop fields from the left pane into either Keys or Values)*
// MAGIC 
// MAGIC #![Line Chart 3](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/linechart_3.png)

// COMMAND ----------

// MAGIC %md Hmm, did you notice that the line chart is a bit deceptive? Beware that it looks like there were almost zero mobile site requests because the y-axis of the line graph starts from 600,000,000 instead of 0.
// MAGIC 
// MAGIC <img src="http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pageviews/linechart_4.png" style="height: 300px" alt="Line chart 4"/>

// COMMAND ----------

// MAGIC %md Since the y-axis is off, it may appear as if there were almost zero mobile site requests. We can restore a zero baseline by using Matplotlib. But first...

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) Q-6) Why is there so much more traffic on Monday vs. other days of the week?

// COMMAND ----------

// MAGIC %md ** Challenge 4:** Can you figure out exactly why there was so much more traffic on Mondays?

// COMMAND ----------

// Type your answer here...

// COMMAND ----------

// MAGIC %md ####![Databricks Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_databricks_tiny.png) Bonus: Matplotlib visualization

// COMMAND ----------

// MAGIC %md Let's use Matplotlib to fix the line chart visualization above so that the y-axis starts with 0.
// MAGIC 
// MAGIC Databricks notebooks let you move seemlessly between Scala and Python code within the same notebook by using `%python` to declare python cells:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # Create a function named simpleMath
// MAGIC def simpleMath(x, y):
// MAGIC   z = x + y
// MAGIC   print "z is: ", z

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC simpleMath(2, 3)

// COMMAND ----------

// MAGIC %md You can also import Matplotlib and easily create more sophisticated plots:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import numpy as np
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC fig1, ax = plt.subplots()
// MAGIC 
// MAGIC # The first list of four numbers is for the x-axis and the next list is for the y-axis
// MAGIC ax.plot([1,2,3,4], [1,4,9,16])
// MAGIC 
// MAGIC display(fig1)

// COMMAND ----------

// MAGIC %md Recall that we had earlier cached 2 DataFrames, one with desktop views by day of week and another with mobile views by day of week:

// COMMAND ----------

desktopViewsByDayOfWeekDF.show()

// COMMAND ----------

mobileViewsByDayOfWeekDF.show()

// COMMAND ----------

// MAGIC %md First let's graph only the desktop views by day of week:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fig2, ax = plt.subplots()
// MAGIC 
// MAGIC # Notice that we are providing the coordinate manually for the x-axis
// MAGIC ax.plot([0,1,2,3,4,5,6], [1566792176,1346947425,1346330702,1306170813,1207342832,1016427413,947169611], 'ro')
// MAGIC 
// MAGIC # The axis() command takes a list of [xmin, xmax, ymin, ymax] and specifies the viewport of the axes
// MAGIC ax.axis([0, 7, 0, 2000000000])
// MAGIC 
// MAGIC display(fig2)

// COMMAND ----------

// MAGIC %md Next graph only the mobile views by day of week:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fig3, ax = plt.subplots()
// MAGIC ax.plot([0,1,2,3,4,5,6], [790026669,648087459,631284694,625338164,635169886,646334635,629556455], 'bo')
// MAGIC 
// MAGIC # The axis() command takes a list of [xmin, xmax, ymin, ymax] and specifies the viewport of the axes
// MAGIC ax.axis([0, 7, 0, 2000000000])
// MAGIC 
// MAGIC display(fig3)

// COMMAND ----------

// MAGIC %md Finally, let's combine the 2 plots above and also programatically get the requests data from a DataFrame (instead of manually entering the y-axis corrdinates).
// MAGIC 
// MAGIC We need a technique to access the Scala DataFrames from the Python cells. To do this, we can register a temporary table in Scala, then call that table from Python.

// COMMAND ----------

mobileViewsByDayOfWeekDF.registerTempTable("mobileViewsByDOW")
desktopViewsByDayOfWeekDF.registerTempTable("desktopViewsByDOW")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC mobileViewsPythonDF = sqlContext.read.table("mobileViewsByDOW")
// MAGIC 
// MAGIC pythonListForMobileAll = [list(r) for r in mobileViewsPythonDF.collect()]
// MAGIC 
// MAGIC pythonListForMobileRequests = []
// MAGIC 
// MAGIC for item in pythonListForMobileAll:
// MAGIC         pythonListForMobileRequests.append(item[1])
// MAGIC 
// MAGIC pythonListForMobileRequests

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC desktopViewsPythonDF = sqlContext.read.table("desktopViewsByDOW")
// MAGIC 
// MAGIC pythonListForDesktopAll = [list(r) for r in desktopViewsPythonDF.collect()]
// MAGIC 
// MAGIC pythonListForDesktopRequests = []
// MAGIC 
// MAGIC for item in pythonListForDesktopAll:
// MAGIC         pythonListForDesktopRequests.append(item[1])
// MAGIC 
// MAGIC pythonListForDesktopRequests

// COMMAND ----------

// MAGIC %md We now have our two Python lists::

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC pythonListForMobileRequests

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC pythonListForDesktopRequests

// COMMAND ----------

// MAGIC %md Finally, we are ready to plot both Desktop and Mobile requests using our python lists:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fig3, ax = plt.subplots()
// MAGIC 
// MAGIC x_axis = [0,1,2,3,4,5,6]
// MAGIC 
// MAGIC ax.plot(x_axis, pythonListForDesktopRequests, marker='o', linestyle='--', color='r', label='Desktop')
// MAGIC ax.plot(x_axis, pythonListForMobileRequests, marker='o', label='Mobile')
// MAGIC 
// MAGIC ax.set_title('Desktop vs Mobile site requests')
// MAGIC 
// MAGIC ax.set_xlabel('Days of week')
// MAGIC ax.set_ylabel('# of requests')
// MAGIC 
// MAGIC ax.legend()
// MAGIC 
// MAGIC # The axis() command takes a list of [xmin, xmax, ymin, ymax] and specifies the viewport of the axes
// MAGIC ax.axis([0, 6, 0, 2000000000])
// MAGIC 
// MAGIC ax.xaxis.set_ticks(range(len(x_axis)), ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
// MAGIC 
// MAGIC display(fig3)
