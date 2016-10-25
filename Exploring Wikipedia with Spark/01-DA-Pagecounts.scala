// Databricks notebook source exported at Tue, 25 Oct 2016 08:16:45 UTC
// MAGIC %md ![Wikipedia/Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark.png)  
// MAGIC 
// MAGIC #### Analyzing Traffic Patterns to Wikimedia Projects
// MAGIC 
// MAGIC **Objective:**
// MAGIC Study traffic patterns to all English Wikimedia projects from the past hour
// MAGIC 
// MAGIC **Time to Complete:**
// MAGIC 30 mins
// MAGIC 
// MAGIC **Data Source:**
// MAGIC Last hour's English Projects Pagecounts (~35 MB compressed parquet file)
// MAGIC 
// MAGIC **Business Questions:**
// MAGIC 
// MAGIC * Question # 1) How many different English Wikimedia projects saw traffic in the past hour?
// MAGIC * Question # 2) How much traffic did each English Wikimedia project get in the past hour?
// MAGIC * Question # 3) What were the 25 most popular English articles in the past hour?
// MAGIC * Question # 4) How many requests did the "Apache Spark" article recieve during this hour?
// MAGIC * Question # 5) Which Apache project received the most requests during this hour?
// MAGIC * Question # 6) What percentage of the 5.1 million English articles were requested in the past hour?
// MAGIC * Question # 7) How many total requests were there to English Wikipedia Desktop edition in the past hour?
// MAGIC * Question # 8) How many total requests were there to English Wikipedia Mobile edition in the past hour?
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Create a DataFrame
// MAGIC - Print the schema of a DataFrame
// MAGIC - Use the following Transformations: `select()`, `distinct()`, `groupBy()`, `sum()`, `orderBy()`, `filter()`, `limit()`
// MAGIC - Use the following Actions: `show()`, `count()`
// MAGIC - Learn about Wikipedia Namespaces

// COMMAND ----------

// MAGIC %md ####![Wikipedia Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_wikipedia_tiny.png) **Introduction: Wikipedia Pagecounts**

// COMMAND ----------

// MAGIC %md The Wikimedia Foundation releases hourly page view statistics for all Wikimedia projects and languages. The projects include Wikipedia, Wikibooks, Wikitionary, Wikinews, etc.
// MAGIC 
// MAGIC You can see the hourly dump files <a href="https://dumps.wikimedia.org/other/pagecounts-raw/" target="_blank">here</a>.

// COMMAND ----------

// MAGIC %md For example, <a href="https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-03/" target="_blank">here</a> is the hourly dumps webpage for March 2016:

// COMMAND ----------

// MAGIC %md ![Pagecounts Hourly Dumps](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/webpage_hourlydumps.png)

// COMMAND ----------

// MAGIC %md Each line in the pagecounts files contains 4 fields:
// MAGIC - Project name
// MAGIC - Page title
// MAGIC - Number of requests the page recieved this hour
// MAGIC - Total size in bytes of the content returned

// COMMAND ----------

// MAGIC %md ![Schema Explanation](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/schema_explanation.png)

// COMMAND ----------

// MAGIC %md In each line, the first column (like `en`) is the Wikimedia project name. The following abbreviations are used for the first column:
// MAGIC ```
// MAGIC wikipedia mobile: ".mw"
// MAGIC wiktionary: ".d"
// MAGIC wikibooks: ".b"
// MAGIC wikimedia: ".m"
// MAGIC wikinews: ".n"
// MAGIC wikiquote: ".q"
// MAGIC wikisource: ".s"
// MAGIC wikiversity: ".v"
// MAGIC mediawiki: ".w"
// MAGIC ```
// MAGIC 
// MAGIC Projects without a period and a following character are Wikipedia projects. So, any line starting with the column `en` refers to the English language Wikipedia (and can be requests from either a mobile or desktop client).
// MAGIC 
// MAGIC There will only be one line starting with the column `en.mw`, which will have a total count of the number of requests to English language Wikipedia's mobile edition. 
// MAGIC 
// MAGIC `en.d` refers to English language Wiktionary. 
// MAGIC 
// MAGIC `fr` is French Wikipedia. There are over 290 language possibilities.

// COMMAND ----------

// MAGIC %md ####![Databricks Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_databricks_tiny.png) **Databricks Hourly Job**

// COMMAND ----------

// MAGIC %md ![Hourly Job](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/hourly_job.png)

// COMMAND ----------

// MAGIC %md In Databricks, behind the scenes, there is an hourly job running that:
// MAGIC - downloads the latest pagecounts file from Wikimedia, 
// MAGIC - uncompresses the .gz file, 
// MAGIC - applies a filter to keep just the English language projects (`en`, `en.b`, `en.d`, etc)
// MAGIC - and writes it as 3 Parquet files (with a third of the data in each)
// MAGIC - to the following S3 folder:

// COMMAND ----------

// MAGIC %fs ls /mnt/wikipedia-readonly/pagecounts/staging_parquet_en_only/

// COMMAND ----------

// MAGIC %md The 3 files will be a total of about 25 - 40 MB, depending on the amount of traffic in the past hour.

// COMMAND ----------

// MAGIC %md ####![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Create a DataFrame**

// COMMAND ----------

// MAGIC %md A `sqlContext` object is your entry point for working with structured data (rows and columns) in Spark.
// MAGIC 
// MAGIC Let's use the `sqlContext` to create a DataFrame from the most recent pagecounts file:

// COMMAND ----------

val pagecountsEnAllDF = sqlContext.read.parquet("/mnt/wikipedia-readonly/pagecounts/staging_parquet_en_only/")

// COMMAND ----------

// MAGIC %md Look at the first 10 records in the DataFrame:

// COMMAND ----------

pagecountsEnAllDF.show(10, false)

// COMMAND ----------

// MAGIC %md `printSchema()` prints out the schema for the DataFrame, the data types for each column and whether a column can be null:

// COMMAND ----------

pagecountsEnAllDF.printSchema

// COMMAND ----------

// MAGIC %md Notice above that the first 2 columns are typed as `string`, but the requests column holds an `integer` and the bytes_served column holds a `long`.

// COMMAND ----------

// MAGIC %md Count the number of total records (rows) in the DataFrame:

// COMMAND ----------

pagecountsEnAllDF.count

// COMMAND ----------

// MAGIC %md So, there are between 2 - 3 million rows in the DataFrame. This includes traffic to not just English Wikipedia articles, but also possibly English Wiktionary, Wikibooks, Wikinews, etc.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Q-1) How many different English Wikimedia projects saw traffic in the past hour?**

// COMMAND ----------

pagecountsEnAllDF.select($"project").distinct().show()

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Q-2) How much traffic did each English Wikimedia project get in the past hour?**

// COMMAND ----------

// MAGIC %md The following command will show the total number of requests each English Wikimedia project received:

// COMMAND ----------

pagecountsEnAllDF
  .select($"project", $"requests")   //transformation
  .groupBy($"project")               //transformation
  .sum()                             //transformation
  .orderBy($"sum(requests)".desc)    //transformation
  .show()                            //action

// COMMAND ----------

// MAGIC %md English Wikipedia desktop (en) typically gets the highest number of requests, followed by English Wikipedia mobile (en.mw).

// COMMAND ----------

// MAGIC %md ####![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Transformations and Actions**

// COMMAND ----------

// MAGIC %md ####![Spark Operations](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/spark_ta.png)

// COMMAND ----------

// MAGIC %md DataFrames support two types of operations: *transformations* and *actions*.
// MAGIC 
// MAGIC Transformations, like `select()` or `filter()` create a new DataFrame from an existing one.
// MAGIC 
// MAGIC Actions, like `show()` or `count()`, return a value with results to the user. Other actions like `save()` write the DataFrame to distributed storage (like S3 or HDFS).

// COMMAND ----------

// MAGIC %md ####![Spark T/A](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/trans_and_actions.png)

// COMMAND ----------

// MAGIC %md Transformations contribute to a query plan,  but  nothing is executed until an action is called.

// COMMAND ----------

// MAGIC %md Consider opening the <a href="https://spark.apache.org/docs/1.6.2/api/scala/index.html#org.apache.spark.sql.DataFrame" target="_blank">DataFrame API docs</a> in a new tab to keep it handy as a reference. 
// MAGIC 
// MAGIC You can also hit 'tab' after the DataFrame name to see a drop down of the available methods:

// COMMAND ----------

// MAGIC %md ####![tab](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/tab.png)

// COMMAND ----------

pagecountsEnAllDF.explain

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Q-3) What were the 25 most popular English articles in the past hour?**

// COMMAND ----------

// MAGIC %md The `filter()` transformation can be used to filter a DataFrame where the language column is `en`, meaning English Wikipedia articles only:

// COMMAND ----------

// Only rows for for English Wikipedia (en) will pass this filter, removing projects like Wiktionary, Wikibooks, Wikinews, etc
val pagecountsEnWikipediaDF = pagecountsEnAllDF.filter($"project" === "en")

// COMMAND ----------

// MAGIC %md Notice above that transformations, like `filter()`, return back a DataFrame. 

// COMMAND ----------

// MAGIC %md The triple ===s in the `filter()` command above is Spark's way of doing equality checks on columns. 
// MAGIC 
// MAGIC You can learn more expression operators on columns (like !==, &&, <=, etc) in the <a href="https://spark.apache.org/docs/1.6.2/api/scala/index.html#org.apache.spark.sql.Column" target="_blank">Column API docs</a>.

// COMMAND ----------

// MAGIC %md Next, we can use the `orderBy()` transformation on the requests column to order the requests in descending order:

// COMMAND ----------

// Order by the requests column, in descending order
pagecountsEnWikipediaDF
  .orderBy($"requests".desc)   // transformation
  .show(25)                    // action

// COMMAND ----------

// MAGIC %md In Databricks, there is a special display function that displays a Dataframe in an HTML table:

// COMMAND ----------

// Display the DataFrame as a HTML table so it's easier to read
display(pagecountsEnWikipediaDF.orderBy($"requests".desc).limit(25))

// COMMAND ----------

// MAGIC %md Hmm, the result doesn't look correct. The article column contains non-articles, like: `Special:`, `File:`, `Category:`, `Portal`, etc. Let's learn about Namespaces so we can filter the non-articles out...

// COMMAND ----------

// MAGIC %md ####![Wikipedia Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_wikipedia_tiny.png) **Wikipedia Namespaces**

// COMMAND ----------

// MAGIC %md Wikipedia has many namespaces. The 5.1 million English articles are in the 0 namespace *(in red below)*. The other namespaces are for things like:
// MAGIC - Wikipedian User profiles (`User:` namespace 2)
// MAGIC - Files like images or videos (`File:` namespace 6)
// MAGIC - Draft articles not yet ready for publishing (`Draft:` namespace 118)
// MAGIC 
// MAGIC The hourly pagecounts file contains traffic requests to all Wikipedia namespaces. We'll need to filter out anything that is not an article.

// COMMAND ----------

// MAGIC %md ![namespaces](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/namespaces.png)
// MAGIC 
// MAGIC Source: <a href="https://en.wikipedia.org/wiki/Wikipedia:Namespace" target="_blank">Wikipedia:Namespace</a>

// COMMAND ----------

// MAGIC %md For example, here is the `User:` page for Jimmy Wales, a co-founder of Wikipedia:
// MAGIC 
// MAGIC ![User:jimbo_wales](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/user-jimbo_wales.png)
// MAGIC 
// MAGIC Source: <a href="https://en.wikipedia.org/wiki/User:Jimbo_Wales" target="_blank">User:Jimbo_Wales</a>

// COMMAND ----------

// MAGIC %md Which is different from the normal article page for Jimmy Wales *(this is the encyclopedic one)*:
// MAGIC 
// MAGIC ![article-jimmy_wales](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/article-jimmy_wales.png)
// MAGIC 
// MAGIC Source: <a href="https://en.wikipedia.org/wiki/Jimmy_Wales" target="_blank">Jimmy_Wales<a/>

// COMMAND ----------

// MAGIC %md Next, here is an image from the `File:` namespace of Jimmy Wales in 2010:
// MAGIC 
// MAGIC ![File:jimmy_wales_2010](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/file-jimmy_wales.png)
// MAGIC 
// MAGIC Source: <a href="https://en.wikipedia.org/wiki/File:Jimmy_Wales_July_2010.jpg" target="_blank">File:Jimmy_Wales_July_2010.jpg</a>

// COMMAND ----------

// MAGIC %md Let's filter out everything that is not an article:

// COMMAND ----------

// The 17 filters will remove everything that is not an article

val pagecountsEnWikipediaArticlesOnlyDF = pagecountsEnWikipediaDF
  .filter($"article".rlike("""^((?!Special:)+)""")) 
  .filter($"article".rlike("""^((?!File:)+)"""))  
  .filter($"article".rlike("""^((?!Category:)+)"""))  
  .filter($"article".rlike("""^((?!User:)+)""")) 
  .filter($"article".rlike("""^((?!Talk:)+)"""))  
  .filter($"article".rlike("""^((?!Template:)+)"""))  
  .filter($"article".rlike("""^((?!Help:)+)"""))  
  .filter($"article".rlike("""^((?!Wikipedia:)+)"""))  
  .filter($"article".rlike("""^((?!MediaWiki:)+)"""))  
  .filter($"article".rlike("""^((?!Portal:)+)"""))  
  .filter($"article".rlike("""^((?!Book:)+)"""))
  .filter($"article".rlike("""^((?!Draft:)+)"""))
  .filter($"article".rlike("""^((?!Education_Program:)+)"""))
  .filter($"article".rlike("""^((?!TimedText:)+)"""))
  .filter($"article".rlike("""^((?!Module:)+)"""))
  .filter($"article".rlike("""^((?!Topic:)+)"""))
  .filter($"article".rlike("""^((?!Images/)+)"""))
  .filter($"article".rlike("""^((?!%22//upload.wikimedia.org)+)"""))
  .filter($"article".rlike("""^((?!%22//en.wikipedia.org)+)"""))

// COMMAND ----------

// MAGIC %md Finally, repeat the `orderBy()` transformation from earlier:

// COMMAND ----------

display(pagecountsEnWikipediaArticlesOnlyDF.orderBy($"requests".desc).limit(25))

// COMMAND ----------

// MAGIC %md That looks better. Above you are seeing the 25 most requested English Wikipedia articles in the past hour!
// MAGIC 
// MAGIC This can give you a sense of what's popular or trending on the planet right now.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Q-4) How many requests did the "Apache Spark" article recieve during this hour? **

// COMMAND ----------

// MAGIC %md **Challenge 1: ** Can you figure out how to filter the `pagecountsEnWikipediaArticlesOnlyDF` DataFrame for just `Apache_Spark`?

// COMMAND ----------

pagecountsEnWikipediaArticlesOnlyDF.filter($"article" === "Apache_Spark").select($"requests").show()

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Q-5) Which Apache project received the most requests during this hour? **

// COMMAND ----------

// In the Regular Expression below:
// ^  - Matches beginning of line
// .* - Matches any characters, except newline

pagecountsEnWikipediaArticlesOnlyDF
//  .filter($"article".rlike("""^Apache_.*""")) 
 .filter($"article".startsWith("Apache_")) 
 .orderBy($"requests".desc)
 .show() // By default, show will return 20 rows

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Q-6) What percentage of the 5.1 million English articles were requested in the past hour?**

// COMMAND ----------

// MAGIC %md Start with the DataFrame that has already been filtered and contains just the English Wikipedia Articles:

// COMMAND ----------

display(pagecountsEnWikipediaArticlesOnlyDF.limit(5))

// COMMAND ----------

// MAGIC %md Call the `count()` action on the DataFrame to see how many unique English articles were requested in the last hour:

// COMMAND ----------

pagecountsEnWikipediaArticlesOnlyDF.count

// COMMAND ----------

// MAGIC %md The `count()` action returns back a `Long` data type.

// COMMAND ----------

// MAGIC %md There are currently about 5.1 million articles in English Wikipedia. So the percentage of English articles requested in the past hour is:

// COMMAND ----------

(pagecountsEnWikipediaArticlesOnlyDF.count)/(5100000.0)*100

// COMMAND ----------

// MAGIC %md You should see that approximately 30% - 40% of the English Wikipedia articles were requested recently.

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Q-7) How many total requests were there to English Wikipedia Desktop edition in the past hour?**

// COMMAND ----------

// MAGIC %md The DataFrame holding English Wikipedia article requests has a 3rd column named `requests`:

// COMMAND ----------

display(pagecountsEnWikipediaArticlesOnlyDF.limit(5))

// COMMAND ----------

// MAGIC %md If we `groupBy()` the project column and then call `sum()`, we can count how many total requests there were to English Wikipedia:

// COMMAND ----------

// Import the sql functions package, which includes statistical functions like sum, max, min, avg, etc.
import org.apache.spark.sql.functions._

// COMMAND ----------

display(pagecountsEnWikipediaDF.groupBy("project").sum())

// COMMAND ----------

pagecountsEnWikipediaDF.groupBy("project").sum().explain

// COMMAND ----------

// MAGIC %md ####![Wikipedia + Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/wiki_spark_small.png) **Q-8) How many total requests were there to English Wikipedia Mobile edition in the past hour?**

// COMMAND ----------

// MAGIC %md We'll need to start with the original, base DataFrame, which contains all the English Wikimedia project requests:

// COMMAND ----------

display(pagecountsEnAllDF.limit(5))

// COMMAND ----------

// MAGIC %md Run a `filter()` to keep just the rows referring to English Mobile:

// COMMAND ----------

val pagecountsEnMobileDF = pagecountsEnAllDF.filter($"project" === "en.mw")

// COMMAND ----------

pagecountsEnMobileDF.count

// COMMAND ----------

// MAGIC %md Hmm, only one record found. Let's see what it is:

// COMMAND ----------

display(pagecountsEnMobileDF)

// COMMAND ----------

// MAGIC %md The requests column above displays how many total requests English Wikipedia got from mobile clients. About 50% of the traffic to English Wikipedia seems to come from mobile clients.

// COMMAND ----------

// MAGIC %md We will analyze the Mobile vs. Desktop traffic patterns to Wikipedia more in the next notebook.
