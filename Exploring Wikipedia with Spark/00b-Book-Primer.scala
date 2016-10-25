// Databricks notebook source exported at Tue, 25 Oct 2016 07:36:11 UTC
// MAGIC %md ![Wikipedia Mission](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/wiki_mission.png)

// COMMAND ----------

// MAGIC %md That's what Wikipedia is trying to do.
// MAGIC 
// MAGIC Throughout this course, we will analyze various Wikipedia data sets using Apache Spark.

// COMMAND ----------

// MAGIC %md ![About Wikipedia](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/wiki_about.png)

// COMMAND ----------

// MAGIC %md We will mostly be focused on the English Wikipedia:

// COMMAND ----------

// MAGIC %md ![About En Wikipedia](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/wiki_enabout.png)

// COMMAND ----------

// MAGIC %md If someone printed out English Wikipedia, it would take up 12 large bookshelves:

// COMMAND ----------

// MAGIC %md ![Comparison to human](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/wiki_man.png)

// COMMAND ----------

// MAGIC %md As you may know, Wikipedia articles are created by volunteer editors *(Wikipedians)* doing edit after edit...

// COMMAND ----------

// MAGIC %md ![En Edits](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/wiki_enedits.png)

// COMMAND ----------

// MAGIC %md Our software tool to do the data analysis will be Apache Spark:

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/Apache-Spark-Logo_TM_200px.png" style="margin-bottom: 20px"/>
// MAGIC 
// MAGIC * Started as a research project at the University of California AMPLab, in 2009
// MAGIC 
// MAGIC * Open Source License (Apache 2.0)
// MAGIC 
// MAGIC * Latest stable release: 2.0.1 (October, 2016)
// MAGIC 
// MAGIC * 600,000+ lines of code (75% Scala)
// MAGIC 
// MAGIC * Built by 1,000+ developers from more than 200 companies

// COMMAND ----------

// MAGIC %md Spark is a unified processing engine that can analyze big data using SQL, machine learning, graph processing or real time stream analysis:

// COMMAND ----------

// MAGIC %md ![Spark Engines](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_4engines.png)

// COMMAND ----------

// MAGIC %md Spark can read from many different databases and file systems and run in various environments. 

// COMMAND ----------

// MAGIC %md ![Spark Goal](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_goal.png)

// COMMAND ----------

// MAGIC %md Although Spark supports four languages (Scala, Java, Python, R), the majority of this course will use Scala.
// MAGIC 
// MAGIC Broadly speaking, there are **2 APIs** for interacting with Spark:
// MAGIC - **DataFrames/SQL/Datasets:** general, higher level API for users of Spark
// MAGIC - **RDD:** a lower level API for spark internals and advanced programming

// COMMAND ----------

// MAGIC %md A Spark cluster is made of one Driver and many Executor JVMs (java virtual machines):

// COMMAND ----------

// MAGIC %md ![Spark Physical Cluster, slots](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_cluster_slots.png)

// COMMAND ----------

// MAGIC %md The Driver sends Tasks to the empty slots on the Executors when work has to be done:

// COMMAND ----------

// MAGIC %md ![Spark Physical Cluster, tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_cluster_tasks.png)

// COMMAND ----------

// MAGIC %md In Databricks Community Edition, everyone gets a local mode cluster, where the Driver and Executor code run in the same JVM. Local mode clusters are typically used for prototyping and learning Spark:

// COMMAND ----------

// MAGIC %md ![Notebook + Micro Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/notebook_microcluster.png)

// COMMAND ----------

// MAGIC %md There are three kinds of chapters/notebooks in this course:

// COMMAND ----------

// MAGIC %md ![About Book](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/book_about.png)

// COMMAND ----------

// MAGIC %md In the Data Analyst sections, you will learn how to explore the data, gain insights and visualize the results. As a Data Engineer, you will learn about Spark internals so you can rewrite the data analyst's code to make it run faster. In the Data Science notebooks,  you'll apply machine learning algorithms to organize the data and make predictions.

// COMMAND ----------

// MAGIC %md Along the way, you will run cells with pre-written code and occasionally work on challenge excercises.
// MAGIC 
// MAGIC Run the following cell to see the **top 25 most popular articles** on Wikipedia in the **past hour**:

// COMMAND ----------

// To Run: Place cursor in this cell and hit SHIFT + ENTER

display(sqlContext
  .read
  .parquet("/mnt/wikipedia-readonly/pagecounts/staging_parquet_enarticles_only/")
  .orderBy($"requests".desc)
  .limit(25))

// COMMAND ----------

// MAGIC %md We hope you enjoy exploring Wikipedia and learning about Spark. There are many discoveries that lie ahead...
