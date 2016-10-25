// Databricks notebook source exported at Tue, 25 Oct 2016 08:19:58 UTC
// MAGIC %md ### Solutions to 01_DA-Pagecounts RunMe lab

// COMMAND ----------

// MAGIC %md **Solution to Question #4 / Challenge 1:** How many requests did the "Apache Spark" article recieve during this hour?

// COMMAND ----------

// MAGIC %md **Challenge 1: ** Can you figure out how to filter the `pagecountsEnWikipediaArticlesOnlyDF` DataFrame for just `Apache_Spark`?

// COMMAND ----------

pagecountsEnWikipediaArticlesOnlyDF.filter($"article" === "Apache_Spark").show()
