# Databricks notebook source exported at Tue, 25 Oct 2016 08:17:16 UTC
# MAGIC %md
# MAGIC # Setup Instructions
# MAGIC General Overview:
# MAGIC 1. Login to Databricks Community Edition.
# MAGIC 2. Upload the lab files.
# MAGIC 3. Mount the datasets.
# MAGIC 4. Install 3rd party libraries required for the exercises.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Login to Databricks Community edition.
# MAGIC * If you have not already registered for a free account at Databricks Community Edition, you will need to [signup here](https://accounts.cloud.databricks.com/registration.html#signup/community).  Email verification is required.
# MAGIC * If you do already have an account, [login here](https://community.cloud.databricks.com/login.html).
# MAGIC * If you are running behind or experience difficulty, your instructor may be able to provide you a pre-configured temporary account valid for 7-days.  (Not all classes have temporary accounts available.)
# MAGIC 
# MAGIC <br/><img src="https://openclipart.org/download/58669/tooltip.svg" alt="Tip" style="height: 90px"/>
# MAGIC > Your personal Databricks Community Edition account is free and yours to keep forever.  You can use it anytime to continue learning and using Spark.  The compute resources in the free Databricks Community Edition are limited to small-data scale tasks, but you can pay to upgrade and run very large big-data jobs at the petabyte scale.  Databricks can also be deployed into your company's AWS account to ensure maximum security for your data.  The cloud and big-data are a perfect fit enabling Databricks to scale up and down compute resources depending on your workload.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Upload the lab files.
# MAGIC 
# MAGIC Once you have a Databricks Community Edition account, you can start to prepare your training workspace:
# MAGIC 1. On the left edge of the window, click the "Home" button.  You'll see a file browser appear.  
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Home+Button.png">
# MAGIC 
# MAGIC 2. Locate your username, and then click on the triangle icon to the right of your username.  
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/User+Name.png">
# MAGIC 
# MAGIC 3. In the popup menu, select "Import."  
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Import+Button.png"> 
# MAGIC 
# MAGIC 4. Select "Import From URL" and copy paste this URL into the dialog box:  
# MAGIC `https://s3-us-west-2.amazonaws.com/curriculum-release/labs/sseu2016.dbc`  
# MAGIC 
# MAGIC 5. Click "Import."  It may take up to 60 seconds for all files to successfully import.  (They will progressively appear.)  
# MAGIC <img style="border: 1px solid black" src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Import+URL+Button.png">
# MAGIC 
# MAGIC <br/><img src="https://openclipart.org/download/58669/tooltip.svg" alt="Tip" style="height: 90px"/>
# MAGIC > You can later download your lab solutions and share them with others by choosing the "Export" option instead of the import option.  This will download a .dbc archive (databricks cloud archive, a variation of a zip file) that you can then import into any other Databricks account.  Alternatively you can download a folder as a zip of HTML files.  You can even download individual notebooks as source code to run them in the open source Apache Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Mount the datasets.
# MAGIC The datasets are located in Amazon's simple storage service (Amazon S3).  Let's make them available inside your Databricks workspace:
# MAGIC 
# MAGIC 1. Locate the folder you just imported in Step 2 above.
# MAGIC   
# MAGIC 2. Open the notebook named "datasets\_mount".  
# MAGIC    <img style="border: 1px solid black" src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Click+on+datasets-mount.png">
# MAGIC 
# MAGIC 3. Click the "Run All" button at the top of the notebook.  
# MAGIC    <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Run+All.png">
# MAGIC 
# MAGIC <br/><img src="https://openclipart.org/download/58669/tooltip.svg" alt="Tip" style="height: 90px"/>
# MAGIC > The notebook you just ran makes Amazon S3 buckets (disk storage) available via a friendly filename.  This is done using the Databricks File System known as DBFS.  It serves the same function as Hadoop's File System (HDFS) by making data available to our distributed compute clusters in the cloud.  Unlke HDFS, DBFS uses Amazon S3 buckets optimized for the cloud environment.  This results in a cloud friendly experience with increased scalability and reduced cost.  If you prefer to use HDFS or access S3 buckets directly this is also possible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Install 3rd party libraries required for the exercises.
# MAGIC 1. On the left edge of the window, click the "Workspace" button.  You'll see a file browser appear.  (Note: "Home" goes to your home folder, whereas "Workspace" goes to the most recently used location in the file browser.)  
# MAGIC    <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Click+Workspace+Button.png"></img>
# MAGIC 
# MAGIC 2. At the top of the file browser, you'll see "Workspace" with a downward arrow next to it.  Click on the arrow.  
# MAGIC    <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Workspace+down+arrow.png" alt="Screenshot placeholder"></img>
# MAGIC 
# MAGIC 3. In the popup menu, select Create -> Library.  You'll then see the "New Library" screen.  
# MAGIC    <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Create-+Library.png" alt="Screenshot placeholder" style="border:1px solid black"></img>
# MAGIC 
# MAGIC 4. Install the [Spark GraphFrames library](https://spark-packages.org/package/graphframes/graphframes):
# MAGIC    * **Source**: Maven Coordinate
# MAGIC    * **Coordinate**: `graphframes:graphframes:0.1.0-spark1.6`
# MAGIC    * **Click**: Create Library  
# MAGIC    <img src="	
# MAGIC https://s3-us-west-2.amazonaws.com/curriculum-release/setup_notebook/Maven+graphframes+1.6.png" alt="Screenshot placeholder" style="border:1px solid black"></img>
# MAGIC 
# MAGIC 5. Repeat steps 1 through 4 to install the Spark Streaming Kafka library.
# MAGIC     * **Source**: Maven Coordinate
# MAGIC     * **Coordinate**: `org.apache.spark:spark-streaming-kafka_2.10:1.6.2`
# MAGIC     * **Click**: Create Library
# MAGIC     
# MAGIC <br/><img src="https://openclipart.org/download/58669/tooltip.svg" alt="Tip" style="height: 90px"/>
# MAGIC > By uploading these libraries, you ensure all the computers in the Spark cluster have access.  The libraries have various uses we'll explore during the labs.  You can learn more about uploading libraries and many other topics in the <a href="https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#01%20Databricks%20Overview/04%20Libraries.html" target="_blank">Databricks Guide</a>.  If using open-source Apache Spark (instead of Databrick) you would instead manually install the libraries onto each computer's Java CLASSPATH or Python PYTHONPATH.
