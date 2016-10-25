{"version":"NotebookV1","origId":503877321547541,"name":"broadcast-demo","language":"scala","commands":[{"version":"CommandV1","origId":503877321547543,"guid":"17ccc2b4-eb69-4009-9900-c7b2019fafb9","subtype":"command","commandType":"auto","position":1.0,"command":"%md\n# Broadcast Variables","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"c188c263-e431-412d-8af9-638be7b82517"},{"version":"CommandV1","origId":503877321547544,"guid":"3109b6fe-9952-4a53-ba91-0fe80d97d74b","subtype":"command","commandType":"auto","position":2.0,"command":"%md\n## What are Broadcast Variables?\nBroadcast Variables allow us to broadcast a read-only copy of non-rdd data to all the executors.  The executors can then access the value of this data locally.  This is much more efficent than relying on the driver to trasmit this data teach time a task is run.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"afde3410-54c9-4a5f-9079-3a635879bbf6"},{"version":"CommandV1","origId":503877321547545,"guid":"31e2bab6-078f-4dc6-ae66-8e2cb12e37a3","subtype":"command","commandType":"auto","position":3.0,"command":"%md\n## Using a Broadcast Variable","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"91b965e2-c714-4f93-bceb-d767f19a2e31"},{"version":"CommandV1","origId":503877321547546,"guid":"2dd7d2d5-4648-4625-ad14-614ba4ff7f61","subtype":"command","commandType":"auto","position":4.0,"command":"{\n  // Create a broadcast variable, transmitting it's value to all the executors.\n  val broadcastVar = sc.broadcast(1 to 3)\n\n  // The value is available on the driver\n  println(s\"Driver: \" + broadcastVar.value)\n\n  // And on the executors\n  val results = sc.parallelize(1 to 5, numSlices=5).map { n => s\"Task $n: \" + broadcastVar.value }.collect()\n  println(results.mkString(\"\\n\"))\n\n  // We can free up the memory in the executors, but it will stay available in the driver if still needed\n  broadcastVar.unpersist\n\n  // And when completely done, we can destroy the broadcastVar everywhere\n  broadcastVar.destroy\n\n  //broadcastVar.value  // Uncomment this to demo what breaks if you use after destroying.\n}","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"cad76a9f-1ed8-4ceb-b490-68953009d8f5"},{"version":"CommandV1","origId":503877321547547,"guid":"0d3023b6-b9f6-4400-84f9-eb80f9e55f59","subtype":"command","commandType":"auto","position":5.0,"command":"%md\n## How Broadcast Variables can improve performance (demo)\nHere we have a medium sized data set, small enough to fit in RAM, but still involves quite a bit of network communication when sending the data set to the executors.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"0bf13761-5848-44c2-ad80-4cd707e52428"},{"version":"CommandV1","origId":503877321547548,"guid":"05690cfd-4d27-45bb-a852-0b56d94ecfa8","subtype":"command","commandType":"auto","position":6.0,"command":"// Create a medium sized dataSet of several million values.\nval size = 60*1000*1000\nvar dataSet = (1 to size).toArray","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"0a049814-2102-4cde-b70d-b42b39e4ee62"},{"version":"CommandV1","origId":503877321547549,"guid":"e9d225d5-48b3-4925-bcd3-132c4c266e71","subtype":"command","commandType":"auto","position":7.0,"command":"%md Now let's demonstrate the overhead of network communication when not using broadcast variables.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"de95ae6c-aefb-4916-88e4-687c723f4c32"},{"version":"CommandV1","origId":503877321547550,"guid":"f5da83d5-2a2a-40c3-8fed-72978ddcc611","subtype":"command","commandType":"auto","position":8.0,"command":"// Ceate an RDD with 5 partitions so that we can do an operation in 25 separate tasks running in parallel on up to 5 different executors.\n\nval rdd = sc.parallelize(1 to 5, numSlices=5)\nprintln(s\"${rdd.partitions.length} partitions\")","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"6d98f526-2539-4749-b62c-aa92177281ec"},{"version":"CommandV1","origId":503877321547551,"guid":"e1f1ac26-2858-4dc0-8a2c-a2c834a7d52f","subtype":"command","commandType":"auto","position":9.0,"command":"// In a loop, do a job 5 times without using broadcast variables...\n\nfor (i <- 1 to 5) rdd.map { x => dataSet.length * x }.collect()\n\n// Look how slow it is...\n// This is because our local `dataSet` variable is being used by the lambda and thus must be sent to each executor every time a task is run.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"dd4a3594-cba5-44d1-8415-33d44ac76dcc"},{"version":"CommandV1","origId":503877321547552,"guid":"33e9f76d-d387-4758-8afe-f9a370c5a22b","subtype":"command","commandType":"auto","position":10.0,"command":"%md Let's do that again, but this time we'll first send a copy of the dataset to the executors once, so that the data is available locally every time a task is run.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"3cffe365-8793-4207-9f1c-c4c9a6cc4c03"},{"version":"CommandV1","origId":503877321547553,"guid":"c1c42ecd-c550-43b1-b569-fcb9b8a81091","subtype":"command","commandType":"auto","position":11.0,"command":"// Create a broadcast variable.  This will transmit the dataset to the executors.\n\nval broadcastVar = sc.broadcast(dataSet)","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"9847fbd6-f252-48d7-a2f8-9e1c222965c1"},{"version":"CommandV1","origId":503877321547554,"guid":"80be73ae-62dd-4300-bc67-8fb3b15fdd1e","subtype":"command","commandType":"auto","position":12.0,"command":"%md Now we'll run the job 5 times again, and you would expect to be faster.  (What happened?)","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"2ca174c2-0a5f-4af6-a73c-b205a63c1659"},{"version":"CommandV1","origId":503877321547555,"guid":"e28e0e0f-5517-421b-b9cc-5fef6c8e3c95","subtype":"command","commandType":"auto","position":13.0,"command":"for (i <- 1 to 5) rdd.map { x => broadcastVar.value.length * x }.collect()","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"3da6da8b-e688-4fe3-8577-ad20a38d7032"},{"version":"CommandV1","origId":503877321547556,"guid":"848a7f40-6083-4a38-8935-1b0cd32776b0","subtype":"command","commandType":"auto","position":14.0,"command":"%md\nWhoa, what wasn't any faster.  What happened?  Well, you called `this.broadcastVar`, which unfortunately includes `this` and `this.dataSet` in the closure.  Let's fix that by introducing a local variable inside { }.\n\nNow we'll run the job 5 times, and notice how much faster it is since we don't have to retransmit the data set each time.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"21d8e024-1132-4638-9d22-6ae462c88598"},{"version":"CommandV1","origId":503877321547557,"guid":"d8950763-988d-4aa5-be3b-4d892a22b958","subtype":"command","commandType":"auto","position":15.0,"command":"{\n  var broadcastVar2 = broadcastVar\n  for (i <- 1 to 5) rdd.map { x => broadcastVar2.value.length * x }.collect()\n}","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"f610b248-a3a1-4128-8395-f6c3aa7bd466"},{"version":"CommandV1","origId":503877321547558,"guid":"5a572b22-fa73-46d4-aa9a-9c2c74bb3e5a","subtype":"command","commandType":"auto","position":16.0,"command":"%md Finally, let's remove the broadcast variable from the Executor JVMs.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"38d1cbc8-d4fe-45d7-9f70-ac664f7fd214"},{"version":"CommandV1","origId":503877321547559,"guid":"baadbed4-5936-4410-a000-e7c96f3f6a94","subtype":"command","commandType":"auto","position":17.0,"command":"// Free up the memory on the executors.\nbroadcastVar.unpersist()","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"cd75bde2-ce0b-4421-bec7-13e0b66d2343"},{"version":"CommandV1","origId":503877321547560,"guid":"18df6a65-849a-42ad-91a8-59c2f9942baa","subtype":"command","commandType":"auto","position":18.0,"command":"%md\n## Frequently Asked Questions about Broadcast Variables\n**Q:** How is this different than using an RDD to keep data on an executor?  \n**A:** With an RDD, the data is divided up into partitions and executors hold only a few partitions.  A broadcast variable is sent out to all the executors.\n\n**Q:** When should I use an RDD and when should I use a broadcast variable?  \n**A:** BroadCast variables must fit into RAM (and they're generally under 20 MB).  And they are on all executors.  They're good for small datasets that you can afford to leave in memory on the executors.  RDDs are better for very large datasets that you want to partition and divide up between executors.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"95bbf431-7b3e-43d4-9c16-90799f25b50d"},{"version":"CommandV1","origId":503877321547561,"guid":"c081b408-71c2-4dc1-975b-9070ecc4dca9","subtype":"command","commandType":"auto","position":19.0,"command":"%md ## How do Broadcasts Work with Dataframes?\n\nBroadcasts can be used to improve performance of some kinds of joins when using Dataframes/Dataset/SparkSQL.\n\nIn many we may want to join one or more (relatively) small tables against a single large dataset -- e.g., \"enriching\" a transaction or event table (containing, say, customer IDs and store IDs) with additional \"business fact tables\" (like customer demographic info by ID, and store location and profile by ID). Instead of joining all of these as distributed datasets, typically requiring a shuffle each time, we could broadcast a copy of the small tables to each executor, where they can can be joined directly (through a hash lookup) against the local partitions of the bigger table.\n\nThis approach is sometimes called a \"map-side join\" or \"hash join\" and is related to, but not the same as, \"skewed join\" in other frameworks.","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"384e7167-10fe-4ac4-93f9-87df08d10474"},{"version":"CommandV1","origId":503877321547562,"guid":"ae78b6b9-9acc-4a76-934e-9fdbc8ca8b0c","subtype":"command","commandType":"auto","position":20.0,"command":"%md ### Using Broadcast Joins with Spark\n\nBy default, Spark will use a shuffle to join two datasets (unless Spark can verify that they are already co-partitioned):","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"3bcfa2bf-c306-4fd6-bf5e-0683f3af795a"},{"version":"CommandV1","origId":503877321547563,"guid":"ab14550f-a05a-4a64-90b3-0f00e8017ec3","subtype":"command","commandType":"auto","position":21.0,"command":"val df1 = sqlContext.range(100)\nval df2 = sqlContext.range(100)\n\ndf1.join(df2, df1(\"id\") === df2(\"id\")).collect","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"c3dba364-09cc-4606-ae8c-0637f6245b4c"},{"version":"CommandV1","origId":503877321547564,"guid":"b8c3b0de-6e32-46da-9364-e0f075029a0c","subtype":"command","commandType":"auto","position":22.0,"command":"%md Look at the Spark UI for that job, and note the stage count and the shuffle.\n\nTo use a broadcast join, we need at least one of the following:\n* statistics from running Hive ANALYZE on the table, and the size less than `spark.sql.autoBroadcastJoinThreshold`\n* statistics from caching the table in Spark, and the size less than `spark.sql.autoBroadcastJoinThreshold`\n* a broadcast hint applied to the table","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"3a6eabab-854b-42bf-86cc-780b2b0584f4"},{"version":"CommandV1","origId":503877321547565,"guid":"c0cce12b-36a6-4c05-a31d-bb5a2a01b322","subtype":"command","commandType":"auto","position":23.0,"command":"import org.apache.spark.sql.functions._\n\ndf1.join(broadcast(df2), df1(\"id\") === df2(\"id\")).collect","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"16db3767-6a9b-4bcf-a156-401dd3adcc88"},{"version":"CommandV1","origId":503877321547566,"guid":"2cc538c3-3244-40f6-ab8d-bfeae9252c5e","subtype":"command","commandType":"auto","position":24.0,"command":"df2.cache.count\ndf1.join(df2, df1(\"id\") === df2(\"id\")).collect","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"cf27a42b-ac97-49d3-8247-b7f3a0f04661"},{"version":"CommandV1","origId":503877321547567,"guid":"ac4a578a-835d-442b-865e-5ab2f7bc81e9","subtype":"command","commandType":"auto","position":25.0,"command":"df2.unpersist\ndf1.join(df2, df1(\"id\") === df2(\"id\")).collect","commandVersion":0,"state":"finished","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0.0,"submitTime":0.0,"finishTime":0.0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"iPythonMetadata":null,"streamStates":{},"nuid":"a579c31c-5a54-4e4d-b830-199c5cbcd059"}],"dashboards":[],"guid":"49eab079-8b14-432a-a325-d496155d18cc","globalVars":{},"iPythonMetadata":null,"inputWidgets":{}}