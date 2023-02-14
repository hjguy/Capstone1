# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.randomnamehj.dfs.core.windows.net",
    dbutils.secrets.get(scope="randomhj", key="capstone"))

# COMMAND ----------

df = spark.read.format('csv').options(header = True, multiline = True, escape = '"').load('/mnt/raw/juniorpublic')
df.createOrReplaceTempView('juniorpublic1')

# COMMAND ----------

df = spark.sql("""
SELECT
    *
    , CAST(NULL AS TIMESTAMP) AS Timestamp
    , CAST(NULL AS STRING) AS SourcefileName
    , CAST (NULL AS STRING) AS MangersIntials
    , CAST (NULL AS DATE) AS Date
    FROM juniorpublic1
""")
df.createOrReplaceTempView('juniorpublic2')

# COMMAND ----------

display(dbutils.fs.mounts())
