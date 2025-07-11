# **Answers - Top 50 Interview Questions :**
---

## ✅ **Batch 1: PySpark Basics & Architecture (1–10)**

---

### **1. What is PySpark and how is it related to Apache Spark?**

**Answer:**
PySpark is the Python API for Apache Spark — a distributed computing framework. It allows Python developers to interact with Spark's core functionalities such as distributed data processing, RDDs, DataFrames, SQL, and machine learning (MLlib), while leveraging the massive scalability and speed of the JVM-based Spark engine.

**Example:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.show()
```

---

### **2. What are the components of the Apache Spark ecosystem?**

**Answer:**

| Component           | Description                                                   |
| ------------------- | ------------------------------------------------------------- |
| **Spark Core**      | Base engine for scheduling, memory management, fault recovery |
| **Spark SQL**       | Enables SQL queries on structured data                        |
| **Spark Streaming** | Real-time stream processing                                   |
| **MLlib**           | Machine learning library                                      |
| **GraphX**          | Graph processing                                              |

---

### **3. What are the key differences between PySpark and Pandas?**

| Feature     | PySpark                 | Pandas                 |
| ----------- | ----------------------- | ---------------------- |
| Scale       | Distributed (big data)  | Local (single machine) |
| Speed       | Faster with big data    | Faster with small data |
| API Type    | Spark API               | Pythonic               |
| Data Format | DataFrame (Distributed) | DataFrame (in-memory)  |

**Note:** PySpark is designed for large-scale data, while Pandas is best for in-memory small to mid-sized datasets.

---

### **4. Explain the architecture of a Spark application.**

**Answer:**
Spark follows a **master-slave** architecture:

* **Driver Program**: Coordinates the execution. Hosts the SparkSession/SparkContext.
* **Cluster Manager**: Allocates resources (YARN, Standalone, Mesos, Kubernetes).
* **Executors**: Run on worker nodes. Perform actual computation and store data.
* **Tasks**: Units of work sent to executors.

<p align="center">
  <img src="https://github.com/ShubhayuMallick1997/ShubhayuMallick1997/blob/main/1_nBsWLtjHm34B388tIytcCQ.png" width = "60%"/>
</p>

---

### **5. What is SparkContext and what is its role?**

**Answer:**
`SparkContext` is the entry point to Spark functionality. It connects your application to the Spark Cluster and coordinates job execution.

**Example:**

```python
from pyspark import SparkContext

sc = SparkContext("local", "MyApp")
rdd = sc.parallelize([1, 2, 3])
print(rdd.collect())
```

---

### **6. What is SparkSession? How is it different from SparkContext?**

**Answer:**

* `SparkSession` is the **entry point** for DataFrame and SQL functionality.
* It encapsulates both `SparkContext` and `SQLContext` under the hood.
* Introduced in Spark 2.0 to simplify the API.

**Example:**

```python
spark = SparkSession.builder.appName("Example").getOrCreate()
df = spark.read.json("data.json")
df.printSchema()
```

<p align="center">
  <img src="https://github.com/ShubhayuMallick1997/ShubhayuMallick1997/blob/main/1_4KJ9N9vLt-xQ3CHe0tmR2A.png" width = "60%"/>
</p>
---

### **7. What is a DAG (Directed Acyclic Graph) in Spark?**

**Answer:**
A **DAG** is a graph of all transformations applied to the data. Spark constructs a DAG to keep track of operations and **optimizes** the execution plan before running jobs.

* **Lazy Evaluation**: Allows Spark to optimize and chain transformations.
* Nodes = RDDs; Edges = Transformations

**Benefits:**

* Fault tolerance
* Optimized execution
* Job tracking

---

### **8. What is lazy evaluation in PySpark?**

**Answer:**
In PySpark, transformations are **lazy**, meaning they are **not executed until an action** (like `collect()` or `count()`) is called. This lets Spark **optimize execution plans** and avoid unnecessary computation.

**Example:**

```python
rdd = sc.parallelize([1, 2, 3, 4])
mapped = rdd.map(lambda x: x * 2)  # Lazy
print(mapped.collect())           # Triggers execution
```

---

### **9. What are transformations and actions in Spark?**

| Operation Type      | Description                | Examples                                      |
| ------------------- | -------------------------- | --------------------------------------------- |
| **Transformations** | Return a new RDD/DataFrame | `map()`, `filter()`, `flatMap()`, `groupBy()` |
| **Actions**         | Trigger execution          | `collect()`, `count()`, `take()`, `reduce()`  |

---

### **10. What is the difference between narrow and wide transformations?**

| Type       | Description                                                                                              |
| ---------- | -------------------------------------------------------------------------------------------------------- |
| **Narrow** | Data from a single partition is used. Faster and no shuffle. Examples: `map()`, `filter()`               |
| **Wide**   | Requires **shuffling** data across partitions. More expensive. Examples: `groupByKey()`, `reduceByKey()` |

---
Great! Let’s continue with **Batch 2** of detailed PySpark interview questions. This batch focuses on **RDDs, Transformations, and Actions** — foundational concepts for mastering distributed data processing.

---

## ✅ **Batch 2: RDDs & Transformations (11–20)**

---

### **11. What is an RDD in PySpark?**

**Answer:**
An **RDD (Resilient Distributed Dataset)** is the **fundamental data structure** in Spark, representing an immutable, distributed collection of objects that can be processed in parallel.

**Features:**

* Immutable
* Lazy evaluation
* Fault-tolerant (lineage-based recovery)
* Partitioned across the cluster

**Use Case:**
Best suited for **low-level data processing**, custom transformation logic, or when schema is not required.

---

### **12. How can you create an RDD in PySpark?**

**Answer:**
You can create an RDD in 3 main ways:

```python
# From a list (parallelize)
rdd1 = sc.parallelize([1, 2, 3, 4])

# From an external file
rdd2 = sc.textFile("path/to/file.txt")

# From an existing RDD
rdd3 = rdd1.map(lambda x: x * 2)
```

---

### **13. Explain the difference between `map()` and `flatMap()` in RDD.**

| Feature     | `map()`                         | `flatMap()`                   |
| ----------- | ------------------------------- | ----------------------------- |
| Output      | One-to-one transformation       | One-to-many transformation    |
| Return Type | RDD of lists or nested elements | Flattened RDD of elements     |
| Use Case    | Transform each element          | Split strings, extract tokens |

**Example:**

```python
rdd = sc.parallelize(["a b", "c d"])
print(rdd.map(lambda x: x.split()).collect())     # [['a', 'b'], ['c', 'd']]
print(rdd.flatMap(lambda x: x.split()).collect()) # ['a', 'b', 'c', 'd']
```

---

### **14. What is the difference between `reduce()` and `fold()`?**

**reduce()** – Combines elements using a binary function:

```python
rdd.reduce(lambda x, y: x + y)
```

**fold()** – Same as reduce but takes a **zero value** (initial value):

```python
rdd.fold(0, lambda x, y: x + y)
```

> `fold()` is safer for **empty RDDs**.

---

### **15. How does the `groupBy()` transformation work in RDD?**

**Answer:**
`groupBy()` groups elements based on a **key function**. It returns an RDD of (key, iterable) pairs.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
grouped = rdd.groupBy(lambda x: x % 2).mapValues(list)
print(grouped.collect())  # [(0, [2, 4]), (1, [1, 3, 5])]
```

> ⚠️ This can be expensive due to shuffling!

---

### **16. What are the most common RDD actions?**

**Answer:**

| Action             | Description                       |
| ------------------ | --------------------------------- |
| `collect()`        | Returns all elements to driver    |
| `count()`          | Counts elements                   |
| `take(n)`          | Returns first `n` elements        |
| `reduce()`         | Reduces to a single value         |
| `first()`          | Returns the first element         |
| `saveAsTextFile()` | Saves the RDD to external storage |

---

### **17. How does caching and persistence work in RDDs?**

**Answer:**

* `cache()` stores RDD in **memory** only.
* `persist()` allows control over **storage level** (e.g., memory and disk).

```python
rdd.persist(StorageLevel.MEMORY_AND_DISK)
rdd.count()  # Triggers evaluation and persists result
```

> Useful when reusing the same RDD multiple times.

---

### **18. What are partitioners in PySpark?**

**Answer:**
A **partitioner** controls how RDD records are distributed across partitions.

* Default: HashPartitioner
* Useful in key-value RDDs to optimize shuffle-heavy operations.

```python
pairRDD = sc.parallelize([("a", 1), ("b", 2)]).partitionBy(2)
```

---

### **19. What is data lineage in RDD?**

**Answer:**
Lineage is the sequence of operations (like `map`, `filter`) that created an RDD. It helps Spark **recompute lost partitions** in case of node failure.

```python
rdd = sc.textFile("log.txt")
filtered = rdd.filter(lambda x: "ERROR" in x)
# Lineage: log.txt -> filter
```

---

### **20. What are some limitations of RDDs compared to DataFrames?**

| RDDs                        | DataFrames                        |
| --------------------------- | --------------------------------- |
| No schema                   | Schema-aware (column names/types) |
| Manual optimization         | Catalyst optimizer                |
| More lines of code          | Concise and readable              |
| Slower with structured data | Faster for analytics              |

---

Perfect! Let’s move to **Batch 3**, which covers one of the most frequently used features in PySpark: **DataFrames and Datasets**.

---

## ✅ **Batch 3: DataFrames & Datasets (21–30)**

---

### **21. What is a DataFrame in PySpark?**

**Answer:**
A **DataFrame** is a distributed collection of **rows organized into columns**, similar to a relational table in SQL or a DataFrame in pandas. It is part of **Spark SQL**.

**Features:**

* Schema-aware
* Optimized via Catalyst engine
* Can read/write from/to multiple data sources

---

### **22. How can you create a DataFrame in PySpark?**

**Answer:**
You can create DataFrames in several ways:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

# From a Python list
data = [("Alice", 34), ("Bob", 45)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, schema=columns)

# From CSV
df = spark.read.csv("file.csv", header=True, inferSchema=True)

# From JSON
df = spark.read.json("file.json")
```

---

### **23. What is schema inference and manual schema definition?**

**Schema Inference:**
Spark automatically determines the schema of data:

```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

**Manual Schema:**
You define the schema explicitly:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

df = spark.read.csv("data.csv", schema=schema)
```

---

### **24. How do you select, filter, and sort columns in DataFrames?**

**Select Columns:**

```python
df.select("Name", "Age")
```

**Filter Rows:**

```python
df.filter(df["Age"] > 30)
df.where("Age > 30")
```

**Sort Data:**

```python
df.orderBy("Age")       # Ascending
df.orderBy(df.Age.desc())  # Descending
```

---

### **25. What is `withColumn()` in PySpark?**

**Answer:**
`withColumn()` is used to **add or modify a column** in a DataFrame.

```python
from pyspark.sql.functions import col

df.withColumn("AgePlus10", col("Age") + 10)
```

---

### **26. How can you drop and rename columns in DataFrames?**

**Drop Column:**

```python
df.drop("Age")
```

**Rename Column:**

```python
df.withColumnRenamed("Name", "FullName")
```

---

### **27. What are aliases in PySpark?**

**Answer:**
Aliases rename columns or subqueries temporarily for better readability or SQL-like operations.

```python
from pyspark.sql.functions import col

df.select(col("Name").alias("Full_Name"))
```

In SQL:

```python
df.createOrReplaceTempView("people")
spark.sql("SELECT Name AS FullName FROM people")
```

---

### **28. How do you convert an RDD to a DataFrame?**

**Answer:**

```python
rdd = sc.parallelize([("Alice", 30), ("Bob", 25)])
df = rdd.toDF(["Name", "Age"])
```

Or:

```python
df = spark.createDataFrame(rdd, ["Name", "Age"])
```

---

### **29. How do you convert a DataFrame to an RDD?**

**Answer:**

```python
rdd = df.rdd
```

This gives you an RDD of `Row` objects.

---

### **30. What are the key advantages of using DataFrames over RDDs?**

| Feature      | RDDs                       | DataFrames                         |
| ------------ | -------------------------- | ---------------------------------- |
| Schema       | Not schema-aware           | Schema-aware                       |
| Performance  | Lower                      | Optimized via Catalyst             |
| Usability    | Verbose, manual processing | High-level, SQL-like               |
| Integration  | Poor with BI tools         | Easy integration (JDBC, Tableau)   |
| Optimization | Manual                     | Automatic with Catalyst & Tungsten |

---

Awesome! Let’s proceed to **Batch 4**, which focuses on **DataFrame Transformations and Aggregations** – essential operations for any Data Engineer.

---

## ✅ **Batch 4: DataFrame Transformations and Aggregations (31–40)**

---

### **31. How do you filter rows in a PySpark DataFrame?**

**Answer:**
You can filter rows using `filter()` or `where()`:

```python
df.filter(df["Age"] > 30)
df.where("Age > 30")
```

You can also use complex conditions:

```python
df.filter((df.Age > 25) & (df.Salary > 50000))
```

---

### **32. How do you add or modify a column in PySpark?**

**Answer:**
Use `withColumn()` to add or update columns:

```python
from pyspark.sql.functions import col

df.withColumn("AgePlus5", col("Age") + 5)
```

To update an existing column:

```python
df = df.withColumn("Age", col("Age") + 1)
```

---

### **33. How do you type cast a column in PySpark?**

**Answer:**
Use the `cast()` method:

```python
df.withColumn("Age", df["Age"].cast("string"))
```

---

### **34. How do you drop a column in a DataFrame?**

**Answer:**
Use the `drop()` method:

```python
df.drop("UnnecessaryColumn")
```

---

### **35. How do you rename a column in PySpark?**

**Answer:**
Use the `withColumnRenamed()` method:

```python
df.withColumnRenamed("oldName", "newName")
```

---

### **36. What is the difference between `distinct()` and `dropDuplicates()`?**

**Answer:**

| Method                             | Description                                        |
| ---------------------------------- | -------------------------------------------------- |
| `distinct()`                       | Removes duplicate rows across the entire DataFrame |
| `dropDuplicates(["col1", "col2"])` | Removes duplicates based on specific columns       |

Example:

```python
df.distinct()
df.dropDuplicates(["Name", "Age"])
```

---

### **37. What are some useful string functions in PySpark?**

**Answer:**
PySpark has many built-in string functions (from `pyspark.sql.functions`):

```python
from pyspark.sql.functions import upper, lower, trim, concat_ws

df.select(upper(df.Name), trim(df.Email))
```

Others: `substring()`, `length()`, `replace()`, `regexp_extract()`

---

### **38. What are some useful date functions in PySpark?**

**Answer:**

```python
from pyspark.sql.functions import current_date, datediff, to_date, date_format

df = df.withColumn("today", current_date())
df = df.withColumn("days_since", datediff(current_date(), col("birth_date")))
```

---

### **39. How do you implement conditional logic in PySpark (like if-else)?**

**Answer:**
Use `when()` and `otherwise()`:

```python
from pyspark.sql.functions import when

df = df.withColumn(
    "Age_Group",
    when(df.Age < 18, "Child")
    .when(df.Age < 60, "Adult")
    .otherwise("Senior")
)
```

---

### **40. How do you perform groupBy and aggregation operations in PySpark?**

**Answer:**

```python
df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    count("*").alias("employee_count")
)
```

You can also use other functions like `sum()`, `min()`, `mean()`, `stddev()`.

---

Great! Let’s dive into **Batch 5**, covering **Joins, Window Functions, and File Handling in PySpark** – highly important for both data wrangling and distributed file systems.

---

## ✅ **Batch 5: Joins, Window Functions & File Handling (41–50)**

---

### **41. What are the different types of joins supported in PySpark?**

**Answer:**

PySpark supports:

* **Inner Join**: Returns matching rows from both DataFrames
* **Left Outer Join**: All rows from left + matching from right
* **Right Outer Join**: All rows from right + matching from left
* **Full Outer Join**: All rows from both sides with NULLs where no match
* **Left Semi Join**: Returns rows from left where match exists in right
* **Left Anti Join**: Returns rows from left where no match in right

Example:

```python
df1.join(df2, df1.id == df2.id, "inner")
```

---

### **42. How do you optimize joins in PySpark?**

**Answer:**

* **Broadcast Join**: Use when one DataFrame is small enough to fit in memory.

```python
from pyspark.sql.functions import broadcast

df1.join(broadcast(df2), "id")
```

* **Salting**: For skewed joins, add a random number as salt to keys.
* **Repartitioning**: Use `.repartition()` on join keys before join to balance partitions.

---

### **43. What is salting in PySpark and when is it used?**

**Answer:**

Salting is a technique to handle **data skew** in join keys. You artificially add randomness to the key so data is spread evenly across partitions.

Example:

```python
from pyspark.sql.functions import concat, lit, rand

# Add salt
df1 = df1.withColumn("salted_key", concat(df1["key"], (rand()*10).cast("int")))
```

---

### **44. What are window functions? Give examples.**

**Answer:**

Window functions operate over a group of rows (window frame). They include:

* `row_number()`, `rank()`, `dense_rank()`
* `lag()`, `lead()`, `ntile()`

Example:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_num", row_number().over(windowSpec))
```

---

### **45. How do you read a CSV file in PySpark?**

**Answer:**

```python
df = spark.read.option("header", True).csv("path/to/file.csv")
```

You can specify schema manually using `schema=` or use `.inferSchema=True`.

---

### **46. How do you read a JSON file in PySpark?**

**Answer:**

```python
df = spark.read.option("multiline", True).json("path/to/file.json")
```

---

### **47. How do you read/write Parquet files in PySpark?**

**Answer:**

```python
# Reading
df = spark.read.parquet("path/to/file.parquet")

# Writing
df.write.mode("overwrite").parquet("output/path")
```

Parquet is the most optimized format for Spark due to its columnar storage.

---

### **48. How do you read files from AWS S3 in PySpark?**

**Answer:**

First set S3 credentials:

```python
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "<ACCESS_KEY>")
hadoop_conf.set("fs.s3a.secret.key", "<SECRET_KEY>")
```

Then:

```python
df = spark.read.csv("s3a://bucket-name/path/file.csv")
```

---

### **49. What are partitioning and bucketing in PySpark file writes?**

**Answer:**

* **Partitioning**: Saves data into folders based on column values

```python
df.write.partitionBy("year", "month").parquet("path/")
```

* **Bucketing**: Groups data into fixed number of buckets for optimization (used mainly with Hive support)

---

### **50. What compression options are supported in PySpark?**

**Answer:**

PySpark supports:

* `gzip`
* `snappy` (default for Parquet)
* `bzip2`
* `lz4`
* `deflate`

Usage example:

```python
df.write.option("compression", "gzip").json("path/")
```

---

