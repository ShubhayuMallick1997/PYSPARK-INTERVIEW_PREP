
# ✅ **Batch 6: PySpark SQL, UDFs, and Performance Optimization (51–60)**

---

### **51. How do you create a temporary SQL view in PySpark?**

**Answer:**

You can create a temporary view on a DataFrame using:

```python
df.createOrReplaceTempView("employees")
```

Then run SQL queries:

```python
result = spark.sql("SELECT * FROM employees WHERE salary > 50000")
```

---

### **52. What is the difference between `createTempView()` and `createGlobalTempView()`?**

**Answer:**

| Feature  | `createTempView()`           | `createGlobalTempView()`               |
| -------- | ---------------------------- | -------------------------------------- |
| Scope    | Session-local                | Global across sessions                 |
| Access   | Only in current SparkSession | Accessible via `global_temp.view_name` |
| Use case | Temporary analysis           | Shared views across jobs               |

---

### **53. How do you register a UDF in PySpark?**

**Answer:**

You register a **User Defined Function (UDF)** using:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def make_upper(s):
    return s.upper()

upper_udf = udf(make_upper, StringType())

df.withColumn("upper_name", upper_udf(df.name))
```

---

### **54. What is the performance drawback of using UDFs?**

**Answer:**

* **UDFs are black boxes** to the Catalyst Optimizer.
* Cannot be optimized or inlined into query plans.
* Slower execution compared to built-in Spark functions.
* Serialization overhead between JVM and Python.

🔄 Prefer built-in functions (`F.upper()`, `F.concat()`, etc.) whenever possible.

---

### **55. What is a pandas UDF?**

**Answer:**

Also known as **vectorized UDFs**, they leverage **Apache Arrow** for efficient batch data transfer between Python and JVM.

Example:

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def add_ten(col: pd.Series) -> pd.Series:
    return col + 10

df.withColumn("adjusted", add_ten(df.salary))
```

✅ Faster than regular UDFs, and more optimized.

---

### **56. What is Catalyst Optimizer in Spark?**

**Answer:**

Catalyst is Spark's query optimizer that transforms logical plans into optimized physical plans using:

* Constant folding
* Predicate pushdown
* Projection pruning
* Join reordering
* Filter reordering

It ensures Spark runs your queries in the most efficient way possible.

---

### **57. What is Tungsten Engine?**

**Answer:**

Tungsten is Spark's **in-memory computation engine**. It improves performance by:

* Managing memory manually off-heap
* Code generation (Whole-stage codegen)
* Binary memory format

Together with Catalyst, it forms the **core performance backbone of Spark**.

---

### **58. How do you persist or cache data in PySpark?**

**Answer:**

```python
df.cache()       # Stores in memory
df.persist()     # Default is MEMORY_AND_DISK

df.unpersist()   # Remove from cache
```

Use when reused across multiple transformations/actions.

---

### **59. When should you use `repartition()` vs `coalesce()`?**

**Answer:**

| Function         | Use Case               | Shuffling  | Increases/Decreases Partitions |
| ---------------- | ---------------------- | ---------- | ------------------------------ |
| `repartition(n)` | To increase partitions | Yes        | Increases or rebalances        |
| `coalesce(n)`    | To reduce partitions   | No/Minimal | Reduces only                   |

Use `repartition()` when writing large files, `coalesce()` for small output jobs.

---

### **60. What is broadcast join and how does it work?**

**Answer:**

A **broadcast join** replicates a small DataFrame to all executor nodes to avoid expensive shuffles.

```python
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id")
```

Best used when one dataset is **very small (few MBs)**.

---

---

# ✅ **Batch 7: PySpark MLlib & Broadcast Variables (61–70)**

---

### **61. What is MLlib in PySpark?**

**Answer:**

**MLlib** is Apache Spark’s **machine learning library**. It provides scalable tools for:

* Classification
* Regression
* Clustering
* Recommendation
* Feature transformation
* Pipelines and evaluation

It supports both **DataFrame-based API (recommended)** and older **RDD-based API** (deprecated).

---

### **62. What are Pipelines in MLlib and why are they important?**

**Answer:**

MLlib Pipelines are inspired by sklearn and help in **building reusable machine learning workflows**.

**Components:**

* `Transformer` – applies transformations (`StringIndexer`, `VectorAssembler`)
* `Estimator` – fits a model (`LogisticRegression`, `RandomForest`)
* `Pipeline` – chains multiple stages together

```python
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[indexer, assembler, model])
model = pipeline.fit(train_data)
```

---

### **63. How do you handle categorical features in PySpark?**

**Answer:**

Using **StringIndexer** and **OneHotEncoder**:

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder

indexer = StringIndexer(inputCol="gender", outputCol="gender_index")
encoder = OneHotEncoder(inputCol="gender_index", outputCol="gender_vec")
```

This converts string categories into numeric or binary vectors.

---

### **64. What is `VectorAssembler` in PySpark?**

**Answer:**

It combines multiple feature columns into a single **vector column** required for ML models.

```python
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["age", "income", "gender_vec"],
    outputCol="features"
)
```

The output column `features` is used by ML models in Spark.

---

### **65. How do you evaluate a classification model in PySpark?**

**Answer:**

Using **evaluator classes**:

```python
from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)
```

Other evaluators: `MulticlassClassificationEvaluator`, `RegressionEvaluator`.

---

### **66. How do you perform hyperparameter tuning in MLlib?**

**Answer:**

Using **CrossValidator** or **TrainValidationSplit**:

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = ParamGridBuilder().addGrid(model.maxDepth, [3, 5, 7]).build()
crossval = CrossValidator(estimator=model, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)
```

---

### **67. What is the difference between RDD-based MLlib and DataFrame-based MLlib?**

| Feature     | RDD-based MLlib | DataFrame-based MLlib       |
| ----------- | --------------- | --------------------------- |
| API         | Deprecated      | Actively maintained         |
| Syntax      | Low-level       | High-level                  |
| Pipelines   | Not supported   | Fully supported             |
| Performance | Less optimized  | Catalyst & Tungsten powered |

✅ Always prefer **DataFrame-based MLlib**.

---

### **68. What is a Broadcast Variable in Spark?**

**Answer:**

A **broadcast variable** allows you to **share read-only data** (e.g., lookup tables) **with all nodes efficiently**.

```python
broadcast_var = sc.broadcast({"HR": 1, "IT": 2})
rdd.map(lambda x: broadcast_var.value.get(x.department, -1))
```

✅ Avoids sending the same data repeatedly with each task.

---

### **69. What are Accumulators in PySpark?**

**Answer:**

Accumulators are **write-only variables** used for **aggregating metrics** (e.g., counters, error counts) across tasks.

```python
acc = sc.accumulator(0)

def count_error(x):
    global acc
    if "error" in x:
        acc += 1

rdd.foreach(count_error)
```

✅ Accumulators are useful for debugging but are **not returned** in transformations.

---

### **70. Can you use accumulators with DataFrames?**

**Answer:**

Direct use of accumulators is limited to **RDD operations**. For DataFrames:

* Use **`foreach()`** or **`foreachPartition()`** to modify accumulators.
* But they **should not** be used for final output logic.

---

---

# ✅ **Batch 8: Structured Streaming, AWS Integration, and Real-Time Processing (71–80)**

---

### **71. What is Structured Streaming in PySpark?**

**Answer:**

**Structured Streaming** is a high-level API in Spark for processing streaming data using the same **DataFrame and SQL API** as batch jobs.

Key features:

* **End-to-end fault tolerance**
* Supports **event-time processing**, **watermarking**
* Output modes: `append`, `update`, `complete`

---

### **72. What are the sources supported by Structured Streaming?**

**Answer:**

Structured Streaming supports:

* **File source** (CSV, JSON, etc.)
* **Kafka**
* **Socket (TCP)**
* **Rate source** (test)

```python
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-topic") \
    .load()
```

---

### **73. What are the output modes in Structured Streaming?**

**Answer:**

1. **Append**: Only new rows are written.
2. **Update**: Only updated rows are written.
3. **Complete**: Entire result table is written every time.

Use based on the type of aggregation.

---

### **74. What is Watermarking in Structured Streaming?**

**Answer:**

**Watermarking** defines a **threshold of event delay tolerance** to manage **late-arriving data**.

```python
df.withWatermark("eventTime", "10 minutes")
```

It helps avoid unbounded state in aggregations.

---

### **75. What are the supported sinks in Structured Streaming?**

**Answer:**

* Console
* Parquet
* Kafka
* Memory (for debugging)
* Foreach (custom sink)

```python
query = df.writeStream.format("parquet").option("path", "/output").start()
```

---

### **76. How do you trigger a PySpark job in AWS EMR using Airflow?**

**Answer:**

Use **Airflow EMR operators** like `EmrAddStepsOperator` or `EmrStepSensor`.

```python
EmrAddStepsOperator(
    task_id="run_spark_job",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster') }}",
    steps=[spark_step],
    aws_conn_id="aws_default"
)
```

Or trigger using `BashOperator` with `spark-submit`.

---

### **77. How do you connect PySpark to AWS S3?**

**Answer:**

Either set the Hadoop AWS config manually:

```python
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.access.key", "<KEY>")
hadoopConf.set("fs.s3a.secret.key", "<SECRET>")
```

Or set environment credentials and use:

```python
df = spark.read.csv("s3a://bucket-name/path/to/file.csv")
```

---

### **78. How do you handle secrets in AWS using PySpark?**

**Answer:**

Use **AWS Secrets Manager** with Boto3:

```python
import boto3
secret = boto3.client('secretsmanager').get_secret_value(SecretId='my_secret')
credentials = json.loads(secret['SecretString'])
```

Inject secrets dynamically into Spark jobs for authentication (e.g., Snowflake, databases).

---

### **79. What’s the difference between Glue and EMR?**

| Feature     | AWS Glue                 | AWS EMR                          |
| ----------- | ------------------------ | -------------------------------- |
| Type        | Serverless ETL service   | Managed Hadoop/Spark cluster     |
| Use case    | Light ETL workloads      | Complex Spark, Hive, Presto jobs |
| Cost model  | Pay per use (per DPU/hr) | Pay per EC2 instance             |
| Flexibility | Less flexible            | Fully configurable environment   |

✅ For PySpark-heavy ETL, **EMR is preferred** for control and scale.

---

### **80. Can you run PySpark jobs directly in AWS Lambda?**

**Answer:**

Yes, but with **severe limitations**:

* Must use **small PySpark scripts**
* Package with **dependencies using a layer**
* Ideal for **micro tasks**, not full data pipelines

👉 Best for triggering glue jobs, or lightweight jobs on small input.

---

Awesome! Let’s dive into **Batch 9**, which focuses on **Testing, Debugging, DevOps practices, and Deployment** in PySpark. These are **must-have skills** for maintaining robust, production-grade data pipelines.

---

# ✅ **Batch 9: Testing, Debugging, DevOps & Deployment (81–90)**

---

### **81. How do you unit test PySpark code?**

**Answer:**

You can use `unittest` or `pytest` in combination with a **local SparkSession**.

Example with `pytest`:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

def test_transformation(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    df2 = df.filter(df["id"] == 1)
    assert df2.count() == 1
```

✅ Mock input/output and validate business logic.

---

### **82. How do you debug a PySpark job locally?**

**Answer:**

* Use `.show()` and `.printSchema()` to inspect data.
* Use `.explain()` to understand query plans.
* Log intermediate outputs.
* Run Spark in **local mode** (`local[*]`) for iterative testing.
* Use **IDE breakpoints** (PyCharm, VSCode) for step-by-step tracing.

---

### **83. What are common logging strategies in PySpark?**

**Answer:**

* Use the built-in `log4j` logger:

```python
log4jLogger = sc._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)
logger.info("Job started.")
```

* Or use Python `logging`:

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
```

✅ Centralize logs in **CloudWatch, S3, or Elasticsearch** for production observability.

---

### **84. What does `.explain()` do in PySpark?**

**Answer:**

`.explain()` prints the **physical and logical plan** used by the Catalyst Optimizer.

```python
df.select("name").where("age > 25").explain()
```

Helps in performance tuning by showing **execution plan** like:

* Filter Pushdown
* Broadcast Joins
* Shuffle operations

---

### **85. What is Spark UI?**

**Answer:**

The **Spark UI** is a web interface (`localhost:4040`) that provides insight into:

* Stages, tasks, and jobs
* DAG visualization
* Executor logs
* Storage and Environment tabs

✅ Essential for **debugging performance bottlenecks** in distributed jobs.

---

### **86. How do you version control PySpark projects?**

**Answer:**

* Use **Git/GitHub** to track scripts and configs
* Follow modular structure:

  ```
  ├── dags/
  ├── src/
  │   ├── transforms/
  │   ├── utils/
  ├── tests/
  └── requirements.txt
  ```
* Use `.gitignore` to avoid pushing large data/log files.

---

### **87. How do you package a PySpark project for deployment?**

**Answer:**

Structure your project and create a `setup.py` for packaging:

```python
from setuptools import setup, find_packages

setup(
    name='my_pyspark_project',
    version='0.1',
    packages=find_packages(),
)
```

Then install locally:

```bash
pip install .
```

Or upload to S3 and reference it in `spark-submit`.

---

### **88. How do you use Jenkins for CI/CD with PySpark?**

**Answer:**

* Trigger PySpark scripts via `spark-submit` in Jenkins pipeline.
* Automate:

  * Unit testing
  * Linting (`flake8`)
  * Packaging
  * Deployment to EMR or S3

Example `Jenkinsfile` snippet:

```groovy
stage('Run PySpark Job') {
  steps {
    sh 'spark-submit --master yarn --deploy-mode cluster src/main.py'
  }
}
```

---

### **89. What is the role of DAGs in Airflow for PySpark?**

**Answer:**

Airflow DAGs define **task sequences and dependencies**. For PySpark:

* Schedule ETL pipelines
* Trigger EMR/Glue jobs
* Add retries, email alerts, SLAs

Example DAG task:

```python
run_spark = BashOperator(
    task_id='run_etl',
    bash_command='spark-submit etl_job.py',
    dag=dag
)
```

---

### **90. How do you monitor and schedule PySpark jobs?**

**Answer:**

* **Scheduling**: Airflow, Oozie, cron, AWS Step Functions
* **Monitoring**:

  * Logs (stdout, stderr)
  * Spark UI
  * CloudWatch, Datadog
  * Alerts via email, Slack

✅ Use `on_failure_callback` in Airflow for intelligent alerting.

---

Perfect! Let’s now dive into **Batch 10**, which covers **advanced PySpark concepts** including **Delta Lake**, **Apache Iceberg**, **Lakehouse architecture**, and advanced **performance optimization** techniques. These topics are essential for **modern big data platforms**.

---

# ✅ **Batch 10: Advanced Topics & Optimization (91–100)**

---

### **91. What is Delta Lake in PySpark?**

**Answer:**

Delta Lake is an open-source storage layer that brings **ACID transactions**, **schema enforcement**, and **time travel** to Apache Spark.

Key benefits:

* Supports **Update**, **Delete**, **Merge (Upsert)** operations.
* Enables **schema evolution**.
* Provides **versioned tables** using transaction logs (`_delta_log`).

```python
df.write.format("delta").mode("overwrite").save("/path/to/delta_table")
```

---

### **92. What is Apache Iceberg and how does it compare with Delta Lake?**

**Answer:**

Apache Iceberg is a **table format** for huge analytic datasets.

* Supports **schema evolution**, **partition evolution**, **hidden partitioning**.
* Works across multiple engines: **Spark, Trino, Flink, Hive**.
* **No lock-in** (open format, used with Parquet/ORC).

| Feature           | Delta Lake  | Apache Iceberg        |
| ----------------- | ----------- | --------------------- |
| ACID Transactions | ✅           | ✅                     |
| Time Travel       | ✅           | ✅                     |
| Multi-engine      | ❌ (limited) | ✅ (Spark, Flink, etc) |
| Cloud-native      | ✅           | ✅                     |

---

### **93. What is Lakehouse architecture?**

**Answer:**

Lakehouse = **Data Warehouse + Data Lake**
It combines the flexibility of data lakes (e.g., S3) with the **performance and reliability** of data warehouses.

Key Traits:

* **ACID support** via Delta/Iceberg/Hudi
* **Unified storage** for structured and semi-structured data
* Supports **BI + ML** from a single source

Popular frameworks: **Databricks Lakehouse**, **Delta Lake**, **Iceberg**, **Dremio**

---

### **94. What is Z-Ordering in Delta Lake?**

**Answer:**

Z-Ordering is a **multi-dimensional clustering technique** that co-locates related data in the same file block to speed up filtering.

```python
df.write.format("delta").option("dataChange", "false")\
  .option("zorderBy", "customer_id")\
  .save("/delta/events")
```

✅ Best used when **frequently filtering by a column** like `customer_id`, `event_type`, etc.

---

### **95. What is OPTIMIZE in Delta Lake and how does it help?**

**Answer:**

`OPTIMIZE` compacts small files in a Delta table into larger files to improve query performance.

```sql
OPTIMIZE delta.`/path/to/table` ZORDER BY (column_name)
```

* Reduces **shuffle time**
* Improves **IO efficiency**
* Often used in **streaming scenarios**

---

### **96. How do you handle schema evolution in Delta Lake?**

**Answer:**

Delta Lake supports automatic or manual **schema evolution**.

```python
df.write.format("delta").mode("append")\
   .option("mergeSchema", "true").save("/delta/events")
```

* Merges new columns
* Prevents write failures on changing schema

---

### **97. What is Data Skew and how do you fix it in PySpark?**

**Answer:**

**Data Skew** happens when **some keys have disproportionately large data**, causing **uneven task distribution** and slow jobs.

Fixes:

* **Salting** (append random values to skewed keys)
* **Broadcast joins**
* **Custom partitioning** using `.repartition()` or `.partitionBy()`

---

### **98. How does the Catalyst Optimizer work in Spark?**

**Answer:**

Catalyst Optimizer is Spark’s query optimizer. It performs:

* **Logical Plan Analysis**
* **Rule-based Transformations**
* **Physical Plan Generation**
* **Cost-based Optimization (CBO)**

✅ Catalyst converts your high-level query into the most **efficient executable form**.

---

### **99. What is Tungsten Engine in Spark?**

**Answer:**

Tungsten is Spark’s execution engine focused on **memory and CPU optimization**.

Features:

* **Whole-stage code generation** (WSCG)
* **Binary memory management**
* **Cache-aware computation**
* **Vectorized reading**

Tungsten + Catalyst = Lightning-fast Spark

---

### **100. What are best practices for optimizing large PySpark jobs?**

**Answer:**

* Use **parquet** format for IO efficiency
* Avoid `collect()` in large datasets
* Tune:

  * `spark.sql.shuffle.partitions`
  * `spark.executor.memory`, `spark.executor.cores`
* Prefer **DataFrame API over RDD**
* Repartition wisely: `repartition()` vs `coalesce()`
* Leverage **broadcast joins**
* Cache intelligently with `.cache()` or `.persist()`
* Monitor via **Spark UI**

---





