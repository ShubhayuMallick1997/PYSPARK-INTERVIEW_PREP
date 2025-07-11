# **60 More PySpark Interview Questions** across **All Topics** 
### — including fundamentals, APIs, performance, streaming, cloud, and real-world scenarios. These are great for advanced interviews, case studies, and system design rounds.

---

### ✅ **16. Fundamentals & APIs**

101. What are the core components of the Spark architecture?
102. Explain the role of the driver and executors in Spark.
103. What is lazy evaluation in PySpark? Why is it important?
104. How does `collect()` differ from `take(n)`?
105. What are accumulators in Spark? When do you use them?
106. How do actions trigger execution in Spark?
107. Why is Spark considered a DAG-based engine?
108. How does Spark handle task scheduling?
109. When should you use RDDs over DataFrames?
110. What is the role of Catalyst Optimizer?

---

### ✅ **17. Transformations & Actions**

111. What is the difference between `map()` and `flatMap()`?
112. How does `groupByKey()` differ from `reduceByKey()`?
113. What’s the difference between `select()`, `withColumn()`, and `selectExpr()`?
114. How do you add a constant column to a DataFrame?
115. How do you remove duplicates from a PySpark DataFrame?
116. Explain how the `when()` and `otherwise()` functions work.
117. How do you perform aggregation on multiple columns?
118. How do you implement ranking within a window function?
119. What is the use of `alias()` in DataFrames?
120. Explain the concept of broadcast variables with examples.

---

### ✅ **18. File Handling**

121. How do you read a multi-line JSON file in PySpark?
122. How do you read compressed files (gzip, snappy) in Spark?
123. Explain partitionBy vs bucketBy when writing files.
124. What are the benefits of Parquet over CSV?
125. How do you optimize reads from a large dataset on S3?
126. How can you read from multiple directories using wildcards?
127. How do you enforce schema while reading data?
128. What are common issues with schema inference?
129. How does Spark handle corrupt records while reading?
130. How do you handle nested JSON in PySpark?

---

### ✅ **19. SQL & UDFs**

131. How do you create and query a temporary view in PySpark?
132. When would you choose SQL syntax over DSL?
133. How do you register a Python function as a UDF?
134. What are the downsides of using UDFs?
135. How is `pandas_udf` better than regular UDF?
136. How do you write a vectorized UDF?
137. Can UDFs be used in joins? What precautions to take?
138. What happens if a UDF fails silently? How do you debug?
139. How do you pass config variables into UDFs?
140. How does Spark optimize SQL queries?

---

### ✅ **20. Real-Time Streaming**

141. What are sinks and sources in Structured Streaming?
142. How do you set checkpointing in PySpark Streaming?
143. How do you maintain state in streaming aggregations?
144. What are output modes in streaming (append, complete, update)?
145. How do you join streaming data with static reference data?
146. How do you guarantee exactly-once processing?
147. What are watermark delays and how do they work?
148. Can you run multiple streaming queries in parallel?
149. What is the use of `trigger(once=True)`?
150. What tools do you use to monitor streaming jobs?

---

### ✅ **21. Cloud & DevOps**

151. How do you handle schema evolution in S3 data?
152. How do you monitor Spark jobs on EMR?
153. How do you run PySpark scripts in AWS Glue?
154. How do you reduce Spark EMR costs in production?
155. What is the difference between Glue and Glue Studio?
156. How do you handle cluster bootstrapping for PySpark?
157. What CI/CD tools do you use with PySpark?
158. How do you validate PySpark code before production deployment?
159. What is the structure of a deployable PySpark project?
160. How do you use GitHub Actions for PySpark jobs?

---
