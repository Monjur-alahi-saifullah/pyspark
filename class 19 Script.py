### PySpark show()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

### PySpark column operations:

data = [
    ("John", "Doe", 28, "M", 5000, "US", "NY", "Software"),
    ("Jane", "Smith", 32, "F", 6000, "US", "CA", "Engineer"),
    ("Sam", "Brown", 25, "M", 4500, "US", "TX", "Analyst"),
    ("Linda", "Jones", 45, "F", 8000, "US", "FL", "Manager"),
    ("Robert", "White", 50, "M", 7000, "US", "NY", "Sales"),
    ("Michael", "King", 33, "M", 5500, "UK", "LD", "Consultant"),
    ("Sara", "Johnson", 40, "F", 7200, "UK", "LD", "Manager"),
    ("Laura", "Wilson", 29, "F", 5200, "CA", "ON", "Analyst"),
    ("David", "Lee", 38, "M", 6400, "CA", "ON", "Engineer"),
    ("Anna", "Davis", 27, "F", 4800, "CA", "QC", "Analyst"),
    ("Tom", "Moore", 31, "M", 5600, "AU", "SY", "Developer"),
    ("Chris", "Evans", 26, "M", 4900, "AU", "SY", "Consultant")
]

columns = ["first_name", "last_name", "age", "gender", "salary", "country", "state", "job_title"]

df = spark.createDataFrame(data, columns)
df.show()


# alias():

df.select(df.salary.alias("income")).show()

# asc() & desc():

df.orderBy(df.age.asc()).show()  # Ascending order
df.orderBy(df.age.desc()).show() # Descending order

# cast():

df.withColumn("salary_float", df.salary.cast("float")).show()

# between():

df.filter(df.age.between(30, 40)).show()

# contains():

df.filter(df.job_title.contains("Manager")).show()


# startswith() & endswith():

df.filter(df.first_name.startswith("J")).show()
df.filter(df.last_name.endswith("n")).show()

# isNull() & isNotNull():

df.filter(df.state.isNotNull()).show()
df.filter(df.gender.isNull()).show()

# like():

df.filter(df.first_name.like("J%")).show()

# substr():

df.withColumn("first_three_letters", df.first_name.substr(1, 3)).show()

# when() & otherwise():

from pyspark.sql.functions import when

df.withColumn("seniority", 
              when(df.age >= 40, "Senior")
              .when(df.age >= 30, "Mid-level")
              .otherwise("Junior")).show()
# isin():

df.filter(df.state.isin("NY", "CA")).show()


# Select specific columns

df.select("first_name", "salary").show()

# Select columns with expressions

from pyspark.sql.functions import expr

df.select("first_name", "salary", expr("salary * 2 as double_salary")).show()

# withColumn() example

df.withColumn("annual_salary", df.salary * 12).show()

# withColumnRenamed() example

df.withColumnRenamed("first_name", "fname").withColumnRenamed("last_name", "lname").show()

### PySpark – where() & filter():

data = [
    ("John", "Doe", 28, "M", 5000, "US"),
    ("Jane", "Smith", 32, "F", 6000, "US"),
    ("Sam", "Brown", 25, "M", 4500, "US"),
    ("Linda", "Jones", 45, "F", 8000, "US"),
    ("Michael", "King", 33, "M", 5500, "UK"),
    ("Sara", "Johnson", 40, "F", 7200, "UK"),
    ("David", "Lee", 38, "M", 6400, "CA"),
    ("Laura", "Wilson", 29, "F", 5200, "CA")
]

columns = ["first_name", "last_name", "age", "gender", "salary", "country"]
df = spark.createDataFrame(data, columns)
df.show()

# Filter rows where age is greater than 30.

df.where(df.age > 30).show()

# Filter rows where country is 'US' and salary is greater than 5000.

df.where((df.country == "US") & (df.salary > 5000)).show()


# Filter rows where gender is 'F'.

df.filter(df.gender == "F").show()

# Filter rows where salary is between 5000 and 7000.

df.filter(df.salary.between(5000, 7000)).show()


# PySpark column drop & duplicate removal:

data = [
    ("John", "Doe", 28, "M", 5000, "US"),
    ("Jane", "Smith", 32, "F", 6000, "US"),
    ("Sam", "Brown", 25, "M", 4500, "US"),
    ("Linda", "Jones", 45, "F", 8000, "US"),
    ("John", "Doe", 28, "M", 5000, "US"),  # Duplicate row
    ("Michael", "King", 33, "M", 5500, "UK"),
    ("Sara", "Johnson", 40, "F", 7200, "UK"),
    ("David", "Lee", 38, "M", 6400, "CA"),
    ("Laura", "Wilson", 29, "F", 5200, "CA"),
    ("Michael", "King", 33, "M", 5500, "UK"),  # Duplicate row
    ("Laura", "Wilson", 45, "F", 3000, "CA"),
]

columns = ["first_name", "last_name", "age", "gender", "salary", "country"]

df = spark.createDataFrame(data, columns)
df.show()


# Drop the salary and country columns.

df_dropped = df.drop("salary", "country")
df_dropped.show()


# Remove duplicate rows based on all columns.

df_no_duplicates = df.dropDuplicates()
df_no_duplicates.show()


# Remove duplicate rows based on specific columns.

df_no_duplicates = df.dropDuplicates(["first_name", "last_name"])
df_no_duplicates.show()


# PySpark – distinct():

data = [
    (1, "Alice", 28),
    (2, "Bob", 35),
    (3, "Charlie", 28),
    (1, "Alice", 28),  # Duplicate row
    (4, "David", 40),
    (2, "Bob", 35)    # Duplicate row
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

# Remove duplicate rows
distinct_df = df.distinct()
distinct_df.show()


# Count the number of rows
row_count = df.count()
print(f"Number of rows: {row_count}")


row_count = df.distinct().count()
print(f"Number of rows: {row_count}")

# PySpark – orderBy() and sort():

data = [
    ("John", "Doe", 28, "M", 5000, "US"),
    ("Jane", "Smith", 32, "F", 6000, "US"),
    ("Sam", "Brown", 25, "M", 4500, "US"),
    ("Linda", "Jones", 45, "F", 8000, "US"),
    ("Michael", "King", 38, "M", 5500, "UK"),
    ("Sara", "Johnson", 40, "F", 7200, "UK"),
    ("David", "Lee", 38, "M", 6400, "CA"),
    ("Laura", "Wilson", 29, "F", 5200, "CA")
]

columns = ["first_name", "last_name", "age", "gender", "salary", "country"]

df = spark.createDataFrame(data, columns)
df.show()

# Sort by age in ascending order.

df_ordered_by_age = df.orderBy("age")
df_ordered_by_age.show()

# Sort by salary in descending order.

df_ordered_by_salary_desc = df.orderBy(df.salary.desc())
df_ordered_by_salary_desc.show()


# Sort by first_name in ascending order.

df_sorted_by_first_name = df.sort("first_name")
df_sorted_by_first_name.show()

# Sort by age in descending order and salary in ascending order.

df_sorted_by_age_and_salary = df.sort(df.age.desc(), df.salary.asc())
df_sorted_by_age_and_salary.show()


# PySpark – union() & unionAll():

spark = SparkSession.builder.appName('SparkUnion').getOrCreate()

simpleData1 = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns1= ["employee_name","department","state","salary","age","bonus"]
df1 = spark.createDataFrame(data = simpleData1, schema = columns1)

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("David","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)


# union() to merge two DataFrames

unionDF = df1.union(df2)
unionDF.show(truncate=False)


# unionAll() to merge two DataFrames

unionAllDF = df1.unionAll(df2)
unionAllDF.show(truncate=False)

# PySpark – fillna() :

data = [
    ("John", "Doe", 28, None),
    ("Jane", "Smith", None, "F"),
    ("Sam", "Brown", 25, "M"),
    ("Linda", "Jones", 45, "F"),
    ("Michael", None, 33, "M")
]
columns = ["first_name", "last_name", "age", "gender"]
df = spark.createDataFrame(data, columns)
df.show()

# Fill missing values in a specific column.

df_filled_last_name = df.fillna({"last_name": "Unknown"})
df_filled_last_name.show()

# Fill missing values in all columns.

df_filled_all = df.fillna({
    "first_name": "Unknown",
    "last_name": "Unknown",
    "age": 0,
    "gender": "Not Specified"
})
df_filled_all.show()


# PySpark Joins:

data1 = [
    (1, "John", "Doe"),
    (2, "Jane", "Smith"),
    (3, "Sam", "Brown"),
    (4, "Linda", "Jones")
]

data2 = [
    (1, "HR", 5000),
    (2, "Finance", 6000),
    (3, "IT", 5500),
    (5, "Marketing", 7000)  # Note: ID 5 does not exist in df1
]

columns1 = ["id", "first_name", "last_name"]
columns2 = ["id", "department", "salary"]

df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)

# Inner join on 'id'

inner_join_df = df1.join(df2, on="id", how="inner")
inner_join_df.show()

# Left join on 'id'

left_join_df = df1.join(df2, on="id", how="left")
left_join_df.show()

# Right join on 'id'

right_join_df = df1.join(df2, on="id", how="right")
right_join_df.show()

# Full outer join on 'id'

full_outer_join_df = df1.join(df2, on="id", how="outer")
full_outer_join_df.show()


# PySpark countDistinct():

from pyspark.sql.functions import countDistinct

spark = SparkSession.builder.appName('example').getOrCreate()
data = [
    (1, "Alice", 28),
    (2, "Bob", 35),
    (3, "Charlie", 28),
    (4, "David", 40),
    (5, "Alice", 28),  # Duplicate name
    (6, "Bob", 35)     # Duplicate name
]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)
df.show()

# Count distinct values in a specific column.

distinct_name_count = df.agg(countDistinct("name").alias("distinct_name_count"))
distinct_name_count.show()

# Count distinct combinations of 'name' and 'age’

distinct_combination_count = df.agg(countDistinct("name", "age").alias("distinct_combination_count"))
distinct_combination_count.show()

# Applying distinct() and count()

data = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Alice", "Marketing", 2000),
    ("Charlie", "Sales", 4100)
  ]
columns = ["Name","Dept","Salary"]
df = spark.createDataFrame(data=data,schema=columns)
df.show()


df1 = df.distinct()
print(df1.count())

df1 = df.distinct().count()
print(df1)


### SparkSQL:

# 1:

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkSQLExample').getOrCreate()
data = [
    (1, "Alice", 28),
    (2, "Bob", 35),
    (3, "Charlie", 28),
    (4, "David", 40),
    (5, "David", 50)
]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)
df.show()

df.createOrReplaceTempView("people")

result = spark.sql("SELECT * FROM people WHERE age > 30")
result.show()

# Find average

from pyspark.sql.functions import avg

result = spark.sql("SELECT avg(age) as average_age FROM people")
result.show()

# Filtering and aggregating

result = spark.sql("""
    SELECT name, COUNT(*) as count, AVG(age) as average_age
    FROM people
    GROUP BY name
    HAVING COUNT(*) > 1
""")
result.show()

# Query to filter out rows with null values

result = spark.sql("SELECT * FROM people WHERE age IS NOT NULL")
result.show()

# SQL query using CASE WHEN

result = spark.sql("""
    SELECT id, name, 
           CASE 
               WHEN age < 30 THEN 'Young'
               WHEN age BETWEEN 30 AND 40 THEN 'Middle-aged'
               ELSE 'Old'
           END AS age_category
    FROM people
""")
result.show()


# Joining

data2 = [
    (1, "Alice", "A"),
    (2, "Bob", "B"),
    (4, "David", "D")
]
columns2 = ["id", "full_name", "grade"]
df2 = spark.createDataFrame(data2, columns2)

df2.createOrReplaceTempView("people_grades")


result = spark.sql("""
    SELECT p.id, p.name, pg.grade
    FROM people p
    JOIN people_grades pg
    ON p.id = pg.id AND p.name = pg.full_name
""")
result.show()


# SQL query to select distinct values

result = spark.sql("SELECT DISTINCT age FROM people")
result.show()

# SQL query with a subquery

result = spark.sql("""
    SELECT * FROM people
    WHERE age > (SELECT AVG(age) FROM people)
""")
result.show()

# Union

data3 = [
    (6, "Eve", 29),
    (7, "Frank", 33)
]
columns3 = ["id", "name", "age"]
df3 = spark.createDataFrame(data3, columns3)
df3.createOrReplaceTempView("people_extra")

result = spark.sql("""
    SELECT id, name, age FROM people
    UNION
    SELECT id, name, age FROM people_extra
""")
result.show()


# Select Distinct Count

spark.sql("SELECT COUNT(DISTINCT age) AS unique_ages FROM people").show()

#  Count Rows

spark.sql("SELECT COUNT(*) AS total_rows FROM people").show()


### PySpark SQL expr()

# Use expr() to perform an arithmetic operation

from pyspark.sql.functions import expr

data = [(1, 10), (2, 20), (3, 30)]
columns = ["id", "value"]
df = spark.createDataFrame(data, columns)

df_with_calculation = df.withColumn("value_times_two", expr("value * 2"))
df_with_calculation.show()

# String Concatenation

data = [("Alice", "Smith"), ("Bob", "Johnson")]
columns = ["first_name", "last_name"]
df = spark.createDataFrame(data, columns)

df_concatenated = df.withColumn("full_name", expr("concat(first_name, ' ', last_name)"))
df_concatenated.show()


# Date Formatting

from pyspark.sql.functions import current_date

df = spark.createDataFrame([(1,)], ["id"]).withColumn("current_date", current_date())
df.show()

df_formatted_date = df.withColumn("formatted_date", expr("date_format(current_date, 'MM/dd/yyyy')"))
df_formatted_date.show()

# Conditional Expressions

data = [(1, 25), (2, 40), (3, 15)]
columns = ["id", "age"]
df = spark.createDataFrame(data, columns)

df_with_category = df.withColumn(
    "age_category",
    expr("""
        CASE
            WHEN age < 18 THEN 'Minor'
            WHEN age BETWEEN 18 AND 35 THEN 'Young Adult'
            ELSE 'Adult'
        END
    """)
)
df_with_category.show()


# Column Aliasing

data = [(1, 1000), (2, 2000)]
columns = ["id", "sales"]
df = spark.createDataFrame(data, columns)
df.show()

df_aliased = df.select(expr("id as identifier"), expr("sales as revenue"))
df_aliased.show()

# Case Sensitivity

data = [("Alice", "Smith"), ("bob", "JOHNSON")]
columns = ["first_name", "last_name"]
df = spark.createDataFrame(data, columns)
df.show()

df_case_sensitive = df.withColumn(
    "lower_last_name",
    expr("lower(last_name)")
)
df_case_sensitive.show()


# Using if Statements

# 1:

data = [(1, 60), (2, 85)]
columns = ["id", "score"]
df = spark.createDataFrame(data, columns)
df.show()

df_score = df.withColumn(
    "status",
    expr("IF(score >= 75, 'Pass', 'Fail')")
)
df_score.show()


# 2:

data = [(85, "John"), (45, "Jane"), (65, "Alice"), (30, "Bob")]
columns = ["score", "name"]
df = spark.createDataFrame(data, columns)
df.show()

df.withColumn(
    "grade",
    expr("""
        IF(score >= 80, 'A', 
            IF(score >= 60, 'B', 
                IF(score >= 40, 'C', 'F')))
    """)
).show()

### PySpark to_date():

from pyspark.sql.functions import to_date,to_timestamp

spark = SparkSession.builder.appName("to_date_example").getOrCreate()

data = [("2024-09-01 12:00:00", "Alice", 1, 1000),
        ("2024-10-15 09:30:00", "Bob", 2, 2000),
        ("2024-12-01 17:45:00", "Charlie", 3, 1500),
        ("2024-06-21 17:45:00", "John", 4, 2500)]
columns = ["date_str", "name", "id", "amount"]
df = spark.createDataFrame(data, columns)
df.show(truncate=False)

# Convert 'date_str' to a proper date format (yyyy-MM-dd)

df_with_date = df.withColumn("formatted_date", to_timestamp('date_str').cast('date'))
df_with_date.show(truncate=False)


# Spark SQL with the to_date() function:
 
spark.sql("SELECT to_date('2023-12-25 08:45:12.000') as date_type").show()


### PySpark to_timestamp():

from pyspark.sql.functions import  to_timestamp, date_format

data = [("02-Mar-2021 17:30:15",), ("15-Apr-2022 09:15:30",), ("28-Dec-2023 23:45:00",)]
columns = ["date_str"]
df = spark.createDataFrame(data, columns)
df.show()

# Basic to_timestamp() usage:

df_with_timestamp = df.withColumn("timestamp", date_format(
        to_timestamp(df["date_str"], "dd-MMM-yyyy HH:mm:ss"),
        "yyyy-MM-dd HH:mm:ss"
    ))
df_with_timestamp.show(truncate=False)


# Spark SQL with to_timestamp() 

df.createOrReplaceTempView("timestamps_table")

result_df = spark.sql("""
    SELECT 
        date_str,
        to_timestamp(date_str, 'dd-MMM-yyyy HH:mm:ss') AS formatted_timestamp
    FROM timestamps_table
""")
result_df.show(truncate=False)


### PySpark aggregate functions:

from pyspark.sql.functions import sum, avg, max, min, count

spark = SparkSession.builder.appName("AggregateFunctionsExample").getOrCreate()
data = [("Alice", 1, 1000, "IT"),
        ("Bob", 2, 1500, "HR"),
        ("Charlie", 3, 1200, "Sales"),
        ("David", 4, 2000, "HR"),
        ("Eve", 5, 2500, "IT"),
        ("Frank", 6, 800, "Sales"),
        ("Alice", 7, 3000, "IT")]

columns = ["name", "id", "amount", "department"]
df = spark.createDataFrame(data, columns)
df.show(truncate=False)

# Group by department and calculate the sum and average of the amount

df.groupBy("department") \
  .agg(sum("amount").alias("total_amount")) \
  .show(truncate=False)

# Group by department and name, and calculate the sum of the amount

df.groupBy("department", "name") \
  .agg(sum("amount").alias("total_amount")) \
  .show(truncate=False)


# Group by department and apply multiple aggregations on the amount

df.groupBy("department") \
  .agg(
      sum("amount").alias("total_amount"),
      max("amount").alias("max_amount"),
      min("amount").alias("min_amount")
  ).show(truncate=False)

# Filter departments where total amount is greater than 3000

df.groupBy("department") \
  .agg(sum("amount").alias("total_amount")) \
  .filter("total_amount > 3000") \
  .show(truncate=False)

### col() function in PySpark:


# Basic column selection

from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ColExample").getOrCreate()
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()

df.select(col("Name")).show()

# Renaming a column

df.withColumn("PersonName", col("Name")).drop("Name").show()

# Performing calculations

data = [("Alice", 1000), ("Bob", 1500), ("Charlie", 1200)]
columns = ["Name", "Salary"]
df = spark.createDataFrame(data, columns)
df.show()

df.withColumn("NewSalary", col("Salary") * 1.10).show()


# Filter rows where Age is greater than 30

df.filter(col("Salary") > 1000).show()

# Using col() in expressions

from pyspark.sql.functions import expr

data = [("Alice", 5, 3), ("Bob", 10, 4), ("Charlie", 7, 6)]
columns = ["Name", "Value1", "Value2"]
df = spark.createDataFrame(data, columns)
df.show()

df.withColumn("Sum", col("Value1") + col("Value2")).show()

# Convert names to uppercase

from pyspark.sql.functions import upper

data = [("Alice",), ("Bob",), ("Charlie",)]
columns = ["Name"]
df = spark.createDataFrame(data, columns)
df.show()

df.withColumn("UpperName", upper(col("Name"))).show()


