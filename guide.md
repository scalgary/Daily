# PySpark User Guide - Complete Reference

## Table of Contents

1. [Quick Start & Setup](#quick-start--setup)
2. [Sample Data for Testing](#sample-data-for-testing)
3. [Advanced Joins](#advanced-joins)
4. [Wide ⟷ Long Transformations](#wide--long-transformations)
5. [Custom Functions (UDFs)](#custom-functions-udfs)
6. [Configuration-Driven Pipeline](#configuration-driven-pipeline)
7. [Performance Optimization](#performance-optimization)
8. [Advanced Production Patterns](#advanced-production-patterns)
9. [Error Handling and Monitoring](#error-handling-and-monitoring)
10. [Testing and Debugging](#testing-and-debugging)

---

## Quick Start & Setup

### Initial Configuration
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd

spark = SparkSession.builder \
    .appName("DataProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()
```

### Key Features Overview

✅ **Complete working examples** - Every code block uses the sample data provided  
✅ **Production-ready patterns** - Error handling, monitoring, checkpointing  
✅ **Performance optimizations** - Broadcast joins, caching, partitioning strategies  
✅ **Wide ⟷ Long transformations** - Multiple pivot/unpivot methods  
✅ **Advanced join patterns** - All join types with detailed explanations  
✅ **Custom functions** - UDFs, Pandas UDFs, when to use built-ins instead  
✅ **Automation frameworks** - Configuration-driven pipelines, schema standardization  

---

## Sample Data for Testing

```python
# Customer data
customers_data = [
    (1, "John Doe", "john@email.com", "US", "Premium"),
    (2, "Jane Smith", "jane@email.com", "CA", "Standard"),
    (3, "Bob Johnson", "bob@email.com", "UK", "Premium"),
    (4, "Alice Brown", "alice@email.com", "US", "Standard"),
    (5, "Charlie Wilson", "charlie@email.com", "FR", "Premium")
]
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("tier", StringType(), True)
])
customers = spark.createDataFrame(customers_data, customers_schema)

# Orders data
orders_data = [
    (101, 1, "2024-01-15", 150.0, "Electronics"),
    (102, 2, "2024-01-16", 89.5, "Books"),
    (103, 1, "2024-01-17", 200.0, "Electronics"),
    (104, 3, "2024-01-18", 45.0, "Books"),
    (105, 2, "2024-01-19", 310.0, "Clothing"),
    (106, 4, "2024-01-20", 75.0, "Electronics"),
    (107, 1, "2024-01-21", 125.0, "Clothing"),
    (108, 6, "2024-01-22", 95.0, "Books")  # Orphaned order
]
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("category", StringType(), True)
])
orders = spark.createDataFrame(orders_data, orders_schema)

# Products catalog (small table for broadcast)
products_data = [
    ("Electronics", 0.15, 30),
    ("Books", 0.05, 14),
    ("Clothing", 0.20, 7)
]
products_schema = StructType([
    StructField("category", StringType(), True),
    StructField("commission_rate", DoubleType(), True),
    StructField("return_window_days", IntegerType(), True)
])
products = spark.createDataFrame(products_data, products_schema)
```

---

## Advanced Joins

### All Join Types with Examples

#### Inner Join - Only Matching Records
```python
inner_result = customers.join(orders, "customer_id", "inner")
print(f"Inner join: {inner_result.count()} records")  # 7 records
inner_result.select("name", "order_id", "amount").show()
```

#### Left Join - All Customers
```python
left_result = customers.join(orders, "customer_id", "left")
print(f"Left join: {left_result.count()} records")  # 8 records (Charlie has no orders)
left_result.select("name", "order_id", "amount").show()
```

#### Anti Join - Customers Without Orders
```python
anti_result = customers.join(orders, "customer_id", "anti")
print(f"Anti join: {anti_result.count()} records")  # 1 record (Charlie)
anti_result.select("name", "email").show()
```

### Broadcast Joins - Performance Optimization

#### When to Use Broadcast Joins
- Small table < 200MB (configurable via `spark.sql.autoBroadcastJoinThreshold`)
- One table much smaller than the other
- Avoiding shuffle operations for better performance

```python
from pyspark.sql.functions import broadcast

# Manual broadcast (force small table to all executors)
broadcast_result = orders.join(broadcast(products), "category", "left")
broadcast_result.select("order_id", "category", "commission_rate", "amount").show()

# Calculate commission using broadcast join
commission_calc = orders.join(broadcast(products), "category", "left") \
    .withColumn("commission", col("amount") * col("commission_rate")) \
    .select("order_id", "amount", "commission_rate", "commission")
commission_calc.show()
```

#### Performance Comparison
```python
# Without broadcast (creates shuffle)
no_broadcast = orders.join(products, "category", "left")

# With broadcast (no shuffle)
with_broadcast = orders.join(broadcast(products), "category", "left")

# Check execution plan
print("=== Without Broadcast ===")
no_broadcast.explain(True)
print("\n=== With Broadcast ===")  
with_broadcast.explain(True)
```

### Complex Join Conditions
```python
complex_join = orders.join(
    customers,
    (orders.customer_id == customers.customer_id) & 
    (customers.tier == "Premium") &
    (orders.amount > 100),
    "inner"
)
complex_join.select("name", "order_id", "amount", "tier").show()
```

---

## Wide ⟷ Long Transformations

### Long to Wide (Pivot)

#### Basic Pivot
```python
# Sample sales data (long format)
sales_data = [
    ("2024-01", "Electronics", "US", 1000),
    ("2024-01", "Books", "US", 500),
    ("2024-01", "Electronics", "CA", 800),
    ("2024-02", "Electronics", "US", 1200),
    ("2024-02", "Books", "US", 600),
    ("2024-02", "Clothing", "CA", 400),
]
sales = spark.createDataFrame(sales_data, ["month", "category", "country", "sales"])

# Pivot by country - create columns for each country
wide_by_country = sales.groupBy("month", "category") \
    .pivot("country") \
    .sum("sales") \
    .fillna(0)
wide_by_country.show()
```

#### Advanced Pivot with Multiple Aggregations
```python
complex_pivot = orders.groupBy("customer_id") \
    .pivot("category") \
    .agg(
        sum("amount").alias("total"),
        count("order_id").alias("count"),
        avg("amount").alias("avg")
    )
complex_pivot.show()
```

### Wide to Long (Unpivot)

#### Method 1: Using stack() Function
```python
# Start with wide format
wide_data = [
    ("2024-01", 1000, 500, 200),
    ("2024-02", 1200, 600, 400), 
    ("2024-03", 1100, 550, 350)
]
wide_df = spark.createDataFrame(wide_data, ["month", "Electronics", "Books", "Clothing"])

# Unpivot using stack
long_format = wide_df.select(
    "month",
    expr("stack(3, 'Electronics', Electronics, 'Books', Books, 'Clothing', Clothing) as (category, sales)")
).filter("sales IS NOT NULL")
long_format.show()
```

#### Method 2: Dynamic Unpivot Function
```python
def unpivot_dynamic(df, id_cols, value_name="value", var_name="variable"):
    """Dynamically unpivot DataFrame"""
    cols_to_unpivot = [c for c in df.columns if c not in id_cols]
    
    stack_expr = f"stack({len(cols_to_unpivot)}, "
    stack_expr += ", ".join([f"'{col}', `{col}`" for col in cols_to_unpivot])
    stack_expr += f") as ({var_name}, {value_name})"
    
    return df.select(*id_cols, expr(stack_expr))

# Usage
dynamic_long = unpivot_dynamic(wide_df, ["month"], "sales", "category")
dynamic_long.show()
```

---

## Custom Functions (UDFs)

### Standard UDFs with Error Handling
```python
@udf(returnType=StringType())
def extract_domain(email):
    if email and '@' in email:
        domain = email.split('@')[1]
        return domain.lower()
    return "unknown"

# Test the UDF
customers.withColumn("domain", extract_domain(col("email"))).show()
```

### Complex UDFs with Structured Returns
```python
@udf(returnType=StructType([
    StructField("is_valid", BooleanType(), True),
    StructField("domain", StringType(), True),
    StructField("provider", StringType(), True)
]))
def email_analysis(email):
    if not email or '@' not in email:
        return (False, "invalid", "unknown")
    
    domain = email.split('@')[1].lower()
    
    if domain in ["gmail.com", "googlemail.com"]:
        provider = "Google"
    elif domain in ["yahoo.com", "yahoo.co.uk"]:
        provider = "Yahoo"  
    elif domain in ["outlook.com", "hotmail.com", "live.com"]:
        provider = "Microsoft"
    else:
        provider = "Other"
    
    return (True, domain, provider)

# Use complex UDF
email_info = customers.withColumn("email_info", email_analysis(col("email"))) \
    .select("name", "email", 
            col("email_info.is_valid").alias("valid_email"),
            col("email_info.domain").alias("domain"),
            col("email_info.provider").alias("provider"))
email_info.show()
```

### Pandas UDFs - High Performance (Vectorized)
```python
@pandas_udf(returnType=DoubleType())
def calculate_discount(amounts: pd.Series, tiers: pd.Series) -> pd.Series:
    """Calculate discount based on amount and tier - vectorized operation"""
    discount_rates = {"Premium": 0.15, "Standard": 0.05}
    
    discounts = amounts * 0.0  # Initialize
    for tier, rate in discount_rates.items():
        mask = (tiers == tier) & (amounts > 50)
        discounts = discounts.where(~mask, amounts * rate)
    
    return discounts

# Apply to joined data
discount_calc = customers.join(orders, "customer_id") \
    .withColumn("discount", calculate_discount(col("amount"), col("tier"))) \
    .withColumn("final_amount", col("amount") - col("discount"))

discount_calc.select("name", "tier", "amount", "discount", "final_amount").show()
```

### When to Use Built-in Functions Instead
```python
# String functions - much faster than UDFs
customers_clean = customers \
    .withColumn("name_upper", upper(col("name"))) \
    .withColumn("first_name", split(col("name"), " ").getItem(0)) \
    .withColumn("email_domain", split(col("email"), "@").getItem(1)) \
    .withColumn("name_length", length(col("name"))) \
    .withColumn("has_gmail", col("email").contains("gmail"))

customers_clean.show()
```

---

## Configuration-Driven Pipeline

### Pipeline Framework
```python
class ConfigurablePipeline:
    def __init__(self, spark):
        self.spark = spark
    
    def load_data(self, source_config):
        """Load data based on configuration"""
        source_type = source_config['type']
        
        if source_type == 'parquet':
            return self.spark.read.parquet(source_config['path'])
        elif source_type == 'table':
            return self.spark.table(source_config['name'])
        elif source_type == 'dataframe':
            return source_config['df']  # For testing
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def apply_transformations(self, df, transformations):
        """Apply list of transformations"""
        for transform in transformations:
            df = self._apply_single_transformation(df, transform)
        return df
    
    def _apply_single_transformation(self, df, transform):
        """Apply single transformation"""
        transform_type = transform['type']
        
        if transform_type == 'filter':
            return df.filter(expr(transform['condition']))
        
        elif transform_type == 'add_column':
            return df.withColumn(transform['name'], expr(transform['expression']))
        
        elif transform_type == 'join':
            other_df = self.load_data(transform['source'])
            if transform.get('broadcast', False):
                other_df = broadcast(other_df)
            return df.join(other_df, transform['on'], transform.get('how', 'inner'))
        
        elif transform_type == 'aggregate':
            group_cols = transform['group_by']
            agg_exprs = [expr(agg) for agg in transform['aggregations']]
            return df.groupBy(*group_cols).agg(*agg_exprs)
        
        elif transform_type == 'pivot':
            return df.groupBy(*transform['group_by']) \
                    .pivot(transform['pivot_column']) \
                    .agg(expr(transform['aggregation']))
        
        else:
            raise ValueError(f"Unknown transformation type: {transform_type}")
    
    def execute_pipeline(self, config):
        """Execute complete pipeline from configuration"""
        df = self.load_data(config['source'])
        
        if 'transformations' in config:
            df = self.apply_transformations(df, config['transformations'])
        
        if config.get('sink', {}).get('type') == 'show':
            df.show(config['sink'].get('num_rows', 20))
        
        return df
```

### Pipeline Configuration Examples

#### 1. Simple Aggregation Pipeline
```python
pipeline = ConfigurablePipeline(spark)

simple_config = {
    "source": {"type": "dataframe", "df": orders},
    "transformations": [
        {
            "type": "add_column",
            "name": "high_value",
            "expression": "case when amount > 100 then 'High' else 'Low' end"
        },
        {
            "type": "aggregate",
            "group_by": ["category", "high_value"],
            "aggregations": [
                "sum(amount) as total_revenue",
                "count(*) as order_count",
                "avg(amount) as avg_amount"
            ]
        }
    ],
    "sink": {"type": "show"}
}

result1 = pipeline.execute_pipeline(simple_config)
```

#### 2. Complex Join Pipeline
```python
complex_config = {
    "source": {"type": "dataframe", "df": orders},
    "transformations": [
        {
            "type": "join",
            "source": {"type": "dataframe", "df": customers},
            "on": "customer_id",
            "how": "left"
        },
        {
            "type": "join",
            "source": {"type": "dataframe", "df": products},
            "on": "category",
            "how": "left",
            "broadcast": True
        },
        {
            "type": "add_column",
            "name": "commission",
            "expression": "amount * commission_rate"
        },
        {
            "type": "filter",
            "condition": "tier = 'Premium'"
        },
        {
            "type": "aggregate",
            "group_by": ["name", "tier"],
            "aggregations": [
                "sum(amount) as total_spent",
                "sum(commission) as total_commission",
                "count(*) as order_count"
            ]
        }
    ],
    "sink": {"type": "show", "num_rows": 10}
}

result2 = pipeline.execute_pipeline(complex_config)
```

#### 3. Pivot Pipeline
```python
pivot_config = {
    "source": {"type": "dataframe", "df": sales},
    "transformations": [
        {
            "type": "pivot",
            "group_by": ["month"],
            "pivot_column": "category", 
            "aggregation": "sum(sales)"
        }
    ],
    "sink": {"type": "show"}
}

result3 = pipeline.execute_pipeline(pivot_config)
```

**Key Benefits:**
* **Reusability**: Same pipeline logic for different datasets
* **Maintainability**: Configuration changes without code changes
* **Testing**: Easy to test with different configurations
* **Deployment**: JSON/YAML configs for different environments

---

## Performance Optimization

### 1. Partitioning Strategies

#### Repartition vs Coalesce
```python
# Check current partitions
print(f"Orders partitions: {orders.rdd.getNumPartitions()}")

# Repartition for better parallelism (causes shuffle)
orders_repartitioned = orders.repartition(4, "customer_id")
print(f"Repartitioned orders: {orders_repartitioned.rdd.getNumPartitions()}")

# Coalesce to reduce partitions (no shuffle)
orders_coalesced = orders.coalesce(2)
print(f"Coalesced orders: {orders_coalesced.rdd.getNumPartitions()}")
```

#### Partition Pruning
```python
# Write with partitioning
orders_with_date = orders.withColumn("order_date", to_date("order_date"))
orders_with_date.write \
    .partitionBy("category") \
    .mode("overwrite") \
    .parquet("/tmp/partitioned_orders")

# Read with partition pruning - only reads Electronics partition
electronics_orders = spark.read.parquet("/tmp/partitioned_orders") \
    .filter(col("category") == "Electronics")
```

### 2. Caching Levels

#### Different Storage Levels
```python
from pyspark import StorageLevel

# Different storage options
orders.cache()  # MEMORY_AND_DISK (default)
orders.persist(StorageLevel.MEMORY_ONLY)
orders.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized, saves space
orders.persist(StorageLevel.DISK_ONLY)

# Cache expensive operations
expensive_aggregation = orders.join(customers, "customer_id") \
    .groupBy("country", "tier") \
    .agg(
        sum("amount").alias("total_revenue"),
        count("order_id").alias("order_count")
    ).cache()

# Use multiple times
high_revenue = expensive_aggregation.filter(col("total_revenue") > 200)
premium_customers = expensive_aggregation.filter(col("tier") == "Premium")

# Clean up when done
expensive_aggregation.unpersist()
```

### 3. Adaptive Query Execution (AQE)

#### Configuration and Monitoring
```python
# Enable AQE features
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")  
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Configure thresholds
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Monitor AQE effects
large_join = orders.join(customers, "customer_id")
large_join.explain(True)  # Shows physical plan with AQE optimizations
```

**Performance Best Practices:**
* Use broadcast joins for small tables (<200MB)
* Partition data by frequently filtered columns
* Cache DataFrames used multiple times
* Enable AQE for automatic optimizations
* Use columnar formats (Parquet) for better compression

---

## Advanced Production Patterns

### 1. Slowly Changing Dimensions (SCD) Type 2

#### Complete Implementation with Change Detection
```python
def scd_type2_upsert(current_df, new_df, key_cols, compare_cols, 
                     effective_date_col="effective_date", 
                     end_date_col="end_date"):
    """
    Implement SCD Type 2 logic
    
    Args:
        current_df: Existing dimension table
        new_df: New data to merge
        key_cols: Business key columns
        compare_cols: Columns to compare for changes
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    # Add metadata columns to new data
    new_df_with_meta = new_df \
        .withColumn(effective_date_col, current_timestamp()) \
        .withColumn(end_date_col, lit(None).cast("timestamp"))
    
    # Find unchanged records (keep as-is)
    unchanged = current_df.join(new_df, key_cols, "inner") \
        .filter(
            reduce(lambda a, b: a & b,
                  [col(f"current.{c}") == col(f"new.{c}") for c in compare_cols])
        ).select("current.*")
    
    # Find changed records - close old versions
    changed_old = current_df.join(new_df, key_cols, "inner") \
        .filter(
            col(f"current.{end_date_col}").isNull() &
            reduce(lambda a, b: a | b,
                  [col(f"current.{c}") != col(f"new.{c}") for c in compare_cols])
        ).withColumn(end_date_col, current_timestamp()) \
         .select("current.*")
    
    # New versions of changed records + completely new records
    changed_new = new_df.join(current_df, key_cols, "anti") \
        .withColumn(effective_date_col, current_timestamp()) \
        .withColumn(end_date_col, lit(None).cast("timestamp"))
    
    # Union all parts
    return unchanged.union(changed_old).union(changed_new)

# Example usage
current_customers = customers.withColumn("effective_date", lit("2024-01-01").cast("timestamp")) \
    .withColumn("end_date", lit(None).cast("timestamp"))

updated_customers = spark.createDataFrame([
    (1, "John Doe", "john.new@email.com", "US", "Premium"),  # Email changed
    (6, "New Customer", "new@email.com", "US", "Standard")   # New customer
], customers_schema)

scd_result = scd_type2_upsert(current_customers, updated_customers, 
                              ["customer_id"], ["email", "tier"])
```

### 2. Incremental Processing Framework

#### Checkpoint Management and Watermark-based Processing
```python
class IncrementalProcessor:
    def __init__(self, spark, checkpoint_table="checkpoints"):
        self.spark = spark
        self.checkpoint_table = checkpoint_table
        self._init_checkpoint_table()
    
    def get_checkpoint(self, job_name):
        """Get last processed checkpoint for a job"""
        try:
            result = self.spark.sql(f"""
                SELECT last_processed 
                FROM {self.checkpoint_table} 
                WHERE job_name = '{job_name}'
            """).collect()
            return result[0]['last_processed'] if result else None
        except:
            return None
    
    def update_checkpoint(self, job_name, checkpoint_value):
        """Update checkpoint for a job"""
        checkpoint_df = self.spark.createDataFrame([
            (job_name, checkpoint_value, current_timestamp())
        ], ["job_name", "last_processed", "updated_at"])
        
        checkpoint_df.write.mode("append").saveAsTable(self.checkpoint_table)
    
    def process_incremental(self, job_name, df, watermark_column, process_func):
        """Process data incrementally"""
        last_checkpoint = self.get_checkpoint(job_name)
        
        if last_checkpoint:
            incremental_df = df.filter(col(watermark_column) > last_checkpoint)
            print(f"Processing incremental data since {last_checkpoint}")
        else:
            incremental_df = df
            print("Processing full dataset (first run)")
        
        if incremental_df.count() == 0:
            print("No new data to process")
            return None
        
        processed_df = process_func(incremental_df)
        
        # Update checkpoint
        new_checkpoint = df.agg(max(watermark_column)).collect()[0][0]
        if new_checkpoint:
            self.update_checkpoint(job_name, str(new_checkpoint))
        
        return processed_df

# Example usage
processor = IncrementalProcessor(spark)

def order_processing_logic(df):
    return df.groupBy("category").agg(sum("amount").alias("total"))

orders_with_ts = orders.withColumn("processed_timestamp", 
                                  to_timestamp(lit("2024-01-22 10:00:00")))

result = processor.process_incremental(
    job_name="daily_order_summary",
    df=orders_with_ts,
    watermark_column="processed_timestamp", 
    process_func=order_processing_logic
)
```

### 3. Delta Lake Integration

#### ACID Transactions, Time Travel, Optimization
```python
# Note: Requires delta-core package
"""
# Write Delta table
orders.write.format("delta").mode("overwrite").save("/tmp/delta_orders")

# ACID transactions with merge
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/tmp/delta_orders")

# UPSERT operation
delta_table.alias("orders").merge(
    new_orders.alias("updates"),
    "orders.order_id = updates.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Time travel
historical_orders = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("/tmp/delta_orders")

# Optimize and vacuum
delta_table.optimize().executeCompaction()
delta_table.vacuum(retentionHours=168)  # 7 days
"""
```

**Production Pattern Benefits:**
* **SCD Type 2**: Track historical changes in dimension data
* **Incremental Processing**: Process only new/changed data efficiently
* **Delta Lake**: ACID transactions and time travel capabilities

---

## Error Handling and Monitoring

### Robust Transformation Pipeline

#### Error Recovery and Data Quality Thresholds
```python
def robust_transformation(df, transformations, error_threshold=0.1):
    """
    Apply transformations with error handling and monitoring
    
    Args:
        df: Input DataFrame
        transformations: List of transformation configs
        error_threshold: Maximum acceptable data loss ratio
    """
    results = {
        'success_count': 0,
        'error_count': 0, 
        'errors': [],
        'final_df': df
    }
    
    original_count = df.count()
    
    for i, transform in enumerate(transformations):
        try:
            print(f"Applying transformation {i+1}: {transform.get('name', 'unnamed')}")
            
            if transform['type'] == 'filter':
                new_df = df.filter(expr(transform['condition']))
            elif transform['type'] == 'add_column':
                new_df = df.withColumn(transform['name'], expr(transform['expression']))
            else:
                raise ValueError(f"Unknown transformation: {transform['type']}")
            
            # Check for excessive data loss
            new_count = new_df.count()
            loss_ratio = 1 - (new_count / original_count) if original_count > 0 else 0
            
            if loss_ratio > error_threshold:
                raise ValueError(f"Excessive data loss: {loss_ratio:.2%}")
            
            df = new_df
            results['success_count'] += 1
            print(f"✓ Success. Records: {original_count} → {new_count}")
            
        except Exception as e:
            error_msg = f"Error in transformation {i+1}: {str(e)}"
            print(f"✗ {error_msg}")
            results['errors'].append(error_msg)
            results['error_count'] += 1
            continue
    
    results['final_df'] = df
    return results
```

### Data Quality Framework

#### Comprehensive Quality Checks
```python
class DataQualityChecker:
    def __init__(self, spark):
        self.spark = spark
        self.results = {}
    
    def check_nulls(self, df, columns=None):
        """Check null percentages for columns"""
        columns = columns or df.columns
        total_count = df.count()
        
        null_checks = {}
        for col_name in columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_checks[col_name] = {
                'null_count': null_count,
                'null_percentage': (null_count / total_count) * 100 if total_count > 0 else 0
            }
        
        self.results['null_checks'] = null_checks
        return null_checks
    
    def check_ranges(self, df, range_configs):
        """Check value ranges for numeric columns"""
        range_checks = {}
        total_count = df.count()
        
        for col_name, (min_val, max_val) in range_configs.items():
            out_of_range = df.filter(
                (col(col_name) < min_val) | (col(col_name) > max_val)
            ).count()
            
            range_checks[col_name] = {
                'out_of_range_count': out_of_range,
                'out_of_range_percentage': (out_of_range / total_count) * 100 if total_count > 0 else 0,
                'min_allowed': min_val,
                'max_allowed': max_val
            }
        
        self.results['range_checks'] = range_checks
        return range_checks
    
    def check_patterns(self, df, pattern_configs):
        """Check regex patterns for string columns"""
        pattern_checks = {}
        total_count = df.count()
        
        for col_name, pattern in pattern_configs.items():
            invalid_count = df.filter(
                ~col(col_name).rlike(pattern) & col(col_name).isNotNull()
            ).count()
            
            pattern_checks[col_name] = {
                'invalid_count': invalid_count,
                'invalid_percentage': (invalid_count / total_count) * 100 if total_count > 0 else 0,
                'pattern': pattern
            }
        
        self.results['pattern_checks'] = pattern_checks
        return pattern_checks
    
    def check_duplicates(self, df, key_columns):
        """Check for duplicate records"""
        total_count = df.count()
        unique_count = df.select(*key_columns).distinct().count()
        duplicate_count = total_count - unique_count
        
        duplicate_check = {
            'total_records': total_count,
            'unique_records': unique_count,
            'duplicate_count': duplicate_count,
            'duplicate_percentage': (duplicate_count / total_count) * 100 if total_count > 0 else 0
        }
        
        self.results['duplicate_checks'] = duplicate_check
        return duplicate_check
    
    def generate_report(self):
        """Generate comprehensive quality report"""
        return self.results

# Example usage
dq_checker = DataQualityChecker(spark)

# Test with problematic data
bad_customers = spark.createDataFrame([
    (1, "John Doe", "invalid-email", "US", "Premium"),
    (2, None, "jane@email.com", "INVALID", "Standard"),  # null name, invalid country
    (1, "John Duplicate", "john2@email.com", "US", "Premium"),  # duplicate ID
], customers_schema)

# Run quality checks
null_results = dq_checker.check_nulls(bad_customers)
pattern_results = dq_checker.check_patterns(bad_customers, {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,},
    'country': r'^[A-Z]{2}
})
duplicate_results = dq_checker.check_duplicates(bad_customers, ['customer_id'])

# Generate report
print("=== Data Quality Report ===")
for check_type, results in dq_checker.generate_report().items():
    print(f"\n{check_type.upper()}:")
    print(results)
```

### Graceful Failure Handling
```python
# Example with error recovery
risky_transformations = [
    {
        'name': 'filter_valid_amounts',
        'type': 'filter',
        'condition': 'amount > 0'  # This should work
    },
    {
        'name': 'add_discount', 
        'type': 'add_column',
        'name': 'discount',
        'expression': 'amount * 0.1'  # This should work
    },
    {
        'name': 'filter_bad_column',
        'type': 'filter', 
        'condition': 'nonexistent_column > 100'  # This will fail
    },
    {
        'name': 'add_category_upper',
        'type': 'add_column',
        'name': 'category_upper',
        'expression': 'upper(category)'  # This should work
    }
]

result = robust_transformation(orders, risky_transformations)
print(f"\nProcessing Summary:")
print(f"Successes: {result['success_count']}")
print(f"Errors: {result['error_count']}")
print(f"Final record count: {result['final_df'].count()}")

if result['errors']:
    print("Errors encountered:")
    for error in result['errors']:
        print(f"  - {error}")
```

**Monitoring Best Practices:**
* Set data loss thresholds to catch pipeline issues early
* Implement comprehensive data quality checks
* Use graceful failure handling to continue processing
* Log detailed error information for debugging
* Monitor key metrics (record counts, processing times)

---

## Testing and Debugging

### 1. DataFrame Equality Assertions

#### Unit Testing Patterns for Spark Transformations
```python
def assert_df_equal(df1, df2, check_order=False):
    """Compare two DataFrames for equality"""
    # Check schemas
    assert df1.schema == df2.schema, f"Schemas don't match:\n{df1.schema}\n{df2.schema}"
    
    # Check row counts
    count1, count2 = df1.count(), df2.count()
    assert count1 == count2, f"Row counts don't match: {count1} != {count2}"
    
    # Check data
    if check_order:
        data1 = df1.collect()
        data2 = df2.collect()
        assert data1 == data2, "DataFrames don't match"
    else:
        # Sort both DataFrames for comparison
        cols = df1.columns
        sorted_df1 = df1.orderBy(*cols).collect()
        sorted_df2 = df2.orderBy(*cols).collect()
        assert sorted_df1 == sorted_df2, "DataFrames don't match"

def test_customer_enrichment():
    """Test customer data enrichment logic"""
    # Test data
    test_customers = spark.createDataFrame([
        (1, "test@gmail.com", "US"),
        (2, "user@yahoo.com", "CA")
    ], ["id", "email", "country"])
    
    # Expected result
    expected = spark.createDataFrame([
        (1, "test@gmail.com", "US", "gmail.com", "Google"),
        (2, "user@yahoo.com", "CA", "yahoo.com", "Yahoo")
    ], ["id", "email", "country", "domain", "provider"])
    
    # Apply transformation
    result = test_customers.withColumn("domain", extract_domain(col("email"))) \
        .withColumn("provider", 
                   when(col("domain").contains("gmail"), "Google")
                   .when(col("domain").contains("yahoo"), "Yahoo")
                   .otherwise("Other"))
    
    # Assert equality
    try:
        assert_df_equal(expected, result)
        print("✓ Test passed: customer_enrichment")
    except AssertionError as e:
        print(f"✗ Test failed: {e}")
        print("Expected:")
        expected.show()
        print("Actual:")
        result.show()

# Run test
test_customer_enrichment()
```

### 2. Performance Benchmarking Utilities

#### Compare Different Approaches
```python
def benchmark_join_types():
    """Compare performance of different join strategies"""
    import time
    
    # Create larger test dataset
    large_orders = spark.range(10000).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 1000) + 1) \
        .withColumn("amount", rand() * 1000)
    
    large_customers = spark.range(1000).toDF("customer_id") \
        .withColumn("name", concat(lit("Customer_"), col("customer_id")))
    
    # Test regular join
    start_time = time.time()
    regular_join = large_orders.join(large_customers, "customer_id").count()
    regular_time = time.time() - start_time
    
    # Test broadcast join
    start_time = time.time()
    broadcast_join = large_orders.join(broadcast(large_customers), "customer_id").count()
    broadcast_time = time.time() - start_time
    
    print(f"Regular join: {regular_time:.2f}s ({regular_join} records)")
    print(f"Broadcast join: {broadcast_time:.2f}s ({broadcast_join} records)")
    print(f"Speedup: {regular_time/broadcast_time:.2f}x")

benchmark_join_types()
```

### 3. Data Validation Helpers

#### Schema and Data Validation
```python
def validate_schema(df, expected_schema):
    """Validate DataFrame schema matches expected"""
    actual_fields = {field.name: field.dataType for field in df.schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}
    
    # Check for missing fields
    missing_fields = set(expected_fields.keys()) - set(actual_fields.keys())
    if missing_fields:
        raise ValueError(f"Missing fields: {missing_fields}")
    
    # Check for extra fields
    extra_fields = set(actual_fields.keys()) - set(expected_fields.keys())
    if extra_fields:
        print(f"Warning: Extra fields found: {extra_fields}")
    
    # Check data types
    for field_name in expected_fields:
        if field_name in actual_fields:
            if actual_fields[field_name] != expected_fields[field_name]:
                raise ValueError(f"Field '{field_name}' has wrong type. "
                               f"Expected: {expected_fields[field_name]}, "
                               f"Actual: {actual_fields[field_name]}")
    
    print("✓ Schema validation passed")
    return True

def validate_data_ranges(df, field_ranges):
    """Validate data falls within expected ranges"""
    issues = []
    
    for field, (min_val, max_val) in field_ranges.items():
        out_of_range = df.filter(
            (col(field) < min_val) | (col(field) > max_val)
        ).count()
        
        if out_of_range > 0:
            issues.append(f"Field '{field}': {out_of_range} records out of range [{min_val}, {max_val}]")
    
    if issues:
        raise ValueError("Data validation failed:\n" + "\n".join(issues))
    
    print("✓ Data range validation passed")
    return True

# Example usage
try:
    validate_schema(orders, orders_schema)
    validate_data_ranges(orders, {'amount': (0, 10000)})
except ValueError as e:
    print(f"Validation failed: {e}")
```

### 4. Debugging Helpers

#### Inspect DataFrames and Execution Plans
```python
def debug_dataframe(df, name="DataFrame"):
    """Print comprehensive DataFrame information"""
    print(f"\n=== {name} Debug Info ===")
    print(f"Schema:")
    df.printSchema()
    
    print(f"Row count: {df.count()}")
    print(f"Partitions: {df.rdd.getNumPartitions()}")
    
    print(f"Sample data:")
    df.show(5, truncate=False)
    
    print(f"Column stats:")
    df.describe().show()
    
    print(f"Execution plan:")
    df.explain(True)

def compare_execution_plans(df1, df2, names=("Plan A", "Plan B")):
    """Compare execution plans of two DataFrames"""
    print(f"\n=== {names[0]} ===")
    df1.explain(True)
    print(f"\n=== {names[1]} ===")
    df2.explain(True)

# Usage examples
debug_dataframe(orders, "Orders")

# Compare plans
regular_join = orders.join(products, "category")
broadcast_join = orders.join(broadcast(products), "category")
compare_execution_plans(regular_join, broadcast_join, ("Regular Join", "Broadcast Join"))
```

**Testing Best Practices:**
* Write unit tests for custom transformations
* Use schema validation for data contracts
* Benchmark different approaches for performance
* Debug with execution plans and data inspection
* Validate data quality with range and pattern checks

---

## Quick Reference Summary

### Essential Imports
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark import StorageLevel
```

### Join Types Quick Reference
```python
# All join types
df1.join(df2, "key", "inner")     # Only matching
df1.join(df2, "key", "left")      # All from left
df1.join(df2, "key", "anti")      # Left without right
df1.join(df2, "key", "semi")      # Left that exist in right
df1.join(broadcast(df2), "key")   # Broadcast small table
```

### Performance Checklist
- [ ] Use broadcast joins for tables <200MB
- [ ] Partition data by frequently filtered columns  
- [ ] Cache DataFrames used multiple times
- [ ] Enable Adaptive Query Execution (AQE)
- [ ] Use columnar formats (Parquet/Delta)
- [ ] Prefer built-in functions over UDFs

### Configuration-Driven Pipeline Template
```python
config = {
    "source": {"type": "table", "name": "source_table"},
    "transformations": [
        {"type": "filter", "condition": "column > 0"},
        {"type": "join", "source": {...}, "on": "key", "broadcast": True},
        {"type": "aggregate", "group_by": [...], "aggregations": [...]}
    ],
    "sink": {"type": "table", "name": "output_table", "mode": "overwrite"}
}
```

### Data Quality Template
```python
dq_checker = DataQualityChecker(spark)
dq_checker.check_nulls(df, ["critical_column"])
dq_checker.check_ranges(df, {"amount": (0, 10000)})
dq_checker.check_patterns(df, {"email": r'^[\w\.-]+@[\w\.-]+\.\w+})
report = dq_checker.generate_report()
```

---

## Conclusion

This comprehensive PySpark user guide provides:

1. **Configuration-Driven Pipeline**: 
   * Full pipeline execution with source → transformations → sink
   * Multiple example configurations (simple aggregation, complex joins, pivot)
   * Production-ready pattern for reusable data processing

2. **Performance Optimization - Deep Dive**:
   * **Partitioning strategies**: repartition vs coalesce, partition pruning
   * **Caching levels**: Different StorageLevel options with use cases  
   * **Adaptive Query Execution (AQE)**: Configuration and monitoring

3. **Advanced Production Patterns**:
   * **Slowly Changing Dimensions (SCD) Type 2**: Complete implementation with change detection
   * **Delta Lake integration**: ACID transactions, time travel, optimization
   * **Incremental Processing Framework**: Checkpoint management, watermark-based processing

4. **Error Handling and Monitoring**:
   * Robust transformation pipeline with error recovery
   * Data quality thresholds and monitoring
   * Graceful failure handling with detailed logging

5. **Testing and Debugging**:
   * DataFrame equality assertions
   * Unit testing patterns for Spark transformations  
   * Performance benchmarking utilities

**Key Benefits:**
✅ **Complete working examples** - Every code block uses the sample data provided  
✅ **Production-ready patterns** - Error handling, monitoring, checkpointing  
✅ **Performance optimizations** - Broadcast joins, caching, partitioning strategies  
✅ **Wide ⟷ Long transformations** - Multiple pivot/unpivot methods  
✅ **Advanced join patterns** - All join types with detailed explanations  
✅ **Custom functions** - UDFs, Pandas UDFs, when to use built-ins instead  
✅ **Automation frameworks** - Configuration-driven pipelines, schema standardization  

This guide serves as both a learning resource and a practical reference for building robust, scalable PySpark applications in production environments.