#!/usr/bin/env python3
"""
Test script to verify Python and Spark setup in container
"""

import sys
import os
from datetime import datetime

def test_python_basics():
    """Test basic Python functionality"""
    print("=" * 50)
    print("TESTING PYTHON BASICS")
    print("=" * 50)
    
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    
    # Test basic operations
    test_list = [1, 2, 3, 4, 5]
    test_sum = sum(test_list)
    print(f"Basic math test: sum({test_list}) = {test_sum}")
    
    # Test imports of common libraries
    libraries_to_test = ['json', 'csv', 'datetime', 'os', 'sys']
    
    for lib in libraries_to_test:
        try:
            __import__(lib)
            print(f"✓ {lib} import: OK")
        except ImportError as e:
            print(f"✗ {lib} import: FAILED - {e}")
    
    print("Python basics test completed!\n")

def test_spark_setup():
    """Test Spark installation and basic functionality"""
    print("=" * 50)
    print("TESTING SPARK SETUP")
    print("=" * 50)
    
    # Check environment variables
    spark_home = os.getenv('SPARK_HOME')
    java_home = os.getenv('JAVA_HOME')
    
    print(f"SPARK_HOME: {spark_home}")
    print(f"JAVA_HOME: {java_home}")
    
    # Test PySpark import
    try:
        import pyspark
        print(f"✓ PySpark version: {pyspark.__version__}")
        
        # Test SparkContext creation
        from pyspark.sql import SparkSession
        
        print("Creating Spark session...")
        spark = SparkSession.builder \
            .appName("ContainerTest") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        print(f"✓ Spark session created successfully!")
        print(f"✓ Spark version: {spark.version}")
        print(f"✓ Spark master: {spark.sparkContext.master}")
        
        # Test basic Spark operations
        print("\nTesting basic Spark operations...")
        
        # Create a simple DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        df = spark.createDataFrame(data, columns)
        
        print("Sample DataFrame:")
        df.show()
        
        print("DataFrame count:", df.count())
        
        # Test SQL query
        df.createOrReplaceTempView("people")
        result = spark.sql("SELECT name, age FROM people WHERE age > 28")
        print("SQL query result (age > 28):")
        result.show()
        
        # Test RDD operations
        rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
        squares = rdd.map(lambda x: x * x).collect()
        print(f"RDD test - squares: {squares}")
        
        spark.stop()
        print("✓ Spark session stopped successfully!")
        
    except ImportError as e:
        print(f"✗ PySpark import failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Spark test failed: {e}")
        return False
    
    print("Spark setup test completed!\n")
    return True

def test_data_libraries():
    """Test common data processing libraries"""
    print("=" * 50)
    print("TESTING DATA PROCESSING LIBRARIES")
    print("=" * 50)
    
    libraries = {
        'pandas': 'pandas',
        'numpy': 'numpy', 
        'matplotlib': 'matplotlib',
        'seaborn': 'seaborn',
        'sklearn': 'scikit-learn'
    }
    
    for import_name, display_name in libraries.items():
        try:
            lib = __import__(import_name)
            version = getattr(lib, '__version__', 'unknown')
            print(f"✓ {display_name}: {version}")
        except ImportError:
            print(f"✗ {display_name}: Not installed")
    
    print("Data libraries test completed!\n")

def main():
    """Run all tests"""
    print(f"Container Test Script - {datetime.now()}")
    print("Testing Python and Spark setup in container\n")
    
    try:
        test_python_basics()
        test_data_libraries()
        spark_ok = test_spark_setup()
        
        print("=" * 50)
        print("SUMMARY")
        print("=" * 50)
        print("✓ Python: Working")
        print(f"{'✓' if spark_ok else '✗'} Spark: {'Working' if spark_ok else 'Issues detected'}")
        
        if spark_ok:
            print("\n🎉 Container is properly set up for Python and Spark!")
        else:
            print("\n⚠️  Container has issues with Spark setup")
            
    except Exception as e:
        print(f"Test script failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

