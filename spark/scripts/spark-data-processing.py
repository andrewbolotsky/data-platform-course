#!/usr/bin/env python3
"""
Spark приложение для чтения, трансформации и записи данных
Запуск: spark-submit --master yarn spark-data-processing.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, when, round as spark_round, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import sys

def create_spark_session():
    """Создает Spark Session с настройками для YARN и Hive"""
    print("=== Создание Spark Session ===", flush=True)
    print("Настройка Spark Session...", flush=True)
    
    # Не указываем master() явно, чтобы можно было передавать через spark-submit
    spark = SparkSession.builder \
        .appName("SparkDataProcessing") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("Spark Session создана успешно", flush=True)
    print(f"Spark Version: {spark.version}", flush=True)
    print(f"Spark Context: {spark.sparkContext}", flush=True)
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_data_from_hdfs(spark, input_path):
    """Читает данные из HDFS"""
    print(f"=== Чтение данных из HDFS: {input_path} ===")
    
    # Определяем схему данных
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("sale_date", StringType(), True),
        StructField("region", StringType(), True)
    ])
    
    # Читаем CSV файл
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(input_path)
    
    print(f"Загружено строк: {df.count()}")
    print("Первые 5 строк:")
    df.show(5, truncate=False)
    
    return df

def transform_data(df):
    """Применяет трансформации к данным"""
    print("\n=== Применение трансформаций данных ===")
    
    # 1. Преобразование типов: преобразуем sale_date из строки в дату
    df_transformed = df.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
    
    # 2. Добавляем вычисляемое поле: общая стоимость продажи
    df_transformed = df_transformed.withColumn(
        "total_sale_amount", 
        col("price") * col("quantity")
    )
    
    # 3. Добавляем категорию цены (дорогой/средний/дешевый)
    df_transformed = df_transformed.withColumn(
        "price_category",
        when(col("price") >= 500, "Expensive")
        .when(col("price") >= 100, "Medium")
        .otherwise("Cheap")
    )
    
    # 4. Агрегация: статистика по категориям
    print("\n--- Агрегация по категориям ---")
    category_stats = df_transformed.groupBy("category") \
        .agg(
            count("*").alias("product_count"),
            spark_sum("quantity").alias("total_quantity"),
            spark_round(avg("price"), 2).alias("avg_price"),
            spark_round(spark_sum("total_sale_amount"), 2).alias("total_revenue")
        ) \
        .orderBy("category")
    
    category_stats.show(truncate=False)
    
    # 5. Агрегация: статистика по регионам
    print("\n--- Агрегация по регионам ---")
    region_stats = df_transformed.groupBy("region") \
        .agg(
            count("*").alias("sale_count"),
            spark_round(spark_sum("total_sale_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_sale_amount"), 2).alias("avg_sale_amount")
        ) \
        .orderBy("region")
    
    region_stats.show(truncate=False)
    
    # 6. Агрегация: статистика по категориям и регионам
    print("\n--- Агрегация по категориям и регионам ---")
    category_region_stats = df_transformed.groupBy("category", "region") \
        .agg(
            count("*").alias("sale_count"),
            spark_round(spark_sum("total_sale_amount"), 2).alias("total_revenue")
        ) \
        .orderBy("category", "region")
    
    category_region_stats.show(truncate=False)
    
    return df_transformed, category_stats, region_stats, category_region_stats

def save_with_partitioning(df, output_path, partition_cols):
    """Сохраняет данные с партиционированием"""
    print(f"\n=== Сохранение данных с партиционированием по {partition_cols} ===")
    
    # Репартиционируем данные перед сохранением
    # Используем coalesce для уменьшения количества партиций или repartition для увеличения
    df_partitioned = df.repartition(*partition_cols)
    
    print(f"Количество партиций перед сохранением: {df_partitioned.rdd.getNumPartitions()}")
    
    # Сохраняем в JSON формате с партиционированием
    df_partitioned.write \
        .mode("overwrite") \
        .partitionBy(*partition_cols) \
        .json(output_path)
    
    print(f"Данные сохранены в: {output_path}")

def save_as_hive_table(spark, df, table_name, partition_cols=None):
    """Сохраняет данные как таблицу Hive"""
    print(f"\n=== Сохранение данных как таблицы Hive: {table_name} ===")
    
    # Создаем временное представление
    temp_view = f"{table_name}_temp_view"
    df.createOrReplaceTempView(temp_view)
    
    # Сохраняем как таблицу Hive
    if partition_cols:
        # Сохраняем с партиционированием
        df.write \
            .mode("overwrite") \
            .format("json") \
            .partitionBy(*partition_cols) \
            .saveAsTable(table_name)
    else:
        # Сохраняем без партиционирования
        df.write \
            .mode("overwrite") \
            .format("json") \
            .saveAsTable(table_name)
    
    print(f"Таблица {table_name} создана в Hive")
    
    # Проверяем таблицу
    print(f"\nПроверка таблицы {table_name}:")
    spark.sql(f"SELECT COUNT(*) as total_rows FROM {table_name}").show()
    spark.sql(f"DESCRIBE {table_name}").show(truncate=False)
    spark.sql(f"SELECT * FROM {table_name} LIMIT 5").show(truncate=False)

def main():
    import sys
    sys.stdout.flush()
    sys.stderr.flush()

    input_path = "hdfs://nn:9000/user/spark/input/sales_data.csv"
    output_path = "hdfs://nn:9000/user/spark/output/sales_transformed"
    
    print("=== Начало выполнения Spark приложения ===", flush=True)
    try:
        spark = create_spark_session()
        print("=== Spark Session создана ===", flush=True)
        
        df = read_data_from_hdfs(spark, input_path)
        
        df_transformed, category_stats, region_stats, category_region_stats = transform_data(df)
        
        # Сохраняем с партиционированием
        save_with_partitioning(
            df_transformed, 
            output_path, 
            partition_cols=["category", "region"]
        )
        
        # Сохраняем статистику как JSON файлы для последующего создания таблиц через Beeline
        # (так как saveAsTable не работает из-за конфликта версий Java/Hive)
        print("\n=== Сохранение статистики в JSON ===")
        
        category_stats_path = f"{output_path}_category_stats"
        category_stats.write.mode("overwrite").json(category_stats_path)
        print(f"Статистика по категориям сохранена в: {category_stats_path}")
        
        region_stats_path = f"{output_path}_region_stats"
        region_stats.write.mode("overwrite").json(region_stats_path)
        print(f"Статистика по регионам сохранена в: {region_stats_path}")
        
        category_region_stats_path = f"{output_path}_category_region_stats"
        category_region_stats.write.mode("overwrite").partitionBy("category").json(category_region_stats_path)
        print(f"Статистика по категориям и регионам сохранена в: {category_region_stats_path}")
        
        print("\n=== Обработка данных завершена успешно ===")
        
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()

