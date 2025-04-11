from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import sys


def main():
    try:
        # 1. Настройка окружения
        os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.26.4-hotspot"
        os.environ["PYSPARK_PYTHON"] = sys.executable

        # 2. Создание SparkSession с минимальной конфигурацией
        spark = SparkSession.builder \
            .appName("ProductCategory") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.sql.shuffle.partitions", "1") \
            .master("local[1]") \
            .getOrCreate()

        # 3. Подготовка тестовых данных
        products = spark.createDataFrame([
            (1, "Product A"),
            (2, "Product B"),
            (3, "Product C")
        ], ["product_id", "product_name"])

        categories = spark.createDataFrame([
            (101, "Category X"),
            (102, "Category Y"),
            (103, "Category Z")
        ], ["category_id", "category_name"])

        product_category = spark.createDataFrame([
            (1, 101),
            (1, 102),
            (1, 103),
            (2, 101)
        ], ["product_id", "category_id"])

        # 4. Анализ данных (упрощенная версия)
        result = products.join(
            product_category,
            "product_id",
            "left"
        ).join(
            categories,
            "category_id",
            "left"
        ).select(
            "product_name",
            "category_name"
        )

        # 5. Вывод результатов через collect()
        print("\nРезультаты анализа:")
        print("=" * 50)
        print("Пары «Продукт - Категория»:")
        for row in result.collect():
            print(row.product_name, "-", row.category_name or "No category")

    except Exception as e:
        print(f"\nОшибка: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        if 'spark' in locals():
            spark.stop()
        print("\nЗавершение работы")


if __name__ == "__main__":
    main()