from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import sys
import subprocess
import findspark


def configure_environment():
    """Настройка окружения Java и Spark"""
    # Установка путей к Java (проверьте актуальность пути)
    java_home = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.26.4-hotspot"
    java_bin = os.path.join(java_home, "bin")

    # Проверка доступности Java
    try:
        java_path = os.path.join(java_bin, "java.exe")
        subprocess.run([java_path, "-version"],
                       check=True,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    except Exception as e:
        print(f"Ошибка доступа к Java: {e}")
        sys.exit(1)

    # Настройка переменных окружения
    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = f"{java_bin};{os.environ.get('PATH', '')}"

    # Инициализация Spark
    findspark.init()


def create_spark_session():
    """Создание и настройка Spark сессии"""
    return SparkSession.builder \
        .appName("ProductCategoryAnalysis") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[2]") \
        .getOrCreate()


def prepare_test_data(spark):
    """Подготовка тестовых данных"""
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

    return products, categories, product_category


def analyze_product_categories(products_df, categories_df, relations_df):
    """Анализ связей продуктов и категорий"""
    # Объединение данных
    product_with_category = products_df.join(
        relations_df,
        on="product_id",
        how="left"
    ).join(
        categories_df,
        on="category_id",
        how="left"
    )

    # Все пары продукт-категория
    pairs = product_with_category.select(
        col("product_name"),
        col("category_name")
    ).na.fill("No category", ["category_name"])

    # Продукты без категорий
    no_category = product_with_category.filter(
        col("category_name").isNull()
    ).select(
        col("product_name")
    ).distinct()

    return pairs, no_category


def main():
    """Основная функция выполнения"""
    try:
        # 1. Настройка окружения
        configure_environment()

        # 2. Создание Spark сессии
        spark = create_spark_session()

        # 3. Подготовка данных
        products, categories, product_category = prepare_test_data(spark)

        # 4. Анализ данных
        pairs_df, no_category_df = analyze_product_categories(
            products, categories, product_category
        )

        # 5. Вывод результатов
        print("\nРезультаты анализа:")
        print("=" * 50)
        print("Пары «Продукт - Категория»:")
        pairs_df.show(truncate=False, n=100)

        print("\nПродукты без категорий:")
        no_category_df.show(truncate=False, n=100)

    except Exception as e:
        print(f"\nОшибка при выполнении: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        # 6. Завершение работы Spark
        if 'spark' in locals():
            spark.stop()
        print("\nЗавершение работы приложения")


if __name__ == "__main__":
    main()