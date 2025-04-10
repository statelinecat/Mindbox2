from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import subprocess
import findspark

# Настройка Java
java_home = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.26.4-hotspot"
java_path = os.path.join(java_home, "bin", "java.exe")

# Проверка доступности Java
try:
    subprocess.run([java_path, "-version"], check=True, capture_output=True)
except Exception as e:
    print(f"Ошибка доступа к Java: {e}")
    raise

os.environ["JAVA_HOME"] = java_home
os.environ["PATH"] = f"{os.path.join(java_home, 'bin')};{os.environ['PATH']}"

findspark.init()

def get_product_category_pairs(products_df, categories_df, product_category_df):
    product_with_category = products_df.join(
        product_category_df,
        on="product_id",
        how="left"
    ).join(
        categories_df,
        on="category_id",
        how="left"
    )

    result = product_with_category.select(
        col("product_name"),
        col("category_name")
    ).na.fill("Нет категории", ["category_name"])

    products_without_categories = product_with_category.filter(
        col("category_name").isNull()
    ).select(
        col("product_name")
    ).distinct()

    return result, products_without_categories

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ProductCategory") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    # Тестовые данные
    products = spark.createDataFrame([
        (1, "Product A"), (2, "Product B"), (3, "Product C")
    ], ["product_id", "product_name"])

    categories = spark.createDataFrame([
        (101, "Category X"), (102, "Category Y"), (103, "Category Z")
    ], ["category_id", "category_name"])

    product_category = spark.createDataFrame([
        (1, 101), (1, 102), (1, 103), (2, 101)
    ], ["product_id", "category_id"])

    pairs_df, no_category_df = get_product_category_pairs(products, categories, product_category)

    print("Пары «Продукт – Категория»:")
    pairs_df.show(truncate=False)

    print("\nПродукты без категорий:")
    no_category_df.show(truncate=False)

    spark.stop()