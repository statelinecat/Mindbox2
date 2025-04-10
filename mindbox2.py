from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import findspark
findspark.init()


def get_product_category_pairs(products_df, categories_df, product_category_df):
    # 1. Соединяем продукты и связи, затем добавляем категории (LEFT JOIN)
    product_with_category = products_df.join(
        product_category_df,
        on="product_id",
        how="left"
    ).join(
        categories_df,
        on="category_id",
        how="left"
    )

    # 2. Выбираем нужные колонки (имена продуктов и категорий)
    result = product_with_category.select(
        col("product_name"),
        col("category_name")
    )

    # 3. Добавляем продукты без категорий (где category_name == NULL)
    products_without_categories = product_with_category.filter(
        col("category_name").isNull()
    ).select(
        col("product_name")
    ).distinct()  # на случай дубликатов

    return result, products_without_categories


# Пример использования:
spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

# Создаем тестовые датафреймы
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
    (1, 102),  # У Product A три категории
    (1, 103),
    (2, 101),  # У Product B одна категория
    # Product C не имеет категорий
], ["product_id", "category_id"])

# Вызываем метод
pairs_df, no_category_df = get_product_category_pairs(products, categories, product_category)

print("Пары «Продукт – Категория»:")
pairs_df.show()

print("Продукты без категорий:")
no_category_df.show()