# Установка необходимых компонентов
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

# Инициализация Spark
spark = SparkSession.builder \
    .appName("ProductCategoryAnalysis") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .master("local[2]") \
    .getOrCreate()

def prepare_data():
    """Создаем тестовые данные и преобразуем в Spark DataFrame"""
    products_pd = pd.DataFrame({
        'product_id': [1, 2, 3, 4],
        'product_name': ['Product A', 'Product B', 'Product C', 'Product D']
    })

    categories_pd = pd.DataFrame({
        'category_id': [101, 102, 103],
        'category_name': ['Category X', 'Category Y', 'Category Z']
    })

    relations_pd = pd.DataFrame({
        'product_id': [1, 1, 1, 2],
        'category_id': [101, 102, 103, 101]
    })

    # Конвертация в Spark DataFrame
    products = spark.createDataFrame(products_pd)
    categories = spark.createDataFrame(categories_pd)
    relations = spark.createDataFrame(relations_pd)

    return products, categories, relations

def get_product_category_pairs(products, categories, relations):
    """
    Возвращает все пары «Имя продукта – Имя категории» в одном датафрейме,
    включая продукты без категорий
    """
    result = (
        products.join(relations, "product_id", "left")
        .join(categories, "category_id", "left")
        .select(
            col("product_name"),
            col("category_name").alias("category")
        )
        .fillna("No category", subset=["category"])
    )
    return result

def main():
    try:
        print("=" * 50)
        print("Начало обработки данных")
        print("=" * 50)

        # 1. Подготовка данных
        products, categories, relations = prepare_data()

        print("\nТестовые данные:")
        print("Продукты:")
        products.show()
        print("\nКатегории:")
        categories.show()
        print("\nСвязи:")
        relations.show()

        # 2. Получение результатов в одном датафрейме
        result_df = get_product_category_pairs(products, categories, relations)

        # 3. Вывод результатов
        print("\n" + "=" * 50)
        print("Все пары «Продукт - Категория» и продукты без категорий")
        print("=" * 50)
        result_df.show(truncate=False)

        # 4. Дополнительная аналитика
        print("\n" + "=" * 50)
        print("Дополнительная статистика")
        print("=" * 50)

        print(f"\nВсего продуктов: {products.count()}")
        print(f"Уникальных пар продукт-категория: {result_df.filter(col('category') != 'No category').count()}")
        print(f"Продуктов без категорий: {result_df.filter(col('category') == 'No category').select('product_name').distinct().count()}")

    except Exception as e:
        print(f"\nОшибка: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()
        print("\n" + "=" * 50)
        print("Обработка завершена")
        print("=" * 50)

if __name__ == "__main__":
    main()