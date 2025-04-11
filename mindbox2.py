# Установка необходимых компонентов
!apt - get
install
openjdk - 11 - jdk - headless - qq > / dev / null
!pip
install
pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pandas as pd

# Инициализация Spark в Colab
spark = SparkSession.builder \
    .appName("ProductCategoryAnalysis") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .master("local[2]") \
    .getOrCreate()


# Подготовка тестовых данных через Pandas DataFrame (для наглядности)
def prepare_data():
    """Создаем тестовые данные и преобразуем в Spark DataFrame"""
    products_pd = pd.DataFrame({
        'product_id': [1, 2, 3],
        'product_name': ['Product A', 'Product B', 'Product C']
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


# Анализ данных с улучшенной обработкой
def analyze_data(products, categories, relations):
    """Анализ связей с более читаемым кодом"""
    # Объединяем данные за один шаг
    result = (products
              .join(relations, "product_id", "left")
              .join(categories, "category_id", "left")
              .select(
        col("product_name"),
        col("category_name").alias("category")
    )
              .fillna("No category", subset=["category"]))

    # Разделяем результаты
    products_with_categories = result.filter(col("category") != "No category")
    products_without_categories = result.filter(col("category") == "No category").select("product_name").distinct()

    return products_with_categories, products_without_categories


# Главная функция выполнения
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

        # 2. Анализ данных
        with_cats, without_cats = analyze_data(products, categories, relations)

        # 3. Вывод результатов
        print("\n" + "=" * 50)
        print("Результаты анализа")
        print("=" * 50)

        print("\nПары «Продукт - Категория»:")
        with_cats.show(truncate=False)

        print("\nПродукты без категорий:")
        without_cats.show(truncate=False)

        # 4. Дополнительная аналитика
        print("\n" + "=" * 50)
        print("Дополнительная статистика")
        print("=" * 50)

        print(f"\nВсего продуктов: {products.count()}")
        print(f"Продуктов с категориями: {with_cats.select('product_name').distinct().count()}")
        print(f"Продуктов без категорий: {without_cats.count()}")

    except Exception as e:
        print(f"\nОшибка: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()
        print("\n" + "=" * 50)
        print("Обработка завершена")
        print("=" * 50)


# Запуск анализа
if __name__ == "__main__":
    main()
