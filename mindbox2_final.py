from pyspark.sql import SparkSession
import os
import sys


def main():
    try:
        # 1. Жесткая настройка окружения
        os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.26.4-hotspot"
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        # 2. Минимальная конфигурация Spark
        spark = SparkSession.builder \
            .appName("SimpleProductAnalysis") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.default.parallelism", "1") \
            .master("local[1]") \
            .getOrCreate()

        # 3. Простейший тест без сложных операций
        print("\nПроверка работы Spark:")
        test_data = [("Test", 1), ("Check", 2)]
        df = spark.createDataFrame(test_data, ["name", "value"])
        df.show()

        # 4. Если простой тест работает, пробуем основную логику
        print("\nЗапуск основной логики:")
        products = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
        categories = spark.createDataFrame([(101, "X")], ["id", "name"])
        relations = spark.createDataFrame([(1, 101)], ["product_id", "category_id"])

        result = products.join(relations, products.id == relations.product_id, "left") \
            .join(categories, relations.category_id == categories.id, "left") \
            .select(products.name, categories.name.alias("category"))

        print("\nРезультат:")
        result.show()

    except Exception as e:
        print(f"\nФАТАЛЬНАЯ ОШИБКА: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        if 'spark' in locals():
            spark.stop()
        print("\nПриложение завершено")


if __name__ == "__main__":
    main()