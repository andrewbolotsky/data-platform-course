## Практическое задание №4: Apache Spark под управлением YARN

### Используемые компоненты
- **YARN / HDFS / Hive** — кластер из предыдущих заданий;
- **Spark 3.3.4** — установлен на все узлы кластера и настроен на работу с YARN и Hive;
- **PySpark** — логика обработки данных.

## Как именно выполняются пункты задания

### 1. Запуск сессии Spark под управлением YARN
- Сессия создается в скрипте `scripts/spark-data-processing.py` через `SparkSession.builder.enableHiveSupport()`.
- Запуск выполняется командой:

```bash
./scripts/run-spark-job.sh
```

- Скрипт внутри вызывает `spark-submit --master yarn --deploy-mode client`, что явно запускает приложение Spark под управлением YARN.

### 2. Подключение к HDFS и чтение данных
- В `spark-data-processing.py` данные читаются из HDFS по пути:
  - `hdfs://nn:9000/user/spark/input/sales_data.csv`
- Используется явная схема и опции `header = true`, `inferSchema = false`.

### 3. Трансформации данных
В скрипте выполняются:
- приведение поля `sale_date` к дате;
- добавление вычисляемого поля `total_sale_amount = price * quantity`;
- добавление категориального признака `price_category` (Cheap/Medium/Expensive);
- три вида агрегаций:
  - по категории (`sales_category_stats`);
  - по региону (`sales_region_stats`);
  - по паре (категория, регион) (`sales_category_region_stats`).

Результаты промежуточных агрегаций выводятся в консоль (и в лог `/tmp/spark-job-output.log`).

### 4. Применение партиционирования при сохранении данных
- Основной набор данных (`df_transformed`) сохраняется в HDFS по пути:
  - `hdfs://nn:9000/user/spark/output/sales_transformed`
- Перед сохранением данные репартиционируются по колонкам `category`, `region`, затем записываются в формате JSON с использованием `partitionBy("category", "region")`.
- В результате в HDFS создаются подкаталоги вида:
  - `.../sales_transformed/category=Electronics/region=North/` и т.п.

### 5. Сохранение преобразованных данных как таблиц Hive
- Преобразованные данные сохраняются в HDFS и регистрируются в Hive как внешние таблицы (EXTERNAL), напрямую ссылающиеся на созданные каталоги:
  - `sales_transformed` (партиционированная по `category`, `region`, указывает на `.../output/sales_transformed`);
  - `sales_category_stats` (указывает на `.../output/sales_transformed_category_stats`);
  - `sales_region_stats` (указывает на `.../output/sales_transformed_region_stats`).

### 6. Проверка чтения данных стандартным клиентом Hive
После регистрации таблиц в Hive их можно прочитать любым стандартным клиентом Hive (HiveServer2/JDBC), например:

```sql
SELECT * FROM sales_transformed LIMIT 10;
```

## Структура проекта
- `deploy-spark.yml` — Ansible playbook для установки и настройки Spark на всех узлах.
- `vars.yml` — общие переменные (пути, имена хостов, ресурсы Spark).
- `scripts/run-spark-job.sh` — сценарий запуска:
  - проверяет наличие Spark и приложения;
  - исполняет `spark-submit` на YARN;
  - выводит ключевые шаги работы приложения и путь к логам.
- `scripts/spark-data-processing.py` — код PySpark-приложения:
  - создает SparkSession с поддержкой Hive;
  - читает CSV из HDFS;
  - выполняет трансформации и агрегации;
  - сохраняет результаты с партиционированием в HDFS.
- `templates/` — шаблоны конфигураций Spark (`spark-defaults.conf.j2`, `spark-env.sh.j2`).

## Веб-интерфейсы

- **YARN Resource Manager UI**  
  - URL: `http://nn:8088`  
  - Позволяет увидеть приложение `SparkDataProcessing`, его статус и использование ресурсов.

- **Spark History Server** (если развернут в среде)  
  - URL по умолчанию: `http://team-11-jn:18080`  
  - Показывает детали выполнения задачи (DAG, стадии, задачи).
