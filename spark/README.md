# Практическое задание №4: Apache Spark на YARN

## Описание решения
Реализована автоматизированная обработка данных с использованием Apache Spark под управлением YARN. Решение включает чтение данных из HDFS, трансформацию (агрегацию), партиционированное сохранение и регистрацию таблиц в Hive.

### Архитектура решения
Из-за несовместимости библиотек Hive (версии 2.3/3.1) с Java 11 (ClassCastException) и конфликтов версий Parquet между Spark 3.x и Hadoop 3.3.6, применена гибридная архитектура:

1.  **Processing (Spark 3.3.4):**
    *   Запускается на YARN в режиме `client`.
    *   Выполняет чтение CSV, очистку и агрегацию данных.
    *   Сохраняет результаты в формате **JSON** (для обхода конфликтов `commons-compress`/`parquet-hadoop`).
    *   Использует партиционирование по колонкам `category` и `region`.
2.  **Metastore Registration (Beeline):**
    *   После успешного завершения Spark, Bash-скрипт запускает CLI утилиту `beeline`.
    *   Выполняет DDL команды (`CREATE EXTERNAL TABLE`) для регистрации созданных файлов как таблиц Hive.
    *   Этот подход обеспечивает надежную изоляцию процессов и гарантирует совместимость.

## Структура проекта
*   `deploy-spark.yml` — Ansible playbook для установки Spark 3.3.4.
*   `vars.yml` — Конфигурационные переменные.
*   `scripts/run-spark-job.sh` — Основной скрипт запуска (оркестратор).
*   `scripts/spark-data-processing.py` — Код приложения Spark (PySpark).
*   `templates/` — Шаблоны конфигураций (`spark-defaults.conf`, `spark-env.sh`).

## Инструкция по запуску

### 1. Развертывание Spark
Если Spark еще не установлен на кластере:
```bash
cd spark
ansible-playbook -i ../hdfs/inventory.ini deploy-spark.yml
```

### 2. Запуск обработки данных
Скрипт выполняет расчеты и автоматически создает таблицы в Hive:
```bash
./scripts/run-spark-job.sh
```

### 3. Проверка результатов
Для проверки наличия данных в таблицах Hive можно использовать скрипт из предыдущего задания или зайти в Beeline вручную:
```bash
/opt/hive/apache-hive-4.0.0-alpha-2/bin/beeline -u "jdbc:hive2://nn:10000/default" -n spark -p spark -e "SELECT * FROM sales_category_stats LIMIT 5;"
```

## Доступ к веб-интерфейсам (UI)

Для просмотра статуса задач и логов используйте SSH-туннелирование к узлу `nn` (192.168.1.47).

*   **YARN Resource Manager:** http://192.168.1.47:8088
    *   Показывает список запущенных и завершенных приложений.
*   **Spark History Server:** http://192.168.1.47:18080
    *   Детальная статистика по этапам (Stages) и задачам (Tasks).
    *   *Примечание:* Требует запуска `/opt/spark/spark-3.3.4/sbin/start-history-server.sh`.

## Созданные таблицы
В ходе выполнения создаются следующие внешние таблицы (External Tables):
1.  `sales_transformed` — Основные данные (партиционированы по `category`, `region`).
2.  `sales_category_stats` — Агрегация по категориям.
3.  `sales_region_stats` — Агрегация по регионам.
4.  `sales_category_region_stats` — Сводная статистика.
