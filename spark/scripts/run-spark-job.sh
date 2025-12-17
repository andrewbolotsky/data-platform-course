#!/bin/bash

set -e

SPARK_HOME="/opt/spark/spark-3.3.4"
SPARK_USER="spark"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_APP="${SCRIPT_DIR}/spark-data-processing.py"
LOG_FILE="/tmp/spark-job-output.log"
NN_HOST="nn"

cleanup() {
    if [ -n "${TEMP_APP}" ] && [ -f "${TEMP_APP}" ]; then
        rm -f "${TEMP_APP}"
    fi
}

trap cleanup EXIT

echo "=== Запуск Spark приложения ==="

if [ ! -f "${SPARK_HOME}/bin/spark-submit" ]; then
    echo "ОШИБКА: Spark не установлен в ${SPARK_HOME}"
    exit 1
fi

if [ ! -f "${SPARK_APP}" ]; then
    echo "ОШИБКА: Spark приложение не найдено: ${SPARK_APP}"
    exit 1
fi

TEMP_APP="/tmp/spark-data-processing-$$.py"
echo "Копирование приложения во временную директорию..."
cp "${SPARK_APP}" "${TEMP_APP}"
chmod 644 "${TEMP_APP}"

echo "Запуск Spark приложения на YARN..."
echo "Вывод будет сохранен в ${LOG_FILE}"

SUDO_PASSWORD="${SUDO_PASSWORD:-ORl8L_hG1f}"

rm -f ${LOG_FILE}
touch ${LOG_FILE}
chmod 666 ${LOG_FILE}
echo "Запуск: $(date)" > ${LOG_FILE}

DRIVER_IP=$(hostname -i)

echo "${SUDO_PASSWORD}" | sudo -S -u ${SPARK_USER} bash -c "export HADOOP_CONF_DIR=/opt/hadoop/current/etc/hadoop && ${SPARK_HOME}/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --verbose \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    --conf spark.driver.host=${DRIVER_IP} \
    --conf spark.driver.bindAddress=${DRIVER_IP} \
    --conf spark.network.timeout=120s \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.hadoop.hive.metastore.uris=thrift://${NN_HOST}:9083 \
    --conf spark.hadoop.fs.defaultFS=hdfs://${NN_HOST}:9000 \
    ${TEMP_APP}" >> ${LOG_FILE} 2>&1

EXIT_CODE=$?

if [ ${EXIT_CODE} -eq 0 ]; then
    echo ""
    echo "=== Spark приложение завершено успешно ==="
    echo "Последние строки вывода:"
    tail -20 ${LOG_FILE}
    
    echo ""
    echo "=== Создание таблиц в Hive (через Beeline) ==="
    HIVE_HOME="/opt/hive/apache-hive-4.0.0-alpha-2"
    BEELINE="${HIVE_HOME}/bin/beeline"
    JDBC_URL="jdbc:hive2://${NN_HOST}:10000/default"
    
    if [ -f "${BEELINE}" ]; then
        echo "Создание таблиц..."
        ${BEELINE} -u "${JDBC_URL}" -n spark -p spark -e "
        -- Таблица с основными данными (партиционированная)
        DROP TABLE IF EXISTS sales_transformed;
        CREATE EXTERNAL TABLE sales_transformed (
            id INT,
            product_name STRING,
            price DOUBLE,
            quantity INT,
            sale_date STRING,
            total_sale_amount DOUBLE,
            price_category STRING
        )
        PARTITIONED BY (category STRING, region STRING)
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        STORED AS TEXTFILE
        LOCATION 'hdfs://${NN_HOST}:9000/user/spark/output/sales_transformed';
        
        -- Восстановление партиций (так как Spark просто создал папки)
        MSCK REPAIR TABLE sales_transformed;
        
        -- Статистика по категориям
        DROP TABLE IF EXISTS sales_category_stats;
        CREATE EXTERNAL TABLE sales_category_stats (
            category STRING,
            product_count BIGINT,
            total_quantity BIGINT,
            avg_price DOUBLE,
            total_revenue DOUBLE
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        STORED AS TEXTFILE
        LOCATION 'hdfs://${NN_HOST}:9000/user/spark/output/sales_transformed_category_stats';
        
        -- Статистика по регионам
        DROP TABLE IF EXISTS sales_region_stats;
        CREATE EXTERNAL TABLE sales_region_stats (
            region STRING,
            sale_count BIGINT,
            total_revenue DOUBLE,
            avg_sale_amount DOUBLE
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        STORED AS TEXTFILE
        LOCATION 'hdfs://${NN_HOST}:9000/user/spark/output/sales_transformed_region_stats';
        
        SHOW TABLES;
        "
        
        echo "Таблицы созданы."
    else
        echo "ОШИБКА: Beeline не найден по пути ${BEELINE}"
    fi

else
    echo ""
    echo "=== Spark приложение завершилось с ошибкой (код: ${EXIT_CODE}) ==="
    echo "Последние строки вывода:"
    tail -50 ${LOG_FILE}
    exit ${EXIT_CODE}
fi

echo ""
echo "=== Проверка результатов ==="
echo "Запустите ./verify-hive-tables.sh для проверки таблиц в Hive."
