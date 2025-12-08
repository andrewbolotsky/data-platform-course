#!/bin/bash

# Скрипт для проверки данных через Hive клиент
# Использование: ./verify-hive-tables.sh

set -e

HIVE_HOME="/opt/hive/apache-hive-4.0.0-alpha-2"
HIVE_USER="hive"
HIVE_SERVER="nn"
HIVE_PORT="10000"

echo "=== Проверка данных через Hive клиент ==="

# Проверяем доступность HiveServer2
echo "Проверка доступности HiveServer2..."
if ! nc -z ${HIVE_SERVER} ${HIVE_PORT} 2>/dev/null; then
    echo "ОШИБКА: HiveServer2 недоступен на ${HIVE_SERVER}:${HIVE_PORT}"
    exit 1
fi
echo "HiveServer2 доступен"

# Используем beeline для подключения к Hive
echo ""
echo "=== Подключение к Hive через beeline ==="

sudo -u ${HIVE_USER} ${HIVE_HOME}/bin/beeline -u "jdbc:hive2://${HIVE_SERVER}:${HIVE_PORT}" -n ${HIVE_USER} << 'EOF'

-- Показываем все базы данных
!echo "=== Список баз данных ==="
SHOW DATABASES;

-- Используем базу данных default
USE default;

-- Показываем все таблицы
!echo ""
!echo "=== Список таблиц ==="
SHOW TABLES;

-- Проверяем таблицу sales_transformed
!echo ""
!echo "=== Проверка таблицы sales_transformed ==="
DESCRIBE sales_transformed;

!echo ""
!echo "=== Количество строк в sales_transformed ==="
SELECT COUNT(*) as total_rows FROM sales_transformed;

!echo ""
!echo "=== Первые 10 строк из sales_transformed ==="
SELECT * FROM sales_transformed LIMIT 10;

-- Проверяем партиции
!echo ""
!echo "=== Партиции в sales_transformed ==="
SHOW PARTITIONS sales_transformed;

-- Проверяем статистику по категориям
!echo ""
!echo "=== Статистика по категориям ==="
SELECT * FROM sales_category_stats ORDER BY category;

-- Проверяем статистику по регионам
!echo ""
!echo "=== Статистика по регионам ==="
SELECT * FROM sales_region_stats ORDER BY region;

-- Проверяем статистику по категориям и регионам
!echo ""
!echo "=== Статистика по категориям и регионам ==="
SELECT * FROM sales_category_region_stats ORDER BY category, region;

-- Пример запроса с фильтрацией
!echo ""
!echo "=== Пример запроса: товары категории Electronics ==="
SELECT product_name, price, quantity, total_sale_amount, region 
FROM sales_transformed 
WHERE category = 'Electronics' 
ORDER BY total_sale_amount DESC 
LIMIT 5;

-- Пример агрегации
!echo ""
!echo "=== Пример агрегации: топ-5 товаров по выручке ==="
SELECT product_name, category, 
       SUM(total_sale_amount) as total_revenue,
       SUM(quantity) as total_quantity
FROM sales_transformed
GROUP BY product_name, category
ORDER BY total_revenue DESC
LIMIT 5;

!quit
EOF

echo ""
echo "=== Проверка завершена ==="

