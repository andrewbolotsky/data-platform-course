#!/bin/bash

set -e

HADOOP_HOME="/opt/hadoop/current"
HDFS_URI="hdfs://nn:9000"
INPUT_DIR="/tmp/spark-input"
HDFS_INPUT_DIR="/user/spark/input"
HDFS_USER="hadoop"

echo "=== Загрузка тестовых данных на HDFS ==="

mkdir -p ${INPUT_DIR}

cat > ${INPUT_DIR}/sales_data.csv << 'EOF'
id,product_name,category,price,quantity,sale_date,region
1,Laptop,Electronics,999.99,5,2024-01-15,North
2,Smartphone,Electronics,699.99,10,2024-01-16,South
3,Tablet,Electronics,399.99,8,2024-01-17,East
4,Monitor,Electronics,249.99,12,2024-01-18,West
5,Keyboard,Electronics,79.99,20,2024-01-19,North
6,Chair,Furniture,199.99,15,2024-01-20,South
7,Desk,Furniture,299.99,10,2024-01-21,East
8,Bookshelf,Furniture,149.99,18,2024-01-22,West
9,T-Shirt,Clothing,29.99,50,2024-01-23,North
10,Jeans,Clothing,59.99,30,2024-01-24,South
11,Shoes,Clothing,89.99,25,2024-01-25,East
12,Jacket,Clothing,129.99,20,2024-01-26,West
13,Coffee,Food,9.99,100,2024-01-27,North
14,Tea,Food,7.99,80,2024-01-28,South
15,Bread,Food,3.99,150,2024-01-29,East
16,Milk,Food,4.99,120,2024-01-30,West
17,Headphones,Electronics,149.99,15,2024-02-01,North
18,Speaker,Electronics,199.99,12,2024-02-02,South
19,Mouse,Electronics,29.99,30,2024-02-03,East
20,Webcam,Electronics,79.99,18,2024-02-04,West
EOF

echo "Тестовые данные созданы в ${INPUT_DIR}"

sudo -u ${HDFS_USER} ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p ${HDFS_INPUT_DIR}

echo "Загрузка данных на HDFS..."
sudo -u ${HDFS_USER} ${HADOOP_HOME}/bin/hdfs dfs -put ${INPUT_DIR}/sales_data.csv ${HDFS_INPUT_DIR}/

echo "Проверка загруженных данных:"
sudo -u ${HDFS_USER} ${HADOOP_HOME}/bin/hdfs dfs -ls ${HDFS_INPUT_DIR}
sudo -u ${HDFS_USER} ${HADOOP_HOME}/bin/hdfs dfs -cat ${HDFS_INPUT_DIR}/sales_data.csv | head -5

echo "=== Данные успешно загружены на HDFS ==="

