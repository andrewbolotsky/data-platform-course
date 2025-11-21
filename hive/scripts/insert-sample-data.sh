#!/bin/bash

HIVE_HOME="/opt/hive/apache-hive-4.0.0-alpha-2"
HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop/current}"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
JDBC_HIVE="jdbc:hive2://nn:10000"

DATABASE="sample_db"
TABLE="sales_data_sample"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -d, --database DATABASE    Database name (default: ${DATABASE})"
    echo "  -t, --table TABLE          Table name (default: ${TABLE})"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "The script will:"
    echo "  * CREATE DATABASE IF NOT EXISTS ${DATABASE}"
    echo "  * CREATE TABLE IF NOT EXISTS with schema:"
    echo "      COLUMNS: id int, product string, amount double, sale_date string"
    echo "      PARTITION: year int, month int"
    echo "  * INSERT OVERWRITE sample data into partitions year=2024,month=1 and year=2024,month=2"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--database)
            DATABASE="$2"; shift 2 ;;
        -t|--table)
            TABLE="$2"; shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

HIVEQL="
CREATE DATABASE IF NOT EXISTS ${DATABASE};
USE ${DATABASE};

CREATE TABLE IF NOT EXISTS ${TABLE} (
    id int,
    product string,
    amount double,
    sale_date string
)
PARTITIONED BY (
    year int,
    month int
)
STORED AS PARQUET
TBLPROPERTIES (
    'comment' = 'Sample partitioned table for testing',
    'created_by' = 'insert-sample-data.sh'
);

INSERT OVERWRITE TABLE ${TABLE}
PARTITION (year=2024, month=1)
VALUES
  (1, 'Widget A', 100.50, '2024-01-05'),
  (2, 'Widget B', 250.00, '2024-01-10'),
  (3, 'Widget C', 89.99, '2024-01-15');

INSERT OVERWRITE TABLE ${TABLE}
PARTITION (year=2024, month=2)
VALUES
  (4, 'Widget D', 175.25, '2024-02-03'),
  (5, 'Widget E', 320.00, '2024-02-14');
"

echo "Setting up sample database/table and inserting data..."
echo "Database: ${DATABASE}"
echo "Table: ${TABLE}"
echo ""

sudo -u hive HADOOP_HOME="$HADOOP_HOME" JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/beeline" \
  -u "$JDBC_HIVE" \
  -n hive \
  -e "$HIVEQL"

if [ $? -eq 0 ]; then
  echo ""
  echo "✓ Sample database/table created and data inserted successfully"
  echo ""
  echo "Verifying data..."
  sudo -u hive HADOOP_HOME="$HADOOP_HOME" JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/beeline" \
    -u "$JDBC_HIVE" \
    -n hive \
    -e "USE ${DATABASE}; SELECT * FROM ${TABLE} ORDER BY id;"
else
  echo "✗ Failed to setup sample data" >&2
  exit 1
fi
