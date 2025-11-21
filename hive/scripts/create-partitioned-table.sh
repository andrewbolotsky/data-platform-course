#!/bin/bash


HIVE_HOME="/opt/hive/apache-hive-4.0.0-alpha-2"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
JDBC_HIVE="jdbc:hive2://nn:10000"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -d, --database DATABASE    Database name (default: default)"
    echo "  -t, --table TABLE          Table name"
    echo "  -p, --partition PARTITION  Partition columns (e.g., 'year int, month int')"
    echo "  -c, --columns COLUMNS      Table columns (e.g., 'id int, name string, value double')"
    echo "  -s, --storage STORAGE      Storage format (parquet, orc, textfile) (default: parquet)"
    echo "  -l, --location LOCATION    HDFS location for table data"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 -d analytics -t sales_data -p 'year int, month int' -c 'id int, product string, amount double, sale_date string' -s parquet"
}

DATABASE="default"
TABLE=""
PARTITION_COLUMNS=""
TABLE_COLUMNS=""
STORAGE_FORMAT="parquet"
LOCATION=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--database)
            DATABASE="$2"; shift 2 ;;
        -t|--table)
            TABLE="$2"; shift 2 ;;
        -p|--partition)
            PARTITION_COLUMNS="$2"; shift 2 ;;
        -c|--columns)
            TABLE_COLUMNS="$2"; shift 2 ;;
        -s|--storage)
            STORAGE_FORMAT="$2"; shift 2 ;;
        -l|--location)
            LOCATION="$2"; shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

if [ -z "$TABLE" ] || [ -z "$PARTITION_COLUMNS" ] || [ -z "$TABLE_COLUMNS" ]; then
    echo "Error: table, partition columns and table columns are required"
    usage
    exit 1
fi

if [ -z "$LOCATION" ]; then
    LOCATION="/user/hive/warehouse/${DATABASE}.db/${TABLE}"
fi

HIVEQL="
CREATE DATABASE IF NOT EXISTS ${DATABASE};
USE ${DATABASE};
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} (
    ${TABLE_COLUMNS}
)
PARTITIONED BY (
    ${PARTITION_COLUMNS}
)
STORED AS ${STORAGE_FORMAT}
LOCATION '${LOCATION}'
TBLPROPERTIES (
    'comment' = 'Partitioned table created by script',
    'created_by' = 'hive-task-3'
);
"

echo "Creating partitioned table ${DATABASE}.${TABLE}..."

sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/beeline" \
  -u "$JDBC_HIVE" \
  -n hive \
  -e "$HIVEQL"

if [ $? -eq 0 ]; then
  echo "Table ${DATABASE}.${TABLE} created successfully"
  sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/beeline" \
    -u "$JDBC_HIVE" \
    -n hive \
    -e "USE ${DATABASE}; DESCRIBE ${TABLE};"
else
  echo "Failed to create table ${DATABASE}.${TABLE}"
  exit 1
fi
