#!/bin/bash


HIVE_HOME="/opt/hive/apache-hive-4.0.0-alpha-2"
HADOOP_HOME="/opt/hadoop/current"
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
JDBC_HIVE="jdbc:hive2://nn:10000"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -d, --database DATABASE    Database name (default: default)"
    echo "  -t, --table TABLE          Table name"
    echo "  -f, --file FILE            Source data file (local path)"
    echo "  -p, --partition PARTITION  Partition spec, e.g. 'year=2024,month=1'"
    echo "  -o, --overwrite            Overwrite existing partition data"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 -d analytics -t sales_data -f /tmp/sales_2024_01.csv -p 'year=2024,month=1' -o"
}

DATABASE="default"
TABLE=""
FILE=""
PARTITION_VALUES=""
OVERWRITE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--database)
            DATABASE="$2"; shift 2 ;;
        -t|--table)
            TABLE="$2"; shift 2 ;;
        -f|--file)
            FILE="$2"; shift 2 ;;
        -p|--partition)
            PARTITION_VALUES="$2"; shift 2 ;;
        -o|--overwrite)
            OVERWRITE=true; shift ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

if [ -z "$TABLE" ] || [ -z "$FILE" ] || [ -z "$PARTITION_VALUES" ]; then
    echo "Error: table, file and partition values are required"
    usage
    exit 1
fi

if [ ! -f "$FILE" ]; then
    echo "Error: local file '$FILE' does not exist"
    exit 1
fi

STAGING_DIR="/tmp/hive_staging/${DATABASE}/${TABLE}/$(date +%Y%m%d_%H%M%S)"
echo "Using HDFS staging dir: $STAGING_DIR"

sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "$STAGING_DIR"
if [ $? -ne 0 ]; then
    echo "Failed to create HDFS staging dir"
    exit 1
fi

sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -put "$FILE" "$STAGING_DIR/"
if [ $? -ne 0 ]; then
    echo "Failed to upload file to HDFS staging dir"
    exit 1
fi

STAGED_FILE=$(sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -ls "$STAGING_DIR" | tail -1 | awk '{print $NF}')

if [ "$OVERWRITE" = true ]; then
    LOAD_MODE="OVERWRITE"
else
    LOAD_MODE="INTO"
fi

LOAD_SQL="
USE ${DATABASE};
LOAD DATA INPATH '${STAGED_FILE}' ${LOAD_MODE} TABLE ${TABLE} PARTITION (${PARTITION_VALUES});
"

echo "Loading data into ${DATABASE}.${TABLE} partition (${PARTITION_VALUES})..."
# выполняем LOAD DATA через HiveServer2
sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/beeline" \
  -u "$JDBC_HIVE" \
  -n hive \
  -e "$LOAD_SQL"

RC=$?

if [ $RC -eq 0 ]; then
    echo "Data loaded successfully"
    PARTITION_WHERE=$(echo "$PARTITION_VALUES" | sed 's/,/ AND /g')
    sudo -u hive JAVA_HOME="$JAVA_HOME" "$HIVE_HOME/bin/beeline" \
      -u "$JDBC_HIVE" \
      -n hive \
      -e "USE ${DATABASE}; SELECT COUNT(*) FROM ${TABLE} WHERE ${PARTITION_WHERE};"
else
    echo "Failed to load data into partition"
fi

sudo -u hadoop JAVA_HOME="$JAVA_HOME" "$HADOOP_HOME/bin/hdfs" dfs -rm -r "$STAGING_DIR" >/dev/null 2>&1 || true
