#!/bin/bash
set -e
set -o pipefail

# Initialize variables
mysql_host=127.0.0.1
mysql_port=3306
mysql_user=root
mysql_pass=root
mysql_sslmode=disable
mysql_db=clickbench

# Function to display usage
usage() {
    echo "Usage: $0 [-mysql_host mysql_host] [-mysql_port mysql_port] [-mysql_user mysql_user] [-mysql_pass mysql_pass] [-mysql_sslmode mysql_sslmode]"
    echo "  --mysql_host Acceleration parameter: mysql_host (default: 127.0.0.1)"
    echo "  --mysql_port Acceleration parameter: mysql_port (default: 3306)"
    echo "  --mysql_user Acceleration parameter: mysql_user (default: root)"
    echo "  --mysql_pass Acceleration parameter: mysql_pass (default: root)"
    echo "  --mysql_sslmode Acceleration parameter: mysql_sslmode (default: disabled)"
    echo "  --mysql_db Acceleration parameter: mysql_db (default: clickbench)"
}

# Parse command-line options
while [[ "$#" -gt 0 ]]; do
    case "${1#--}" in
        engine)
            engine="$2"
            shift 2
            ;;
        mysql_host)
            mysql_host="$2"
            shift 2
            ;;
        mysql_port)
            mysql_port="$2"
            shift 2
            ;;
        mysql_user)
            mysql_user="$2"
            shift 2
            ;;
        mysql_pass)
            mysql_pass="$2"
            shift 2
            ;;
        mysql_sslmode)
            mysql_sslmode="$2"
            shift 2
            ;;
        mysql_db)
            mysql_db="$2"
            shift 2
            ;;
        *)
            echo "error: Invalid option: $1" 1>&2
            usage
            exit 1
            ;;
    esac
done

# Prepare MySQL tables
mysql -h $mysql_host -P $mysql_port -u $mysql_user -p$mysql_pass -e "CREATE DATABASE IF NOT EXISTS $mysql_db;"
mysql -h $mysql_host -P $mysql_port -u $mysql_user -p$mysql_pass -e "SET GLOBAL local_infile=1;"
mysql -h $mysql_host -P $mysql_port -u $mysql_user -p$mysql_pass $mysql_db < create.sql

# Download clickbench data
wget --no-verbose --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

# Copy CSV data into MySQL
mysql -h $mysql_host -P $mysql_port -u $mysql_user -p$mysql_pass --local-infile=1 $mysql_db -e "LOAD DATA LOCAL INFILE 'hits.csv' INTO TABLE hits FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"