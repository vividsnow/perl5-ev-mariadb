#!/bin/bash
set -e

BASE_DIR=$(pwd)/mariadb_tmp
DATA_DIR=$BASE_DIR/data
SOCKET=$BASE_DIR/mysql.sock
PID_FILE=$BASE_DIR/mysql.pid
PORT=3307

mkdir -p $DATA_DIR

echo "Initializing MariaDB..."
/usr/bin/mysql_install_db --no-defaults --user=$(whoami) --datadir=$DATA_DIR --auth-root-authentication-method=normal

echo "Starting MariaDB..."
/usr/sbin/mariadbd \
    --no-defaults \
    --datadir=$DATA_DIR \
    --socket=$SOCKET \
    --port=$PORT \
    --pid-file=$PID_FILE \
    --skip-networking=0 \
    --bind-address=127.0.0.1 \
    --innodb-buffer-pool-size=16M \
    --log-error=$BASE_DIR/error.log \
    2>&1 &

# Wait for it to start
echo "Waiting for MariaDB to start..."
for i in {1..30}; do
    if mariadb-admin --no-defaults --socket=$SOCKET --user=root ping >/dev/null 2>&1; then
        echo "MariaDB is up!"
        
        echo "Configuring test database and user..."
        mariadb --no-defaults --socket=$SOCKET --user=root <<SQL
            CREATE DATABASE IF NOT EXISTS test;
            CREATE USER IF NOT EXISTS 'testuser'@'127.0.0.1' IDENTIFIED BY 'testpass';
            GRANT ALL ON test.* TO 'testuser'@'127.0.0.1';
            FLUSH PRIVILEGES;
SQL
        exit 0
    fi
    sleep 1
done

echo "Failed to start MariaDB"
cat $BASE_DIR/error.log
exit 1
