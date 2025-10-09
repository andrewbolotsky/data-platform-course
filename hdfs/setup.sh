#!/bin/bash

echo "Настройка SSH для кластера HDFS..."

if [ ! -f ~/.ssh/id_rsa ]; then
    echo "Создание SSH ключа..."
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
fi

for host in 192.168.1.46 192.168.1.47 192.168.1.48 192.168.1.49; do
    echo "Копирование ключа на $host"
    ssh-copy-id -i ~/.ssh/id_rsa.pub team@$host
done

cat > ~/.ssh/config << EOF
Host nn
    HostName 192.168.1.47
    User team
    StrictHostKeyChecking no
    
Host jn
    HostName 192.168.1.46
    User team
    StrictHostKeyChecking no
    
Host dn-00
    HostName 192.168.1.48
    User team
    StrictHostKeyChecking no
    
Host dn-01
    HostName 192.168.1.49
    User team
    StrictHostKeyChecking no
EOF

chmod 600 ~/.ssh/config
echo "SSH настройка завершена!"


wget "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz" -O /tmp/hadoop-3.3.6.tar.gz
