## Запуск HDFS-кластера

### Команды для запуска HDFS-кластера и Yarn
Необходимо запустить скрипт

```bash 
./setup.sh
```

После этого, установить `ansible`, если он еще не установлен. 

```bash
sudo apt-add-repository --yes --update ppa:ansible/ansible
sudo apt install ansible
```

Запустить HDFS-кластер и YARN

```bash
ansible-playbook -i inventory.ini deploy-hdfs.yml -vvv
```

### Артефакты

Overview.

<img src="artefacts/overview.png" height="75%" width="75%"/>

Список дата-нод в web-ui hadoop.

<img src="artefacts/datanode_list.png" height="75%" width="75%"/>

Summary

<img src="artefacts/summary.png" height="75%" width="75%"/>

Yarn 

<img src="artefacts/yarn.png" height="75%" width="75%"/>


