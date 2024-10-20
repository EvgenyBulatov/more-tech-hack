# More.Tech Трек 1. Озеро данных

## Описание

Реализация инкрементной выгрузки данных типа SCD2 двумя способами:
- через классический Spark с оптимизацией работы с партициями, сжатием и ORC-форматом файлов
- с применением технологии Delta Lake

## Структура проекта

```
.
├── orc_optimization      # Директория для оптимизации Spark с использованием ORC
│   ├── copy_init_orc.py   
│   ├── join_orc.py         
│   └── check_result_orc.py 
└── delta_lake_solution   # Директория для оптимизации Spark с использованием Delta Lake
    ├── copy_init_delta.py   
    ├── join_delta.py        
    └── check_result_delta.py 

```

## Подключение и запуск Spark

Для работы с проектом необходимо установить следующие зависимости:

- Apache Spark
- Delta Lake (прокинули в --packages и conf)
- PySpark

Заходим на эдж-ноду:

```plaintext
ssh -i /path/to/your_login your_login@edge_ip
```
Запускаем Spark:

```plaintext
SPARK_SSH_OPTS='-i /home/your_login/your_login' /opt/spark/sbin/start-all.sh
```

## Запуск решения с отимизацией кода и orc-форматом файлов
Запускаем init-скрипт:

```plaintext
/opt/spark/bin/spark-submit --jars spark-hadoop-cloud_2.13-3.5.3.jar orc_optimization/copy_init_orc.py
```

Запускаем скрипт записи инкремента

```plaintext
/opt/spark/bin/spark-submit --jars spark-hadoop-cloud_2.13-3.5.3.jar orc_optimization/join_orc.py
```

## Запуск решения с отимизацией через Delta Lake
Для запуска Spark с расширением Delta Lake нужно добавить при запуске spark-submit скрипта опцию --package

Запускаем init-скрипт:

```plaintext
/opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.13:3.2.0 --jars spark-hadoop-cloud_2.13-3.5.3.jar delta_lake_solution/copy_init_delta.py
```

Запускаем скрипт записи инкремента

```plaintext
/opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.13:3.2.0 --jars spark-hadoop-cloud_2.13-3.5.3.jar delta_lake_solution/join_delta.py
```
## Рекомендации при проблеме с S3 или Spark на нодах

Первоочередно ребутаем ноды, как воркеры, так и эдж-ноду. Это можно сделать через UI-интерфейс

Далее необходимо смонтировать диски спарка, делаем при помощи команды:
```plaintext
curl -s -sfLk https://gitlab.t1.cloud/events/more-tech2024/-/raw/main/mount-spark.sh?ref_type=heads | sh -
```
Затем, запускаем Spark уже известной командой
```plaintext
SPARK_SSH_OPTS='-i /home/your_login/your_login' /opt/spark/sbin/start-all.sh
```
Делее тестим, запуская любой скрипт .py, обращающийся к S3. Если есть проблемы с DNS, выполняем команду 
```plaintext
curl -s -sfLk https://185.159.111.62/events/more-tech2024/-/raw/main/fix-dns.sh?ref_type=heads  | sh -
```
Успехов в работе с кластером и инкрементной загрузкой данных!