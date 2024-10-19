# More.Tech Трек 1. Озеро данных

## Описание

Реализация инкрементной выгрузки данных типа SCD2 при помощи Deltsa Lake

## Преимущества решения

## Структура проекта

```
.
├── spark_delta_init.py          # инициализация главной таблицы
└── spark_delta_join.py     # операция добавления инкремента
└── check_result.py  # чекер для проверки работоспособности
└── requirements.txt  
```

## Зависимости и подключение

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
Запускаем init-скрипт:


```plaintext
/opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.13:3.2.0 --jars spark-hadoop-cloud_2.13-3.5.3.jar spark_delta_init.py
```

Запускаем скрипт записи инкремента

```plaintext
/opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.13:3.2.0 --jars spark-hadoop-cloud_2.13-3.5.3.jar spark_delta_join.py
```
## Сложности и как их фиксить
