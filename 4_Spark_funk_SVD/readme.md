### Задача

По имеющимся данным о рейтингах фильмов (MovieLens: 100 000 рейтингов) построить коллаборативную рекомендательную систему. Вторая часть (ачивка) — это продолжение работы, начатой в [Лабе 8] `здесь не представлена в виду простоты`. Мы предлагаем построить новую систему на основе коллаборативной фильтрации путём подсчёта базовых предикторов и создания с ними Item-Item Recommender System.

#### Обработка данных на вход

Входные данные: датасет MovieLens и `id` выданного вам пользователя для расчёта — тот же, что для [Лабы 8].

#### Обработка данных на выход

**Часть 3. Базовые предикторы:**

1. Глобальное среднее 𝞵 (`average_rating`) по всему датасету. `Сумма всех оценок по всем фильмам / Количество всех оценок по всем фильмам.`

2. Базовый предиктор для каждого пользователя (суммирование по фильмам, оцененным данным пользователем). Здесь I<sub>a</sub> — множество фильмов, по которым у пользователя есть рейтинги, а |I<sub>a</sub>| — их количество.

<img width="200px" src="images/laba08s_base_u.png">

3. Базовый предиктор для каждого фильма (суммирование по пользователям, поставившим оценку данному фильму). Здесь U<sub>i</sub> — множество пользователей, которые оценили данный фильм, а |U<sub>i</sub>| — их количество.

<img width="250px" src="images/laba08s_base_i.png">

4. Базовый предиктор для каждого пользователя и каждого фильма:

<img width="140px" src="images/laba08s_base_ui.png">

**Часть 4. Item-item CF:**

1. Вычесть из всех рейтингов r<sub>ui</sub> базовый предиктор b<sub>ui</sub> из пункта 3.4 (для всей таблицы рейтингов). Если рейтинга нет, то можно поставить результат = 0.
2. Найдите попарные меры близости (косинус) для всех фильмов, используя очищенные оценки из пункта 4.1. Суммирование идет по всем пользователям.

<img width="350px" src="images/laba08s_cosine_items.png">

3.  Для каждого фильма, по которому у данного пользователя не стоит рейтинг, найдите:

    - [a] 30 ближайших фильмов-соседей для этого фильма (среди всех фильмов, а не фильмов, оценённых пользователем).

    - [b] прогноз оценки пользователя по формуле (базовый предиктор из пункта 3.4). Здесь _S(i)_- множество фильмов-соседей для фильма _i_, по которым у данного пользователя есть оценка.
      <img width="300px" src="images/laba08s_item_item_cf.png">

           Заметим, что суммирование идет только по тем фильмам-соседям, которые оценил пользователь.

4.  Рекомендуйте пользователю 10 фильмов (`predicators_top10`) с самыми высокими оценками из пункта 4.3.

5.  При подсчете прогноза по формуле из пункта 4.3 отфильтруйте всех соседей с отрицательной близостью.

6.  Рекомендуйте пользователю 10 фильмов (`predicators_positive_top10`) с самыми высокими оценками из пункта 4.5.

### Подсказки
- Для понимания контекста ознакомьтесь с обзором рекомендательных систем ([часть 1](https://habr.com/ru/company/lanit/blog/420499/) и [часть 2](https://habr.com/ru/company/lanit/blog/421401/)).
- Item-Item Recommender System описана в разделе "Коллаборативная фильтрация (Item-based вариант)" в части 2.


#### Проверка

Результат следует сохранить в файл lab08s.json в своей домашней директории в следующем формате:

```
{
    "average_rating": 3.1111,
    "predicators_positive_top10": [489, 604, 1449, 611, 656, 170, 847, 793, 615, 963],
    "predicators_top10": [123, 604, 124, 456, 656, 170, 847, 793, 615, 963]
}
```

При равенстве соответствующих метрик сортировать необходимо по возрастанию `id`, рассматривать как число.

Округление метрик — до 4 знаков после запятой.

Для получения `True` по двум последним ключам **точность совпадения должна быть не менее 0.8**.

Проверка осуществляется [автоматическим скриптом] из Личного кабинета.


#### Решение

```python
import os
import sys
os.environ["PYSPARK_PYTHON"]='/opt/anaconda/envs/bd9/bin/python'
os.environ["SPARK_HOME"]='/usr/hdp/current/spark2-client'
os.environ["PYSPARK_SUBMIT_ARGS"]='--num-executors 3 pyspark-shell'

spark_home = os.environ.get('SPARK_HOME', None)

sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))
```


```python
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructType, StructField, IntegerType, StringType, DoubleType, ShortType
import json
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
```


```python
conf = SparkConf()

spark = SparkSession.builder.config(conf=conf).appName("Lab08 mikhail.novikov").getOrCreate()
```


```python
spark
```

#### Читаем Данные


```python
# user id | item id | rating | timestamp
df_rates_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", ShortType(), True),
    StructField("timestamp", LongType(), True),
    ])
```


```python
df_rates = spark.read.csv(path='/labs/lab08data/u.data', schema=df_rates_schema, sep='\t')
```


```python
df_rates.show(5)
```

    +-------+-------+------+---------+
    |user_id|item_id|rating|timestamp|
    +-------+-------+------+---------+
    |    196|    242|     3|881250949|
    |    186|    302|     3|891717742|
    |     22|    377|     1|878887116|
    |    244|     51|     2|880606923|
    |    166|    346|     1|886397596|
    +-------+-------+------+---------+
    only showing top 5 rows
    



```python
# movie id | movie title | release date | video release date |
#              IMDb URL | unknown | Action | Adventure | Animation |
#              Children's | Comedy | Crime | Documentary | Drama | Fantasy |
#              Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
#              Thriller | War | Western |
df_film_descr_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("movie_title", StringType(), True),
    StructField("release_date", StringType(), True),
    ])
```


```python
df_film_descr = spark.read.csv(path='/labs/lab08data/u.item', schema=df_film_descr_schema, sep='|')
df_film_descr.show(5)
```

    +--------+-----------------+------------+
    |movie_id|      movie_title|release_date|
    +--------+-----------------+------------+
    |       1| Toy Story (1995)| 01-Jan-1995|
    |       2| GoldenEye (1995)| 01-Jan-1995|
    |       3|Four Rooms (1995)| 01-Jan-1995|
    |       4|Get Shorty (1995)| 01-Jan-1995|
    |       5|   Copycat (1995)| 01-Jan-1995|
    +--------+-----------------+------------+
    only showing top 5 rows
    



```python
MY_USER_ID = 7
```

#### Часть 3


```python
# 3.1
# Глобальное среднее 𝞵 (average_rating) по всему датасету. 
# Сумма всех оценок по всем фильмам / Количество всех оценок по всем фильмам.
average_rating = df_rates.agg({'rating':'avg'}).collect()[0][0]
mu = average_rating
average_rating
```
    3.52986

```python
# 3.2
# Базовый предиктор для каждого пользователя (суммирование по фильмам, оцененным данным 
# пользователем). Здесь Ia — множество фильмов, по которым у пользователя есть рейтинги, 
# а |Ia| — их количество.

user_base_predictor = (df_rates.withColumn('r_ui-mu', F.col('rating')-F.lit(mu))
                       .groupBy('user_id')
                       .agg(F.count('item_id').alias('rating_cnt'),
                            F.sum('r_ui-mu').alias('Sum(r_ui-mu)'))
                       .withColumn('b_u', F.col('Sum(r_ui-mu)')/(F.col('rating_cnt')+10)
                                  )
                      )
user_base_predictor.show(5)
```

    +-------+----------+-------------------+--------------------+
    |user_id|rating_cnt|       Sum(r_ui-mu)|                 b_u|
    +-------+----------+-------------------+--------------------+
    |    148|        65| 30.559100000000022| 0.40745466666666696|
    |    463|       133| -88.47137999999993| -0.6186809790209785|
    |    471|        31| -4.425660000000007|-0.10794292682926845|
    |    496|       129| -64.35193999999993| -0.4629635971223016|
    |    833|       267|-126.47261999999986| -0.4565798555956674|
    +-------+----------+-------------------+--------------------+
    only showing top 5 rows
  
```python
user_base_predictor
```


```python
# 3.3
# Базовый предиктор для каждого фильма (суммирование по пользователям, поставившим 
# оценку данному фильму). Здесь Ui — множество пользователей, которые оценили данный 
# фильм, а |Ui| — их количество.

item_base_predictor = (df_rates.join(user_base_predictor.select('user_id','b_u'), on='user_id', how='left')
                       .withColumn('r_ui-b-mu', F.col('rating')-F.col('b_u')-F.lit(mu))
                       .groupBy('item_id')
                       .agg(F.count('user_id').alias('rating_cnt'),
                            F.sum('r_ui-b-mu').alias('Sum(r_ui-b-mu)'))
                       .withColumn('b_i', F.col('Sum(r_ui-b-mu)')/(F.col('rating_cnt')+25)))
item_base_predictor.show(5)
```

    +-------+----------+------------------+--------------------+
    |item_id|rating_cnt|    Sum(r_ui-b-mu)|                 b_i|
    +-------+----------+------------------+--------------------+
    |    496|       231|  110.213205230345| 0.43052033293103514|
    |    463|        71|15.669908023554438| 0.16322820857869205|
    |    471|       221|13.428881775187126| 0.05458895030563872|
    |    148|       128|-38.45526015087247|-0.25134156961354553|
    |   1591|         6|0.7390940292408579| 0.02384174287873735|
    +-------+----------+------------------+--------------------+
    only showing top 5 rows

```python
# 3.4 
# Базовый предиктор для каждого пользователя и каждого фильма:
user_item_base_predictor = (df_rates.select('user_id', 'item_id', 'rating')
                            .join(user_base_predictor.select('user_id','b_u'), on='user_id', how='left')
                            .join(item_base_predictor.select('item_id','b_i'), on='item_id', how='left')
                            .withColumn('b_ui', F.lit(mu)+F.col('b_u')+F.col('b_i')
                                       )
                           )

user_item_base_predictor.show(5)
```

    +-------+-------+------+--------------------+--------------------+------------------+
    |item_id|user_id|rating|                 b_u|                 b_i|              b_ui|
    +-------+-------+------+--------------------+--------------------+------------------+
    |    148|    251|     2| 0.23219287356321877|-0.25134156961354553| 3.510711303949673|
    |    148|    580|     4|0.019238245614034907|-0.25134156961354553|3.2977566760004895|
    |    148|    633|     1|-0.18723352941176444|-0.25134156961354553|  3.09128490097469|
    |    148|    642|     5| 0.08995280487804924|-0.25134156961354553| 3.368471235264504|
    |    148|    406|     3|0.002238295454545...|-0.25134156961354553|    3.280756725841|
    +-------+-------+------+--------------------+--------------------+------------------+
    only showing top 5 rows
    
#### Часть 4

```python
# Часть 4.1
# Вычесть из всех рейтингов rui базовый предиктор bui из пункта 3.4 
# (для всей таблицы рейтингов). Если рейтинга нет, то можно поставить результат = 0.
part4 = (user_item_base_predictor.withColumn('r', F.col('rating')-F.col('b_ui'))
         .fillna({'r':0})
         .select('item_id', 'user_id', 'r')
        )
part4.show(5)
```

    +-------+-------+-------------------+
    |item_id|user_id|                  r|
    +-------+-------+-------------------+
    |    148|    251|-1.5107113039496731|
    |    148|    580| 0.7022433239995105|
    |    148|    642| 1.6315287647354961|
    |    148|    406|    -0.280756725841|
    |    148|    271|-0.3295563470531211|
    +-------+-------+-------------------+
    only showing top 5 rows
    



```python
# Часть 4.2
# Найдите попарные меры близости (косинус) для всех фильмов, используя очищенные оценки 
# из пункта 4.1. Суммирование идет по всем пользователям.

# группируем для векторизации, все фильмы
part4_2 = part4.groupBy(F.col("item_id"),F.col("user_id"))\
               .avg('r')\
               .withColumnRenamed('avg(r)', 'r')
part4_2.show(2)
```

    +-------+-------+-------------------+
    |item_id|user_id|                  r|
    +-------+-------+-------------------+
    |    148|    251|-1.5107113039496731|
    |    148|    580| 0.7022433239995105|
    +-------+-------+-------------------+
    only showing top 2 rows
    



```python
# собираем через рдд спарс вектора
rdd = part4_2.rdd.map(lambda x: (x.item_id, [(x.user_id, x.r)]))
rdd = rdd.reduceByKey(lambda a, b: a + b)
# rdd = rdd.map(lambda x: (x[0], Vectors.sparse(len(x[1]), x[1])))

rdd.take(2)
```

    [(1400,
      [(847, 1.9795295953091974),
       (924, 0.3140397402367343),
       (416, 0.20598642102159292),
       (189, -0.9102841948769278),
       (381, -0.7449544679511431),
       (537, -0.833050938024134),
       (234, -0.08567537067719311),
       (378, -0.38120334841374603),
       (655, 0.12856985909816387),
       (405, -0.8116138451191739)]),
     (1600,
      [(514, 0.1138759922212027),
       (439, 1.2730496172809835),
       (782, 0.11945342699676997),
       (655, 0.021892726598404355)])]

```python
# получили колличество юзеров
num_of_users = df_rates.groupby(F.col('user_id')).count().groupby().count().collect()[0][0]
num_of_users
```

    943

```python
# собрали спарс вектора
from pyspark.mllib.linalg import SparseVector
rdd = rdd.map(lambda x: (x[0], SparseVector(num_of_users, x[1])))
```

```python
rdd.take(2)
```

    [(1400,
      SparseVector(943, {189: -0.9103, 234: -0.0857, 378: -0.3812, 381: -0.745, 405: -0.8116, 416: 0.206, 537: -0.8331, 655: 0.1286, 847: 1.9795, 924: 0.314})),
     (1600,
      SparseVector(943, {439: 1.273, 514: 0.1139, 655: 0.0219, 782: 0.1195}))]

```python
# Вернули исходный вид спарк дф
part4_2 = rdd.toDF(['item_id', 'feature'])
part4_2.show(5)
```

    +-------+--------------------+
    |item_id|             feature|
    +-------+--------------------+
    |   1400|(943,[189,234,378...|
    |   1600|(943,[439,514,655...|
    |    800|(943,[13,21,28,92...|
    |   1000|(943,[22,87,125,2...|
    |   1200|(943,[90,167,234,...|
    +-------+--------------------+
    only showing top 5 rows
    
```python
# задали udf и матрицу cos_sim
@F.udf
def sim_cos(v1,v2):
    try:
        p = 2
        return float(v1.dot(v2))/float(v1.norm(p)*v2.norm(p))
    except:
        return 0

# строим лениво матрицу
cossimmatrix = (part4_2.alias("i")
                .join(part4_2.alias("j"), F.col("i.item_id") != F.col("j.item_id"), how='left')
                .select(
                    F.col("i.item_id").alias("item_id"),
                    F.col("i.feature").alias("i_feature"),
                    F.col("i.item_id").alias("i"),
                    F.col("j.item_id").alias("j"),
                    sim_cos("i.feature", "j.feature").cast("float").alias("sim_cosine"),
                    F.col("j.feature").alias("j_feature")
                )
               )
                      
cossimmatrix.show(30)
```

    +-------+--------------------+----+----+-------------+--------------------+
    |item_id|           i_feature|   i|   j|   sim_cosine|           j_feature|
    +-------+--------------------+----+----+-------------+--------------------+
    |   1400|(943,[189,234,378...|1400|1600|  8.307104E-4|(943,[439,514,655...|
    |   1400|(943,[189,234,378...|1400| 800| -0.005992483|(943,[13,21,28,92...|
    |   1400|(943,[189,234,378...|1400|1000|          0.0|(943,[22,87,125,2...|
    |   1400|(943,[189,234,378...|1400|1200|   0.15488388|(943,[90,167,234,...|
    |   1400|(943,[189,234,378...|1400| 400|   0.03447101|(943,[5,13,38,254...|
    |   1400|(943,[189,234,378...|1400| 200| 1.3568508E-4|(943,[1,5,6,7,10,...|
    |   1400|(943,[189,234,378...|1400| 600|          0.0|(943,[7,932],[0.1...|
    |   1400|(943,[189,234,378...|1400|1201|          0.0|(943,[90],[0.7683...|
    |   1400|(943,[189,234,378...|1400| 601| 0.0023102893|(943,[7,13,60,91,...|
    |   1400|(943,[189,234,378...|1400|   1| -0.017771928|(943,[1,2,5,6,10,...|
    |   1400|(943,[189,234,378...|1400|1601|          0.0|(943,[445],[-1.04...|
    |   1400|(943,[189,234,378...|1400|1001|          0.0|(943,[22,45,57,75...|
    |   1400|(943,[189,234,378...|1400|1401|  -0.03782458|(943,[189,201,214...|
    |   1400|(943,[189,234,378...|1400| 801|          0.0|(943,[13,87,125,2...|
    |   1400|(943,[189,234,378...|1400| 201|  0.024288274|(943,[1,7,9,13,19...|
    |   1400|(943,[189,234,378...|1400| 401| -0.014229348|(943,[5,7,11,13,3...|
    |   1400|(943,[189,234,378...|1400| 614|    0.0140135|(943,[7,13,18,90,...|
    |   1400|(943,[189,234,378...|1400|1614|          0.0|(943,[496],[-0.06...|
    |   1400|(943,[189,234,378...|1400| 814|          0.0|(943,[13],[1.8229...|
    |   1400|(943,[189,234,378...|1400|1214|-0.0061655883|(943,[92,116,299,...|
    |   1400|(943,[189,234,378...|1400|1414|          0.0|(943,[195],[-1.26...|
    |   1400|(943,[189,234,378...|1400| 214|  0.058000457|(943,[1,5,7,18,23...|
    |   1400|(943,[189,234,378...|1400|1014|-0.0019000055|(943,[26,38,92,94...|
    |   1400|(943,[189,234,378...|1400| 414| -0.018889753|(943,[5,10,11,13,...|
    |   1400|(943,[189,234,378...|1400|  14|  -0.07412605|(943,[1,2,6,13,14...|
    |   1400|(943,[189,234,378...|1400|1226|   -0.0142052|(943,[94,104,116,...|
    |   1400|(943,[189,234,378...|1400|1426|  0.018733377|(943,[201,250,303...|
    |   1400|(943,[189,234,378...|1400|  26|   -0.0596494|(943,[1,18,43,89,...|
    |   1400|(943,[189,234,378...|1400| 626|   0.15146291|(943,[7,234,405,6...|
    |   1400|(943,[189,234,378...|1400|1626|          0.0|(943,[648],[-2.18...|
    +-------+--------------------+----+----+-------------+--------------------+
    only showing top 30 rows
    
```python
# Фильмы просмотренные моим 7 юзером
my_user_films = df_rates.filter(df_rates.user_id == MY_USER_ID).select('item_id').distinct()
my_user_films_list = [row[0] for row in my_user_films.collect()]
my_user_films_list[0:10]
```

    [471,
     496,
     463,
     623,
     540,
     31,
     451,
     580,
     481,
     53]

```python
# 4.3 Для каждого фильма, по которому у данного пользователя не стоит рейтинг, найдите:
# 
# [a] 30 ближайших фильмов-соседей для этого фильма (среди всех фильмов, а не фильмов, оценённых пользователем).

window = Window.partitionBy(cossimmatrix['i'])\
               .orderBy(cossimmatrix['sim_cosine'].desc())

part4_3a = cossimmatrix.filter(F.col('i').isin(my_user_films_list)==False)\
                       .select('*', rank().over(window).alias('rank'))\
                       .filter(F.col('rank') <= 30)\
                       .orderBy(F.col('i').asc(), F.col('rank').asc(), F.col('j').asc())
                        
part4_3a.show(300)
```

    +-------+--------------------+---+----+----------+--------------------+----+
    |item_id|           i_feature|  i|   j|sim_cosine|           j_feature|rank|
    +-------+--------------------+---+----+----------+--------------------+----+
    |     26|(943,[1,18,43,89,...| 26| 267|0.23520336|(943,[1,5,130,268...|   1|
    |     26|(943,[1,18,43,89,...| 26|1048|0.22428009|(943,[42,43,58,59...|   2|
    |     26|(943,[1,18,43,89,...| 26| 581|0.20442957|(943,[7,43,49,59,...|   3|
    |     26|(943,[1,18,43,89,...| 26|1467|0.20111226|(943,[244,886],[1...|   4|
    |     26|(943,[1,18,43,89,...| 26| 467|0.18032318|(943,[6,10,13,16,...|   5|
    |     26|(943,[1,18,43,89,...| 26| 503|0.17422262|(943,[6,7,59,64,1...|   6|
    |     26|(943,[1,18,43,89,...| 26|   5|0.17372009|(943,[1,13,21,28,...|   7|
    |     26|(943,[1,18,43,89,...| 26|  42|0.16993286|(943,[1,5,11,13,1...|   8|
    |     26|(943,[1,18,43,89,...| 26|1316|0.16796073|(943,[179,286,351...|   9|
    |     26|(943,[1,18,43,89,...| 26|  90|0.16691972|(943,[1,5,7,11,13...|  10|
    |     26|(943,[1,18,43,89,...| 26|1550|0.16322465|(943,[405,417],[1...|  11|
    |     26|(943,[1,18,43,89,...| 26|1074|0.15895696|(943,[49,56,59,62...|  12|
    |     26|(943,[1,18,43,89,...| 26|  49|0.15140241|(943,[1,13,43,49,...|  13|
    |     26|(943,[1,18,43,89,...| 26| 186|0.14950731|(943,[1,5,6,7,10,...|  14|
    |     26|(943,[1,18,43,89,...| 26| 591|0.14677568|(943,[7,12,15,16,...|  15|
    |     26|(943,[1,18,43,89,...| 26| 156| 0.1458928|(943,[1,6,7,10,16...|  16|
    |     26|(943,[1,18,43,89,...| 26|1554| 0.1436465|(943,[405,655],[2...|  17|
    |     26|(943,[1,18,43,89,...| 26|1181|   0.14344|(943,[87,279,280,...|  18|
    |     26|(943,[1,18,43,89,...| 26|1297|0.14190628|(943,[151,184,280...|  19|
    |     26|(943,[1,18,43,89,...| 26|1274|0.14142677|(943,[130,276,279...|  20|
    +-------+--------------------+---+----+----------+--------------------+----+
    only showing top 20 rows
    
```python
# 4.3 Для каждого фильма, по которому у данного пользователя не стоит рейтинг, найдите:

# [b] прогноз оценки пользователя по формуле (базовый предиктор из пункта 3.4). Здесь S(i)- множество фильмов-соседей для фильма i, по которым у данного пользователя есть оценка.
# i не смотрел
# j смотрел

part4_3b = (cossimmatrix.filter((F.col('i').isin(my_user_films_list)==False) & (F.col('j').isin(my_user_films_list)))
            .orderBy(F.col('i').asc(),  F.col('sim_cosine').asc(), F.col('j').asc()))
                        
part4_3b.show(10)
```

    +-------+--------------------+---+---+------------+--------------------+
    |item_id|           i_feature|  i|  j|  sim_cosine|           j_feature|
    +-------+--------------------+---+---+------------+--------------------+
    |      1|(943,[1,2,5,6,10,...|  1|428| -0.11013166|(943,[5,7,11,13,1...|
    |      1|(943,[1,2,5,6,10,...|  1|179|-0.085115865|(943,[1,7,10,13,1...|
    |      1|(943,[1,2,5,6,10,...|  1|679|-0.084515445|(943,[7,13,23,38,...|
    |      1|(943,[1,2,5,6,10,...|  1|642| -0.08405596|(943,[7,16,23,59,...|
    |      1|(943,[1,2,5,6,10,...|  1|213|-0.082500264|(943,[1,6,7,11,14...|
    |      1|(943,[1,2,5,6,10,...|  1|675| -0.08131201|(943,[7,13,21,59,...|
    |      1|(943,[1,2,5,6,10,...|  1|135| -0.08115436|(943,[1,5,6,7,10,...|
    |      1|(943,[1,2,5,6,10,...|  1|573|-0.081140764|(943,[7,11,13,21,...|
    |      1|(943,[1,2,5,6,10,...|  1|162| -0.07727488|(943,[1,5,7,10,18...|
    |      1|(943,[1,2,5,6,10,...|  1|662| -0.07595064|(943,[7,11,13,23,...|
    +-------+--------------------+---+---+------------+--------------------+
    only showing top 10 rows
    
```python
# очищенные рейтинги для моего пользователя
part4_3b2 = part4.filter(F.col('user_id')==MY_USER_ID)
part4_3b2.show(10)
```

    +-------+-------+--------------------+
    |item_id|user_id|                   r|
    +-------+-------+--------------------+
    |    463|      7|-0.11794636838498551|
    |    471|      7|-0.00930711011193...|
    |    496|      7|  0.6147615072626715|
    |    540|      7|-0.37353481660374444|
    |    623|      7| -0.5988911473682395|
    |     31|      7|-0.00388018878821...|
    |    451|      7|  1.2067161215493885|
    |    580|      7| -0.8658980912952567|
    |     53|      7|  1.4593934056870008|
    |    481|      7|  0.8297069637468546|
    +-------+-------+--------------------+
    only showing top 10 rows
    
```python
# готовим для подсчета по формуле из задания
# Джойн по просмотренным фильмам
part4_3b = (part4_3b.alias('a')
            .join(part4_3b2.alias('b'), F.col('a.j') == F.col('b.item_id'))
            .select(F.col('a.i').alias('i'),
                    F.col('a.j').alias('j'),
                    F.col('a.sim_cosine').alias('sim_cosine'),
                    F.abs(F.col('a.sim_cosine')).alias('|sim_cosine|'),
                    F.col('b.r').alias('r')))

part4_3b.show(10)
```

    +---+---+------------+------------+--------------------+
    |  i|  j|  sim_cosine||sim_cosine||                   r|
    +---+---+------------+------------+--------------------+
    |  1| 29| 0.024921766| 0.024921766|-0.31089245964025247|
    |  2| 29| 0.044533655| 0.044533655|-0.31089245964025247|
    |  3| 29| 0.099985875| 0.099985875|-0.31089245964025247|
    |  5| 29|  0.15394446|  0.15394446|-0.31089245964025247|
    |  6| 29|-0.057951916| 0.057951916|-0.31089245964025247|
    | 13| 29| -0.02142117|  0.02142117|-0.31089245964025247|
    | 35| 29| 0.056229163| 0.056229163|-0.31089245964025247|
    | 36| 29|   0.1228695|   0.1228695|-0.31089245964025247|
    | 37| 29|  0.02684979|  0.02684979|-0.31089245964025247|
    | 38| 29|  0.29243577|  0.29243577|-0.31089245964025247|
    +---+---+------------+------------+--------------------+
    only showing top 10 rows
    
```python
# рандом чек фильма который не смотрел мой пользователь
item_base_predictor.filter(F.col('item_id')==30).show(5)
```

    +-------+----------+------------------+------------------+
    |item_id|rating_cnt|    Sum(r_ui-b-mu)|               b_i|
    +-------+----------+------------------+------------------+
    |     30|        37|13.604479960717354|0.2194270961406025|
    +-------+----------+------------------+------------------+
    
```python
#  подсчитали для этого фильма его средние предикторы
user_item_base_predictor.groupBy('item_id').avg('rating', 'b_u', 'b_i', 'b_ui').filter(F.col('item_id')==30).show(5)
```

    +-------+-----------------+-------------------+-------------------+------------------+
    |item_id|      avg(rating)|           avg(b_u)|           avg(b_i)|         avg(b_ui)|
    +-------+-----------------+-------------------+-------------------+------------------+
    |     30|3.945945945945946|0.04839729835899021|0.21942709614060257|3.7976843944995933|
    +-------+-----------------+-------------------+-------------------+------------------+
    
```python
# прогноз оценки пользователя по формуле (базовый предиктор из пункта 3.4). 

# Джойн по непросмотренным фильмам
part4_3b = (part4_3b.alias('a')
            .join(user_item_base_predictor.groupBy('item_id')
                  .avg('rating', 'b_u', 'b_i', 'b_ui')
                  .alias('c'),
                  F.col('a.i') == F.col('c.item_id'))
            .select(F.col('a.i').alias('i'),
                    F.col('a.j').alias('j'),
                    F.col('a.sim_cosine').alias('sim_cosine'),
                    F.col('a.|sim_cosine|').alias('|sim_cosine|'),
                    F.col('a.r').alias('r'),
                    F.col('c.avg(b_ui)').alias('b_i')
                   )
           )
part4_3b.show(10)
```

    +---+---+------------+------------+--------------------+------------------+
    |  i|  j|  sim_cosine||sim_cosine||                   r|               b_i|
    +---+---+------------+------------+--------------------+------------------+
    | 26| 29| 0.056706835| 0.056706835|-0.31089245964025247|3.4809318633836077|
    | 26|474| 0.033516977| 0.033516977|  0.4855446567115971|3.4809318633836077|
    | 26|367| -0.11908158|  0.11908158|   1.095988562510123|3.4809318633836077|
    | 26|415| -0.08627768|  0.08627768|  -1.619574529687684|3.4809318633836077|
    | 26|385|  0.08782647|  0.08782647|  1.0153318343323812|3.4809318633836077|
    | 26| 22|  0.07210166|  0.07210166|  0.5291210224992398|3.4809318633836077|
    | 26|198|-0.018113887| 0.018113887|  -1.317625367042294|3.4809318633836077|
    | 26|530|  0.01482598|  0.01482598|  0.7244642128226264|3.4809318633836077|
    | 26|196|0.0030560917|0.0030560917|  0.7396723590923209|3.4809318633836077|
    | 26|427| 0.058441754| 0.058441754| 0.47458339402331884|3.4809318633836077|
    +---+---+------------+------------+--------------------+------------------+
    only showing top 10 rows
    
```python
# Решение для 4.3 
# Предсказываем рейтинги фильмам которые не смотрел наш юзер...

# i не смотрел/ не стоит рейтинг
# j смотрел/ стоит рейтинг
part4_3b3 = (part4_3b.groupBy('i')
             .agg(F.sum(F.col('sim_cosine')*F.col('r')).alias('nominator'),
                  F.sum(F.col('|sim_cosine|')).alias('denominator'),
                  F.first(F.col('b_i')).alias('b_i'))
             .withColumn('r_forecast', F.col('b_i')+F.col('nominator')/F.col('denominator')))

part4_3b3.show(10)
```

    +----+--------------------+------------------+------------------+------------------+
    |   i|           nominator|       denominator|               b_i|        r_forecast|
    +----+--------------------+------------------+------------------+------------------+
    |  26|  1.5370581764352829| 17.87699457461713| 3.480931863383608|3.5669115379882923|
    | 964|  1.1527818545737145| 24.28822864482936|3.3520340533173663| 3.399496627531033|
    |1677| -0.6203980776529017| 6.205257705645636| 3.217293290410031| 3.117313868590297|
    |  65| 0.16138649214481215|17.440598226823454|3.5427488232070297|3.5520023184166494|
    |1010|  1.9567643609914511|12.652809326857096|3.2923504598783135| 3.447001044617466|
    |1224|  -2.589329071381268|23.761719456262654| 3.080604422356753|2.9716338120376067|
    |1258|-0.27318067786049677|10.389754281604837| 3.013432921795194| 2.987139645702595|
    |1277| -0.5592807856732617| 7.413083972038294| 3.418858795000096|3.3434137052395005|
    |1360|  1.9995694797106338| 9.803339127087384|2.4842620374766327| 2.688230241961742|
    | 222| -1.6805419189800535|17.464347807304875|3.6532325729381414|3.5570055659223323|
    +----+--------------------+------------------+------------------+------------------+
    only showing top 10 rows
    
```python
# 4.4 
# Рекомендуйте пользователю 10 фильмов (predicators_top10) с самыми высокими оценками из пункта 4.3.
part4_3b3.coalesce(12)
predicators_top10 = part4_3b3.orderBy(F.col('r_forecast').desc()).limit(10)
predicators_top10.show()
```

    +----+-------------------+------------------+------------------+------------------+
    |   i|          nominator|       denominator|               b_i|        r_forecast|
    +----+-------------------+------------------+------------------+------------------+
    |1293| 0.9283553507759515|1.2186059455852956|3.9070838589673165|4.6689013719534875|
    | 408| 1.0024240627232512|15.652935922757024| 4.319980826646948| 4.384021468458168|
    |1201| 1.3470673343690938|10.884480696753599|4.2316954094292765| 4.355455777231745|
    | 169|0.23884599015881397|18.244602496444713| 4.315290273508022| 4.328381596829829|
    | 313| 1.3212459475239784| 13.01592663043266| 4.198514594579809| 4.300024535826309|
    | 114| 0.9891641082736152|17.859253708651522|4.2317052993706525| 4.287091942307162|
    | 272|0.27813632118428605|10.867353827925399| 4.177797618912422|    4.203391367454|
    | 963| 1.8244485719833556|12.747346713636944| 4.055365644628591| 4.198489434462563|
    | 302| 0.9171377336698364| 11.57350911184767| 4.110736324261289| 4.189980892594585|
    | 124|  2.992154971669891|16.067599083100504|3.9970574742633684|  4.18328037889241|
    +----+-------------------+------------------+------------------+------------------+
    
```python
# собираем predicators_top10_list по 4.4 заданию
predicators_top10_list = predicators_top10.select('i').rdd.flatMap(lambda x: x).collect()
predicators_top10_list
# [1293, 408, 1201, 169, 313, 114, 272, 963, 302, 124]
```

    [1293, 408, 1201, 169, 313, 114, 272, 963, 302, 124]

```python
# 5 При подсчете прогноза по формуле из пункта 4.3 отфильтруйте всех соседей с отрицательной близостью.
part4_3b5 = (part4_3b.filter(F.col('sim_cosine') >= 0)
             .groupBy('i')
             .agg(F.sum(F.col('sim_cosine')*F.col('r')).alias('nominator'),
                  F.sum(F.col('|sim_cosine|')).alias('denominator'),
                  F.first(F.col('b_i')).alias('b_i'))
             .withColumn('r_forecast', F.col('b_i')+F.col('nominator')/F.col('denominator')
                        )
            )
part4_3b5.show(10)
```

    +----+--------------------+------------------+------------------+------------------+
    |   i|           nominator|       denominator|               b_i|        r_forecast|
    +----+--------------------+------------------+------------------+------------------+
    |  26|  1.6768130826894376|10.133650999108795| 3.480931863383608|3.6464016514034348|
    | 964|  0.8941873813950388|11.373356908981805|3.3520340533173663|3.4306552896463587|
    |1677|-0.46460105795994056|  2.70791155542247| 3.217293290410031|  3.04572156494891|
    |  65|-0.05122382862242582|  7.84756628752973|3.5427488232070297| 3.536221471064261|
    |1010| 0.47766998467614363| 5.799465168471215|3.2923504598783135| 3.374714948809637|
    |1224| -1.6675687890656763| 9.350898484233767| 3.080604422356753|2.9022719560298746|
    |1258| -0.5802762537064605|3.8872033714069403| 3.013432921795194|2.8641543278387283|
    |1277|0.004111978740068354|4.0332165650470415| 3.418858795000096|3.4198783233774472|
    |1360|  1.3266619823594372| 4.844760075429804|2.4842620374766327|2.7580964403581296|
    | 222| -1.4888144576508189|11.059768332088424|3.6532325729381414| 3.518617234447621|
    +----+--------------------+------------------+------------------+------------------+
    only showing top 10 rows
    
```python
# фильтруем для для вывода 4.5
predicators_positive_top10 = part4_3b5.orderBy(F.col('r_forecast').desc()).limit(10)
predicators_positive_top10.show()
```

    +----+--------------------+------------------+------------------+------------------+
    |   i|           nominator|       denominator|               b_i|        r_forecast|
    +----+--------------------+------------------+------------------+------------------+
    | 408|  0.5000364734944016| 7.874806440202519| 4.319980826646947| 4.383479082418874|
    | 114|   0.686426059127643| 9.564765722374432| 4.231705299370653| 4.303471412525094|
    |1293| 0.24105780473282026|0.6157638791482896|3.9070838589673165| 4.298561522558506|
    | 169|-0.24974798939124807| 9.100340002332814| 4.315290273508021| 4.287846464889087|
    | 963|  1.3947129589876777| 6.371045008854708| 4.055365644628591| 4.274279960419682|
    | 313|  0.5253140443485713| 7.952847369007941| 4.198514594579809| 4.264568175137096|
    |1201| 0.07555117015341485| 5.914108835742809|4.2316954094292765|4.2444701439246595|
    |1122| 0.37354771477205706|2.9571424122550525| 4.097118031051518| 4.223438533312076|
    |1660|  1.2066501331747612|   4.8250292862067|  3.95893183549124| 4.209013246732455|
    | 302|  0.5931914045612989|  6.64045332531532| 4.110736324261288| 4.200066280440618|
    +----+--------------------+------------------+------------------+------------------+
    
```python
# собираем лист 4.5
predicators_positive_top10_list = predicators_positive_top10.select('i').rdd.flatMap(lambda x: x).collect()
predicators_positive_top10_list
# [408, 114, 1293, 169, 963, 313, 1201, 1122, 1660, 302]
```

    [408, 114, 1293, 169, 963, 313, 1201, 1122, 1660, 302]

Точность по проверочному скрипту 0,931

```python
# сохранили файл. Его нужно положить на сервер и отдать чекеру
with open(file='lab08s.json', mode='wt') as file:
    file.write(json.dumps({'average_rating': average_rating, 'predicators_positive_top10': predicators_positive_top10_list, 'predicators_top10': predicators_top10_list}) + '\n')
```

```python
spark.stop()
```
