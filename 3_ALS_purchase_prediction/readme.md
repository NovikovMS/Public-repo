## Построить рекомендательную систему видеоконтента с implicit feedback

<img width="350px" src="images/megafon_logo.jpg">

##### Задание создано при поддержке компании «МегаФон».

### Задача

В вашем распоряжении имеется уже предобработанный и очищенный датасет с фактами
покупок абонентами телепередач от компании E-Contenta. По доступным вам данным нужно предсказать вероятность покупки других передач этими, а, возможно, и другими абонентами.

### Обработка данных на вход

Для выполнения работы вам следует взять все файлы из папки на HDFS `/labs/lab10data/`. Давайте посмотрим, что у нас есть:

```
$ hadoop fs -ls /labs/lab10data
Found 4 items
-rw-r--r--   3 hdfs hdfs   91066524 2017-05-09 13:51 /labs/lab10data/lab10_items.csv
-rw-r--r--   3 hdfs hdfs   29965581 2017-05-09 13:50 /labs/lab10data/lab10_test.csv
-rw-r--r--   3 hdfs hdfs   74949368 2017-05-09 13:50 /labs/lab10data/lab10_train.csv
-rw-r--r--   3 hdfs hdfs  871302535 2017-05-09 13:51 /labs/lab10data/lab10_views_programmes.csv
```

- В `lab10_train.csv` содержатся факты покупки (колонка `purchase`) пользователями (колонка `user_id`) телепередач (колонка `item_id`). Такой формат файла вам уже знаком.

- `lab10_items.csv` — дополнительные данные по items. В данном файле много лишней или ненужной информации, так что задача её фильтрации и отбора ложится на вас. Поля в файле, на которых хотелось бы остановиться:
  - `item_id` — primary key. Соответствует `item_id` в предыдущем файле.
  - `content_type` — тип телепередачи (`1` — платная, `0` — бесплатная). Вас интересуют платные передачи.
  - `title` — название передачи, текстовое поле.
  - `year` — год выпуска передачи, число.
  - `genres` — поле с жанрами передачи, разделёнными через запятую.
- `lab10_test.csv` — тестовый датасет без указанного целевого признака `purchase`, который вам и предстоит предсказать.
- Дополнительный файл `lab10_views_programmes.csv` по просмотрам передач с полями:
  - `ts_start` — время начала просмотра
  - `ts_end` — время окончания просмотра
  - `item_type`— тип просматриваемого контента:
    - `live` — просмотр "вживую", в момент показа контента в эфире
    - `pvr` — просмотр в записи, после показа контента в эфире

### Обработка данных на выход

Предсказание целевой переменной "купит/не купит". Поскольку нам важны именно вероятности отнесения пары `(пользователь, товар)` к классу "купит" (`1`), то, на самом деле, вы можете подойти к проблеме с разных сторон:

1. Как к разработке рекомендательной системы: рекомендовать пользователю `user_id` топ-N лучших телепередач, которые были найдены по методике user-user / item-item коллаборативной фильтрации.
2. Как к задаче факторизации матриц: алгоритмы SVD, ALS, FM/FFM.
3. Как просто к задаче бинарной классификации. У вас есть два датасета, которые можно каким-то образом объединить, дополнительно обработать и сделать предсказания классификаторами (Apache Spark, pandas + sklearn на ваше усмотрение).
4. Как к задаче регрессии. Поскольку от вас требуется предсказать не факт покупки, а его _вероятность_, то можно перевести задачу в регрессионную и решать её соответствующим образом.

### Проверка

Мы будем оценивать ваш алгоритм по метрике ROC AUC. Ещё раз напомним, что чекеру требуются _вероятности_ в диапазоне `[0.0, 1.0]` отнесения пары `(пользователь, товар)` в тестовой выборке к классу "1" (купит).

Для успешного прохождения лабораторной работы **AUC должен составить не менее 0.6**.

### Решение

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
from pyspark.sql.types import *
import json
```


```python
conf = SparkConf()

spark = (SparkSession
         .builder
         .config(conf=conf)
         .appName("ALS purchase prediction")
         .getOrCreate())
```


```python
spark
```


```python


from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, HashingTF, IDF
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
import numpy as np
import pandas as pd
import ast
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from tqdm import tqdm
```

#### Читаем данные 


```python
# lab10_items.csv — дополнительные данные по items. В данном файле много лишней или ненужной 
# информации, так что задача её фильтрации и отбора ложится на вас. Поля в файле, на которых 
# хотелось бы остановиться:

#     item_id — primary key. Соответствует item_id в предыдущем файле.
#     content_type — тип телепередачи (1 — платная, 0 — бесплатная). Вас интересуют платные передачи.
#     title — название передачи, текстовое поле.
#     year — год выпуска передачи, число.
#     genres — поле с жанрами передачи, разделёнными через запятую.



items = spark.read.csv('/labs/lab10data/lab10_items.csv', header=True, sep='\t')
               
items.limit(5).toPandas()
# items.show(2, vertical=True, truncate=False)
```



<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item_id</th>
      <th>channel_id</th>
      <th>datetime_availability_start</th>
      <th>datetime_availability_stop</th>
      <th>datetime_show_start</th>
      <th>datetime_show_stop</th>
      <th>content_type</th>
      <th>title</th>
      <th>year</th>
      <th>genres</th>
      <th>region_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>65667</td>
      <td>None</td>
      <td>1970-01-01T00:00:00Z</td>
      <td>2018-01-01T00:00:00Z</td>
      <td>None</td>
      <td>None</td>
      <td>1</td>
      <td>на пробах только девушки (all girl auditions)</td>
      <td>2013.0</td>
      <td>Эротика</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>65669</td>
      <td>None</td>
      <td>1970-01-01T00:00:00Z</td>
      <td>2018-01-01T00:00:00Z</td>
      <td>None</td>
      <td>None</td>
      <td>1</td>
      <td>скуби ду: эротическая пародия (scooby doo: a x...</td>
      <td>2011.0</td>
      <td>Эротика</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>65668</td>
      <td>None</td>
      <td>1970-01-01T00:00:00Z</td>
      <td>2018-01-01T00:00:00Z</td>
      <td>None</td>
      <td>None</td>
      <td>1</td>
      <td>горячие девочки для горячих девочек (hot babes...</td>
      <td>2011.0</td>
      <td>Эротика</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>65671</td>
      <td>None</td>
      <td>1970-01-01T00:00:00Z</td>
      <td>2018-01-01T00:00:00Z</td>
      <td>None</td>
      <td>None</td>
      <td>1</td>
      <td>соблазнительницы женатых мужчин (top heavy hom...</td>
      <td>2011.0</td>
      <td>Эротика</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>65670</td>
      <td>None</td>
      <td>1970-01-01T00:00:00Z</td>
      <td>2018-01-01T00:00:00Z</td>
      <td>None</td>
      <td>None</td>
      <td>1</td>
      <td>секретные секс-материалы ii: темная секс парод...</td>
      <td>2010.0</td>
      <td>Эротика</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
items.registerTempTable("items")
spark.sql("""
select
    count(distinct item_id) as item_id,
    count(distinct channel_id) as channel_id,
    count(distinct datetime_availability_start) as datetime_availability_start,
    count(distinct datetime_availability_stop) as datetime_availability_stop,
    count(distinct datetime_show_start) as datetime_show_start,
    count(distinct datetime_show_stop) as datetime_show_stop,
    count(distinct content_type) as content_type,
    count(distinct title) as title,
    count(distinct year) as year,
    count(distinct genres) as genres,
    count(distinct region_id) as region_id,
    count(*) as interactions    
from
    items
""").toPandas()
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item_id</th>
      <th>channel_id</th>
      <th>datetime_availability_start</th>
      <th>datetime_availability_stop</th>
      <th>datetime_show_start</th>
      <th>datetime_show_stop</th>
      <th>content_type</th>
      <th>title</th>
      <th>year</th>
      <th>genres</th>
      <th>region_id</th>
      <th>interactions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>635568</td>
      <td>207</td>
      <td>2</td>
      <td>44071</td>
      <td>43070</td>
      <td>43356</td>
      <td>2</td>
      <td>23678</td>
      <td>80</td>
      <td>1076</td>
      <td>18</td>
      <td>635568</td>
    </tr>
  </tbody>
</table>
</div>




```python
# lab10_test.csv — тестовый датасет без указанного целевого признака purchase, который вам и 
# предстоит предсказать.

test = spark.read.csv('/labs/lab10data/lab10_test.csv', header=True)
               
test.limit(5).toPandas()
# items.show(2, vertical=True, truncate=False)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>item_id</th>
      <th>purchase</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1654</td>
      <td>94814</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1654</td>
      <td>93629</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1654</td>
      <td>9980</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1654</td>
      <td>95099</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1654</td>
      <td>11265</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
test.registerTempTable("test")
spark.sql("""
select
    count(distinct user_id) as user_id,
    count(distinct item_id) as item_id,
    count(*) as interactions    
from
    test
""").toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>item_id</th>
      <th>interactions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1941</td>
      <td>3704</td>
      <td>2156840</td>
    </tr>
  </tbody>
</table>
</div>




```python
# В lab10_train.csv содержатся факты 
# покупки (колонка purchase) 
# пользователями (колонка user_id) 
# телепередач (колонка item_id). 
# Такой формат файла вам уже знаком.

train = spark.read.csv('/labs/lab10data/lab10_train.csv', header=True)
               
train.limit(5).toPandas()
# items.show(2, vertical=True, truncate=False)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>item_id</th>
      <th>purchase</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1654</td>
      <td>74107</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1654</td>
      <td>89249</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1654</td>
      <td>99982</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1654</td>
      <td>89901</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1654</td>
      <td>100504</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
train.registerTempTable("train")
spark.sql("""
select
    count(distinct user_id) as user_id,
    count(distinct item_id) as item_id,
    count(*) as interactions    
from
    train
""").toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>item_id</th>
      <th>interactions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1941</td>
      <td>3704</td>
      <td>5032624</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Дополнительный файл lab10_views_programmes.csv по просмотрам передач с полями:

#     ts_start — время начала просмотра
#     ts_end — время окончания просмотра
#     item_type— тип просматриваемого контента:
#         live — просмотр "вживую", в момент показа контента в эфире
#         pvr — просмотр в записи, после показа контента в эфире



views = spark.read.csv('/labs/lab10data/lab10_views_programmes.csv', header=True)
               
views.limit(5).toPandas()
# items.show(2, vertical=True, truncate=False)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>item_id</th>
      <th>ts_start</th>
      <th>ts_end</th>
      <th>item_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>818606</td>
      <td>6739803</td>
      <td>1489489378</td>
      <td>1489494212</td>
      <td>live</td>
    </tr>
    <tr>
      <th>1</th>
      <td>818606</td>
      <td>6271248</td>
      <td>1486459714</td>
      <td>1486460684</td>
      <td>live</td>
    </tr>
    <tr>
      <th>2</th>
      <td>818606</td>
      <td>6963181</td>
      <td>1490737862</td>
      <td>1490752561</td>
      <td>live</td>
    </tr>
    <tr>
      <th>3</th>
      <td>818606</td>
      <td>7371187</td>
      <td>1492854851</td>
      <td>1492855995</td>
      <td>live</td>
    </tr>
    <tr>
      <th>4</th>
      <td>818606</td>
      <td>6538677</td>
      <td>1488209891</td>
      <td>1488211584</td>
      <td>live</td>
    </tr>
  </tbody>
</table>
</div>




```python
views.registerTempTable("views")
spark.sql("""
select
    count(distinct user_id) as user_id,
    count(distinct item_id) as item_id,
    count(distinct ts_start) as ts_start,
    count(distinct ts_end) as ts_end,
    count(distinct item_type) as item_type,
    count(*) as interactions    
from
    views
""").toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>item_id</th>
      <th>ts_start</th>
      <th>ts_end</th>
      <th>item_type</th>
      <th>interactions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>79385</td>
      <td>633840</td>
      <td>5126018</td>
      <td>5106233</td>
      <td>2</td>
      <td>20845607</td>
    </tr>
  </tbody>
</table>
</div>



#### Оцениваем предыдущий подбор параметров


```python
als_model_results = pd.read_csv('lab10_als_cv.csv')
```


```python
sns.lineplot(data=als_model_results, x='rank', y='rmse', hue='regParam')
plt.show()
```

#### ALS


```python
print('Тестовыe user_id рекомендовали в учебном сете : ' + 
      str(train.groupBy('user_id').count().alias('a')
          .join(test.groupBy('user_id').count().alias('b'), F.col('a.user_id')==F.col('b.user_id'), how='inner').count()) + 
      ' из ' + 
      str(train.groupBy('user_id').count().count()) + 
      '. Всего записей: ' + 
      str(train.count()))

print('Было рекомендовано тестовых item_id в учебном сете : ' + 
      str(train.groupBy('item_id').count().alias('a')
          .join(test.groupBy('item_id').count().alias('b'), F.col('a.item_id')==F.col('b.item_id'), how='inner').count()) + 
      ' из ' + 
      str(train.groupBy('item_id').count().count()) + 
      '. Всего записей: ' + 
      str(train.count()))
```

    Тестовыe user_id рекомендовали в учебном сете : 1941 из 1941. Всего записей: 5032624
    Было рекомендовано тестовых item_id в учебном сете : 3704 из 3704. Всего записей: 5032624



```python
# chk_train, chk_val = train_als.randomSplit([0.8, 0.2], seed=42)
```


```python
%%time
train_als = (train
               .select(
                   F.col('user_id').cast('integer').alias('user_id'),
                   F.col('item_id').cast('integer').alias('item_id'),
                   F.col('purchase').cast('float').alias('purchase')
               ).dropna()
             
              )
train_als.show(5)
```

    +-------+-------+--------+
    |user_id|item_id|purchase|
    +-------+-------+--------+
    |   1654|  74107|     0.0|
    |   1654|  89249|     0.0|
    |   1654|  99982|     0.0|
    |   1654|  89901|     0.0|
    |   1654| 100504|     0.0|
    +-------+-------+--------+
    only showing top 5 rows
    
    CPU times: user 5.87 ms, sys: 394 µs, total: 6.26 ms
    Wall time: 212 ms



```python
train_als.printSchema()
```

    root
     |-- user_id: integer (nullable = true)
     |-- item_id: integer (nullable = true)
     |-- purchase: float (nullable = true)
    



```python
%%time
test_als = (test
               .select(
                   F.col('user_id').cast('integer').alias('user_id'),
                   F.col('item_id').cast('integer').alias('item_id')
               )
              )
test_als.show(5)
```

    +-------+-------+
    |user_id|item_id|
    +-------+-------+
    |   1654|  94814|
    |   1654|  93629|
    |   1654|   9980|
    |   1654|  95099|
    |   1654|  11265|
    +-------+-------+
    only showing top 5 rows
    
    CPU times: user 286 µs, sys: 4.22 ms, total: 4.5 ms
    Wall time: 136 ms



```python
%%time
# учим ml.ALS
als = ALS(userCol='user_id',
          itemCol='item_id',
          ratingCol='purchase', 
          implicitPrefs=True, 
          numUserBlocks=16, 
          numItemBlocks=16,
          maxIter=20,
          seed=42,
          nonnegative=True)

from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator

grid = (ParamGridBuilder()
        .addGrid(als.rank, [103]) ## Уже подобрали оптимум
        .addGrid(als.regParam, [.005]) ## Уже подобрали оптимум
        .build()
       )

evaluator = RegressionEvaluator(metricName="rmse", 
                                labelCol="purchase", 
                                predictionCol="prediction")

cv = CrossValidator(estimator=als,
                    estimatorParamMaps=grid,
                    evaluator=evaluator,
                    numFolds=5
                   )


als_model = cv.fit(train_als)
```

Подбирали по RMSE, т.к. RegressionEvaluator не работает с ROC

    CPU times: user 594 ms, sys: 174 ms, total: 767 ms
    Wall time: 6min 53s



```python
%%time
# собираем параметры для следующего подбора
als_model_params = [{p.name: v for p, v in m.items()} for m in als_model.getEstimatorParamMaps()]

als_model_results = pd.DataFrame.from_dict([{als_model.getEvaluator().getMetricName(): metric, **ps}
                                            for ps, metric in zip(als_model_params, als_model.avgMetrics)
                                           ]
                                          )\
                      .sort_values(by='rmse', ascending=False)
als_model_results.to_csv('lab10_als_cv.csv')
als_model_results.head(20)
```

    CPU times: user 16 ms, sys: 0 ns, total: 16 ms
    Wall time: 15.5 ms



```python
sns.lineplot(data=als_model_results, x='rank', y='rmse', hue='regParam')
plt.show()
```


```python
# посчитали ROC_AUC
chk_val_als = (als_model.bestModel.transform(train_als)).toPandas()

from sklearn.metrics import roc_auc_score
roc_auc_score(chk_val_als.purchase, chk_val_als.prediction)
```




    0.9933840201174169



#### На скрытом датасете результат 0,89 что тоже хорошо.


```python
# als_model.save(MAIN_PATH + 'model_rank_120_iter_20.mdl')  
```


```python
# als_model = ALS_Model.read().load(MAIN_PATH + 'model_rank_120_iter_20.mdl')
```


```python
%%time

# просто обрезаем верхи больше 1
def to_range(x, r):
    if x > r[1]:
        x = r[1]
#     elif x < r[0]:
#         x = r[0]
    return x

preds_als = (als_model.bestModel.transform(test_als)
             .rdd
             .map(lambda x: (x['user_id'], x['item_id'], to_range(x['prediction'], [0.0,1.0])))
             .toDF(['user_id', 'item_id', 'purchase'])
             .select(F.col('user_id').cast('integer'), 
                     F.col('item_id').cast('integer'), 
                     F.col('purchase').cast('float'))
             .na.fill(0)
             .orderBy(F.col('user_id').asc(), F.col('item_id').asc())
            )
            
preds_als.show(20)
```

    +-------+-------+------------+
    |user_id|item_id|    purchase|
    +-------+-------+------------+
    |   1654|    336|         0.0|
    |   1654|    678|         0.0|
    |   1654|    691|         0.0|
    |   1654|    696| 6.175371E-4|
    |   1654|    763| 8.159396E-4|
    |   1654|    795|0.0016494164|
    |   1654|    861|         0.0|
    |   1654|   1137|         0.0|
    |   1654|   1159|         0.0|
    |   1654|   1428| 0.001444104|
    |   1654|   1685|0.0014418728|
    |   1654|   1686|         0.0|
    |   1654|   1704|9.2851755E-4|
    |   1654|   2093|         0.0|
    |   1654|   2343|         0.0|
    |   1654|   2451|         0.0|
    |   1654|   2469| 0.028444147|
    |   1654|   2603|         0.0|
    |   1654|   2609|         0.0|
    |   1654|   2621|0.0033966978|
    +-------+-------+------------+
    only showing top 20 rows
    
    CPU times: user 89.7 ms, sys: 16.1 ms, total: 106 ms
    Wall time: 1min 4s



```python
preds_als = preds_als.toPandas()
```


```python
test_als.count() - preds_als.shape[0]
```


#### Пишем результат в файл


```python
%%time

preds_als.to_csv('ALS_purchase_prediction.csv', index=False)
```

    CPU times: user 8.95 s, sys: 59.6 ms, total: 9.01 s
    Wall time: 9.03 s



```python
spark.stop()
```
