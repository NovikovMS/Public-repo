
<img width="50px" align="left" style="margin-right:20px" src="./images/npl_logo.png"> <b>New Professions Lab</b> <br /> Специалист по большим данным

# Выпускной Проект

# Построить content-based рекомендательную систему товаров интернет-магазина ozon.ru

<img width="200px" align="left" src="./images/ozon.png">

### Задача

Нужно построить рекомендательный алгоритм наилучшей предсказательной точности по метрике NDCG@100.

<!-- Подробнее о метрике: https://www.kaggle.com/wiki/NormalizedDiscountedCumulativeGain -->

Можно прочесть качественный пост на FastML: http://fastml.com/evaluating-recommender-systems/

Или на русском https://habr.com/company/econtenta/blog/303458/

Как и в первом проекте, мы рекомендуем сделать всё решение в этом ноутбуке. Но, в отличие от первого проекта, где всё-таки были очерчены примерные способы решения, в данном случае вам предоставляется полная свобода для трактования, исследования и выбора подходов к задаче. Более того, нет гарантий, что задача вообще решаема с необходимой точностью.

### Обработка данных на вход

Для выполнения работы вам следует взять данные из HDFS:

```bash
$ hadoop fs -ls /labs/project02 
Found 6 items
-rw-r--r--   3 hdfs hdfs   13550896 2017-04-22 15:04 /labs/project02/catalog_path
-rw-r--r--   3 hdfs hdfs  453269519 2017-04-22 15:04 /labs/project02/catalogs
-rw-r--r--   3 hdfs hdfs 8726734647 2017-04-22 15:05 /labs/project02/item_details_full
-rw-r--r--   3 hdfs hdfs    2240879 2017-04-22 15:05 /labs/project02/ozon_test.txt
-rw-r--r--   3 hdfs hdfs   19097164 2017-04-22 15:05 /labs/project02/ozon_train.txt
-rw-r--r--   3 hdfs hdfs   12906349 2017-04-22 15:05 /labs/project02/ratings
```

Итак, давайте разберёмся, какие входные данные мы имеем:

#### `ozon_train.txt`

Обучающая выборка строчки в json, где для товара `item` мы предоставляем наиболее популярные рекомендации в `true_recoms` (здесь словарь из id рекомендуемого товара и веса — чем больше, тем лучше). Веса означают клики. Текущая рекомендательная система Ozon.ru — это смесь content-based и коллаборативной фильтрации.

Пример:

```
{"item":"24798277","true_recoms":{"24798314":1,"24798279":2,"24798276":4,"24798277":1,"24798280":2}}
```

❗️ В файле есть строчка с 40 000 рекомендаций — это мусор.

#### `ozon_test.txt`

Тестовая выборка.

Пример:

```
{"item": "28759795","recoms": null}
```

#### `item_details_full`

Атрибуты товаров.

Пример:

```
{"id":"4381194","name":"Графиня де Монсоро - В двух томах - Номерованный экземпляр № 84 (подарочное издание)","annotation":"Настоящее издание отпечатано в количестве тысячи пятисот экземпляров, сто из которых изготовлены в переплетах из черной кожи с золотыми обрезами и пронумерованы.  Номер настоящего экземпляра 84.<br>\r\n\"Графиня де Монсоро\" (1846) - одно из самых значительных произведений Александра Дюма. В этом увлекательном авантюрно-историческом романе писатель с замечательным мастерством воскрешает события второй половины XVI века - эпохи религиозных войн и правления Генриха III, последнего короля династии Валуа. История трагической любви благородного графа де Бюсси и прекрасной Дианы де Монсоро развертывается на фоне придворных интриг, политических заговоров и религиозных раздоров. <br>\r\nВ настоящем издании впервые публикуются все 245 иллюстраций выдающегося французского художника Мориса Лелуара, выполненные им для парижского издания 1903 года. Книга дополнена очерком А. И. Куприна \"Дюма-отец\" и обстоятельными комментариями.","parent_id":"18255189"}
```

❗️Под `parent_id` объединяются модификации одного товара (например, разные айфоны).


#### `catalogs`

В каких каталогах лежит товар (может быть несколько записей). 

Пример:

```
{"itemid":"29040016","catalogid":"1179259"}
```

#### `catalog_path`

Пути для каталогов нижнего уровня (в которых лежат товары) в дереве каталогов. Для каждого каталога отдаётся полный путь до корня. 

Пример:

```
{"catalogid":1125630,"catalogpath":[{"1125630":"Изысканные напитки. Сигары"},{"1125623":"Книга - лучший подарок!"},{"1112250":"Архив раздела (Нехудож.лит-ра)"},{"1095865":"Нехудожественная литература"}]}
```

#### `ratings`

Средний рейтинг `itemid` (звёздочки). 

Пример:

```
{“itemid”: 2658646, “rating”:4.0}
```

### Обработка данных на выход

Выходной файл должен иметь следующий формат (пример для одной строчки). Вес товара тем выше, чем выше его близость:

```
{"item": "28759795", "recoms": {"28759801": 1, "28759817": 2, "28759803": 13}}
```

Вы можете использовать любые алгоритмы и их смеси для предсказания рейтингов. Мы будем оценивать точность работы вашего алгоритма, рассчитывая средний NDCG@100 по всем товарам. Это означает, что для каждого `item` в тестовой выборке вы рекомендуете 100 товаров.

Чекер выглядит следующим образом:

```python
lines_number_ok = False
score = 0.0

file_exists = False
test_passed = False
true_recs = {}
lines_number = 0
lines_number_hidden = 0


try:
    if not cli.startcheck():
        exit(-1)

    file_exists = cli.checkfileexists(filename)

    if file_exists:
        with open(cli.getans('test_file_path')) as f:
            for line in f:
                data = json.loads(line)
                true_recs[data['item']] = data['true_recoms']
        lines_number_hidden = len(true_recs.keys())
        
        ndcg_sum = 0
        with open(cli.getfilepath(filename)) as f:
            for line in f:
                lines_number += 1
        
        if lines_number == len(true_recs.keys()):
            lines_number_ok = True
            with open(cli.getfilepath(filename)) as f:
                for line in f:                     
                    data = json.loads(line)     
                    trs = true_recs[data['item']]
                    tmrs = data['recoms']    

                    sorted_trs = sorted(trs.items(), key = lambda x: float(x[1]), reverse = True)
                    sorted_tmrs = sorted(tmrs.items(), key = lambda x: float(x[1]), reverse = True)
                    dcg = 0
                    idcg = 0

                    for i in range(len(trs)):
                        delta = sorted_trs[i][1]
                        if i + 1 != 1:
                            delta = delta / float(math.log(i + 1, 2))
                        idcg += delta

                    for i in range(len(tmrs)):
                        if sorted_tmrs[i][0] in trs:                    
                            delta = trs[sorted_tmrs[i][0]]
                            if i + 1 != 1:
                                delta = delta / float(math.log(i + 1, 2))
                            dcg += delta

                    ndcg_sum += dcg / idcg
            score = ndcg_sum / lines_number

            if score > 0.1:
                test_passed = True

```

Ваши результаты будут заноситься на обновляемую доску лидеров на [странице Проекта 2](http://lk.newprolab.com/lab/project02).

### Проверка

Для автоматической проверки необходимо сохранить заполненный предсказанными вами рейтингами файл `ozon_test.txt` в вашей домашней директории под именем `project02.txt`.

**ВАЖНО: Для точной проверки сохраняйте порядок и количество строк исходного файла.**

<p style="color:DarkRed"><b>✅ Проект будет засчитан, если вы преодолеете порог 0.1.</b></p>

### Подсказки

1. Обратите внимание на размер датасета.

2. Не забывайте мониторить свои ресурсы: если выполняете long-running jobs, сохраняйте результаты в конце и освобождайте память. Помните, что простаивающие процессы, держащие в памяти гигабайты данных, могут отстреливаться.

3. “Когда в руках молоток, все начинает напоминать гвоздь”. Думайте outside of the box.

<img width="60px" align="left" style="margin-right:20px" src="./common/images/npl_logo.png"> <br /><b>Желаем вам удачи и успехов!</b>

## Ваше решение здесь


```python
# import os
# import sys
# os.environ["PYSPARK_PYTHON"]='/opt/anaconda/envs/bd9/bin/python'
# os.environ["SPARK_HOME"]='/usr/hdp/current/spark2-client'
# os.environ["PYSPARK_SUBMIT_ARGS"]='--num-executors 3 pyspark-shell'

# spark_home = os.environ.get('SPARK_HOME', None)

# sys.path.insert(0, os.path.join(spark_home, 'python'))
# sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))
```


```python
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, HashingTF, IDF
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
import numpy as np
import pandas as pd
import ast
import matplotlib.pyplot as plt
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from tqdm import tqdm
```


```python
spark = (SparkSession
         .builder
         .config("spark.driver.memory", "10g")
         .config("spark.executor.memory", "10g")
         .appName("Project02 misha")
         .getOrCreate()
        )
```


```python
spark
```





            <div>
                <p><b>SparkSession - in-memory</b></p>
                
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://ubuntu-advanced-16-64-60gb.mcs.local:4040">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v3.0.1</code></dd>
              <dt>Master</dt>
                <dd><code>local[*]</code></dd>
              <dt>AppName</dt>
                <dd><code>Project02 misha</code></dd>
            </dl>
        </div>
        
            </div>
        



### Читаем данные


```python
# [Row(catalogid='1139010', 
# itemid='26881367')]

catalogs_schema  = StructType([
    StructField('catalogid', StringType()),
    StructField('itemid', StringType())
])

catalogs_df = spark.read.json("./data/catalogs",
                           schema=catalogs_schema)
# catalogs.take(42)
```


```python
# [Row(item='28759795',
# recoms=None)]

ozon_test_schema = StructType([
    StructField("item", StringType()),
    StructField("recoms", MapType(StringType(), IntegerType()))
])

ozon_test = (spark
             .read.json("./data/ozon_test.txt", schema=ozon_test_schema)
             .withColumnRenamed('item', 'itemid')
            )

ozon_test.show(2)
```

    +--------+------+
    |  itemid|recoms|
    +--------+------+
    |28759795|  null|
    |32712593|  null|
    +--------+------+
    only showing top 2 rows
    



```python
! head -n 10 ./data/ozon_test.txt
```

    {"item": "28759795", "recoms": null}
    {"item": "32712593", "recoms": null}
    {"item": "29616882", "recoms": null}
    {"item": "6242189", "recoms": null}
    {"item": "32663967", "recoms": null}
    {"item": "7976823", "recoms": null}
    {"item": "32772551", "recoms": null}
    {"item": "27976812", "recoms": null}
    {"item": "32788806", "recoms": null}
    {"item": "32362592", "recoms": null}



```python
# [Row(item='31471303', 
# true_recoms={'31471373': 1, '31471307': 3, '31471342': 1})]
# ❗️ В файле есть строчка с 40 000 рекомендаций — это мусор.


ozon_train_schema = StructType([
    StructField("item", StringType()),
    StructField("true_recoms", MapType(StringType(), IntegerType()))
])

ozon_train = (spark
              .read.json("./data/ozon_train.txt", schema=ozon_train_schema)
              .rdd
              .map(lambda x: (x[0], x[1], len(x[1])))
              .toDF(StructType([
                  StructField("itemid", StringType()),
                  StructField("true_recoms", MapType(StringType(), IntegerType())), 
                  StructField("cnt", IntegerType())
                  ]))
              .filter(F.col('cnt')<40000)
              .drop('cnt')
             )

ozon_train.show()
```

    +--------+--------------------+
    |  itemid|         true_recoms|
    +--------+--------------------+
    |31471303|[31471373 -> 1, 3...|
    |31489016|[31489022 -> 1, 3...|
    | 5629613|[19063889 -> 14, ...|
    |24829740|[32610140 -> 1, 2...|
    |29185962|[1493930 -> 1, 33...|
    |20063162|[21458776 -> 1, 6...|
    |30232057|[30232031 -> 1, 3...|
    |32966349|[26221282 -> 1, 3...|
    |21199341|[24326908 -> 6, 2...|
    |20896456|[21206232 -> 5, 2...|
    | 1149434|[3867974 -> 2, 31...|
    | 4779816|[4938380 -> 5, 47...|
    |31375526|[31379185 -> 3, 3...|
    |24744311|[24744312 -> 1, 2...|
    |24797999|[21207734 -> 1, 2...|
    |33039301|[33039287 -> 11, ...|
    |32663516|[31423287 -> 1, 3...|
    |17579560|[3836618 -> 1, 18...|
    | 5549423|[5549424 -> 1, 17...|
    | 4473202|[24324351 -> 1, 2...|
    +--------+--------------------+
    only showing top 20 rows
    


### Готовим данные 


```python
%%time
# ozone_train сделали эксплод, к типу |itemid|rec_itemid|clicks|
train_exploded_df = (ozon_train
                     .select(
                         'itemid',
                         F.explode_outer('true_recoms')
                     )
                     .withColumnRenamed('key', 'rec_itemid')
                     .withColumnRenamed('value', 'clicks')
                    )

train_exploded_df.show(2, truncate=False)
```

    +--------+----------+------+
    |itemid  |rec_itemid|clicks|
    +--------+----------+------+
    |31471303|31471373  |1     |
    |31471303|31471307  |3     |
    +--------+----------+------+
    only showing top 2 rows
    
    CPU times: user 5.26 ms, sys: 0 ns, total: 5.26 ms
    Wall time: 581 ms



```python
# 28759795 из тест сета
train_exploded_df.filter(F.col('itemid')==28759795).show()
```

    +------+----------+------+
    |itemid|rec_itemid|clicks|
    +------+----------+------+
    +------+----------+------+
    



```python
# Чекер, 28759795 из тест сета
train_exploded_df.filter(F.col('rec_itemid')==28759795).show()
```

    +--------+----------+------+
    |  itemid|rec_itemid|clicks|
    +--------+----------+------+
    |28759796|  28759795|     1|
    |28759803|  28759795|     2|
    |31594265|  28759795|     1|
    |28759792|  28759795|     2|
    +--------+----------+------+
    



```python
# а если нам не хватает взиамности/симметрии в отношении переходов, без этого не собирается пазл...
train_df = (train_exploded_df
            .select('itemid', 'rec_itemid')
            .union(
                train_exploded_df
                .select(
                    F.col('rec_itemid').alias('itemid'),
                    F.col('itemid').alias('rec_itemid')
                )
            )
           )

```

Данных 9 гб. Времени просчитать 1 неделя. 
Считать через: 
cossim по котексту c дроблением ~ 14 дней,
ALS подбор параметров на итерацию с дроблением на батчи ~ 1 час 40 мин, подбирать примерно 5 дней или больше...
Самые быстрые варианты Pagerank или Graph ~ 10 мин.

Выберем симметричный граф для начального приближения. Т.к. структура очень похожа на соцсеть...


```python
# собираем группы листами после эксплода имеющих больше одного общего соседа
items_with_common_reference_df = (
    train_exploded_df
    .select(F.col('itemid').cast('integer'),
         F.col('rec_itemid').cast('integer')
        )
    .groupBy("itemid")
    .agg(F.collect_list("rec_itemid").alias("items_with_common_reference"))
    .select("items_with_common_reference")
    .where(F.size(F.col("items_with_common_reference")) >= 2)
    .select(F.sort_array("items_with_common_reference").alias("sorted_items_with_common_reference"))
    .drop("items_with_common_reference")        
    )

items_with_common_reference_df.show(20, truncate=200)    

```

    +------------------------------------------------------------------+
    |                                sorted_items_with_common_reference|
    +------------------------------------------------------------------+
    |                                    [73933, 76069, 846911, 853379]|
    |                                       [117436, 4949484, 31465642]|
    |                                        [136933, 1605683, 2720033]|
    |                                     [5045541, 33137562, 33229505]|
    |                               [154741, 1367530, 1403385, 1467197]|
    |                                       [1804644, 2457681, 3562100]|
    |                              [2350009, 4418562, 6161700, 8390787]|
    |                             [2886910, 7578904, 8757239, 19433918]|
    |                    [1717782, 3994785, 4319859, 5793291, 32273885]|
    |                                    [19155967, 19437869, 20023681]|
    |                                      [1477804, 3204253, 19374726]|
    |                                      [3239644, 6266905, 20335253]|
    |            [2497953, 3694436, 4721165, 5955256, 7228575, 8141475]|
    |                                       [2423464, 4519353, 8377826]|
    |                                    [26312061, 31421681, 31617239]|
    |                             [1704867, 3935627, 4467399, 26969197]|
    |                                       [5028553, 5406428, 7419807]|
    |                             [3043490, 3429937, 7609271, 18097317]|
    |                                      [2667108, 5146697, 19434083]|
    |[4915865, 4951264, 5636830, 6243906, 19683778, 24219253, 32091726]|
    +------------------------------------------------------------------+
    only showing top 20 rows
    



```python
# чекер на наши таргеты есть 4 группы - круто
(items_with_common_reference_df
 .select(
 F.explode('sorted_items_with_common_reference').alias('sorted_items_with_common_reference')
     )
 .filter(F.col('sorted_items_with_common_reference') == 28759795)
#  .filter(F.col('sorted_items_with_common_reference').isin(itemsToPredict_list))
 .show())
```

    +----------------------------------+
    |sorted_items_with_common_reference|
    +----------------------------------+
    |                          28759795|
    |                          28759795|
    |                          28759795|
    |                          28759795|
    +----------------------------------+
    



```python
# собираем список итемов в лист, для которых требуется предсказание
itemsToPredict_list = (ozon_test
                  .select(F.col("itemid").cast("integer"))
                  .rdd
                  .map(lambda x : x[0])
                  .collect()
                 )

itemsToPredictBC = spark.sparkContext.broadcast(set(itemsToPredict_list))
itemsToPredict_list[0:10]
```




    [28759795,
     32712593,
     29616882,
     6242189,
     32663967,
     7976823,
     32772551,
     27976812,
     32788806,
     32362592]




```python
# делаем функцию где собираем треуголную матрицу возможных пар соседей (сосед-сосед) с нашими таргетами
def pairs_with_common_reference(list_of_items):
    pairs = []
    length = len(list_of_items)
    # для каждого от 0 до последнего - 1        
    for item_1_id in range(0, length):
        # для каждого от 1 до последнего
        for item_2_id in range(item_1_id + 1, length):
            # для не одинаковых item_id
            if item_1_id != item_2_id:
                # для item_id в нашем таргете
                if (list_of_items[item_1_id] in itemsToPredictBC.value or 
                    list_of_items[item_2_id] in itemsToPredictBC.value):
                    # для item_id в нашем таргете
                    pairs.append((list_of_items[item_1_id], 
                                  list_of_items[item_2_id])
                                )
    return pairs
        
schema = ArrayType(ArrayType(IntegerType()))    
    
pairs_with_common_reference_UDF = F.udf(pairs_with_common_reference, schema)
```


```python
# примениил функцию и посчитали пары соседей, смотрим пары
common_reference_counts = (items_with_common_reference_df
                           .select(pairs_with_common_reference_UDF('sorted_items_with_common_reference').alias("pairs_with_common_reference"))
                           .where(F.size(F.col("pairs_with_common_reference")) > 0)
                          )    
common_reference_counts.show(20, truncate=100)
```

    +----------------------------------------------------------------------------------------------------+
    |                                                                         pairs_with_common_reference|
    +----------------------------------------------------------------------------------------------------+
    |                                                  [[73933, 76069], [73933, 846911], [73933, 853379]]|
    |                                                             [[136933, 1605683], [1605683, 2720033]]|
    |                                                           [[3239644, 6266905], [3239644, 20335253]]|
    |                                                           [[2667108, 5146697], [2667108, 19434083]]|
    |[[4915865, 19683778], [4951264, 19683778], [5636830, 19683778], [6243906, 19683778], [19683778, 2...|
    |                                        [[3040732, 3318564], [3040732, 3338819], [3318564, 3338819]]|
    |                                                         [[2684063, 18477296], [18477296, 18477306]]|
    |                                       [[5009128, 7625350], [5009128, 8254804], [5009128, 20276591]]|
    |[[2389223, 6108427], [2681717, 6108427], [4283505, 6108427], [4287256, 6108427], [6108427, 156174...|
    |[[1515542, 4103762], [4073706, 4103762], [4073854, 4103762], [4073855, 4103762], [4103762, 427977...|
    |                                        [[3907194, 5511990], [3938305, 5511990], [4648908, 5511990]]|
    |[[2500590, 4190397], [4190397, 6253299], [4190397, 7624245], [4190397, 8208890], [4190397, 207426...|
    |                                                          [[4174804, 18649470], [5280178, 18649470]]|
    |[[9595891, 9596439], [9595891, 17579560], [9595891, 18045327], [9596439, 17579560], [9596439, 180...|
    |[[1494025, 19863711], [2458182, 19863711], [5676581, 19863711], [6082543, 19863711], [8740813, 19...|
    |[[5386856, 5386890], [5386856, 5386894], [5386856, 5386987], [5386856, 5608956], [5386856, 560896...|
    |                                        [[3290202, 5301371], [4209694, 5301371], [5301371, 7366488]]|
    |                                                          [[1524537, 21238925], [4073084, 21238925]]|
    |[[3301008, 5629860], [3301008, 20001268], [4539777, 5629860], [4539777, 20001268], [4694251, 5629...|
    |                                                           [[5046919, 5434741], [5046919, 21450045]]|
    +----------------------------------------------------------------------------------------------------+
    only showing top 20 rows
    



```python
# пара итемов и их счетчик их повторений по датасету
target = (common_reference_counts
     .withColumn("pair_with_common_reference", F.explode("pairs_with_common_reference"))
     .drop(F.col("pairs_with_common_reference"))
     .groupBy(F.col("pair_with_common_reference"))
     .count()
    )

target.show(20, truncate=100)
```

    +--------------------------+-----+
    |pair_with_common_reference|count|
    +--------------------------+-----+
    |        [5608956, 5608964]|    1|
    |        [6299919, 7597192]|    1|
    |       [7279123, 23896153]|    1|
    |      [19126405, 19126464]|    1|
    |      [18777684, 18777707]|    1|
    |      [27711801, 28840211]|    2|
    |      [19158550, 22443578]|    3|
    |      [20312682, 20896103]|    3|
    |      [20393919, 20393988]|    2|
    |      [21211828, 25632037]|    1|
    |       [5409862, 27986616]|    5|
    |      [26467134, 32498498]|    1|
    |       [4961527, 27915134]|    1|
    |      [18518944, 29739007]|    1|
    |      [29174703, 29898655]|    1|
    |      [29276612, 32486302]|    1|
    |      [18424153, 31083734]|    1|
    |      [24916405, 32654829]|    1|
    |      [24931171, 32611294]|    1|
    |      [28345971, 30708407]|    1|
    +--------------------------+-----+
    only showing top 20 rows
    



```python
target.count() # 1,3кк матрицу с такой стороной будет очень тяжело посчитать в обычном ML
```




    1291846




```python
# собрали дф с соседями, максимум пар 26, дополнительно не пишем окно для ограничения числа
target_df = (target             
             .select(
                 target.pair_with_common_reference[0].alias('item_1_id'),
                 target.pair_with_common_reference[1].alias('item_2_id'),
                 'count'
                    )
             .orderBy(F.col('count').desc())
             )
target_df.show(20)
```

    +---------+---------+-----+
    |item_1_id|item_2_id|count|
    +---------+---------+-----+
    | 33057866| 33191212|   26|
    | 33191005| 33191212|   26|
    | 32871593| 33191005|   26|
    | 32103777| 33191212|   25|
    | 32871593| 33191212|   25|
    | 33057866| 33191005|   25|
    | 31258986| 31258987|   25|
    | 33191212| 33405980|   25|
    | 32538709| 33191212|   24|
    | 31258987| 31668990|   24|
    | 32167310| 32871593|   24|
    | 32871593| 32934171|   24|
    | 27961959| 31619658|   24|
    | 32167310| 33191212|   24|
    | 33191212| 33255573|   24|
    | 33191005| 33405980|   24|
    | 27766016| 29202403|   24|
    | 33191212| 33300839|   24|
    | 28006379| 28006396|   23|
    | 29091413| 33191212|   23|
    +---------+---------+-----+
    only showing top 20 rows
    



```python
# смапили соседей + счетчик ближайших
target_df_mapped = (target_df_topn
                     .groupby("item_1_id")
                     .agg(F.map_from_arrays(F.collect_list("item_2_id"),F.collect_list("count")).alias("recoms"))
                    )

target_df_mapped.show(10, truncate=100)
```

    +---------+----------------------------------------------------------------------------------------------------+
    |item_1_id|                                                                                              recoms|
    +---------+----------------------------------------------------------------------------------------------------+
    |    73933|[157266 -> 1, 846911 -> 1, 75719 -> 1, 75531 -> 1, 154430 -> 1, 75991 -> 1, 76239 -> 1, 74287 -> ...|
    |    77234|                                                                        [4037290 -> 1, 2906836 -> 1]|
    |   117437|                                                                                     [31333005 -> 1]|
    |   133018|                                                                       [33373690 -> 1, 6840573 -> 1]|
    |   140266|                                                                       [30681471 -> 1, 8535800 -> 1]|
    |   154202|                                                                        [2263148 -> 1, 1605683 -> 1]|
    |   158257|          [28062291 -> 1, 25908412 -> 1, 26312264 -> 1, 30262708 -> 1, 32872216 -> 1, 29630360 -> 1]|
    |   158593|                                                                                     [27883343 -> 1]|
    |  1012902|[2650597 -> 3, 4295162 -> 3, 3078298 -> 3, 19385109 -> 2, 23041582 -> 2, 23996149 -> 2, 4172526 -...|
    |  1307463|                                                                                      [5403014 -> 1]|
    +---------+----------------------------------------------------------------------------------------------------+
    only showing top 10 rows
    



```python
ozon_test.printSchema()
```

    root
     |-- itemid: string (nullable = true)
     |-- recoms: map (nullable = true)
     |    |-- key: string
     |    |-- value: integer (valueContainsNull = true)
    



```python
# к ozon_test приджойнили результаты и убрали null
output_df = (ozon_test
             .alias('a')
             .join(target_df_mapped.alias('b'), F.col('a.itemid') == F.col('b.item_1_id'), 'left')
             .select(
             F.col('a.itemid').alias('item'),
             F.col('b.recoms').alias('recoms')
             )
             .rdd
             .map(lambda x: (x['item'], x['recoms'] or {}))
             .toDF(ozon_test.schema)
             .withColumnRenamed('itemid', 'item')
            )

output_df.show(10, truncate=100)
```

    +-------+----------------------------------------------------------------------------------------------------+
    |   item|                                                                                              recoms|
    +-------+----------------------------------------------------------------------------------------------------+
    |  73933|[157953 -> 1, 76069 -> 1, 853288 -> 1, 75719 -> 1, 75531 -> 1, 853379 -> 1, 74287 -> 1, 76239 -> ...|
    | 119517|                                                                                                  []|
    |1012902|[28193325 -> 1, 27809323 -> 1, 27881263 -> 1, 32114028 -> 1, 30227146 -> 1, 27960992 -> 1, 332650...|
    |1353799|                                                                                                  []|
    |1486867|                                                                        [2639932 -> 1, 2428047 -> 1]|
    |1562184|                                                                                                  []|
    |1594277|                                           [2448101 -> 1, 4181179 -> 1, 17434367 -> 1, 1941875 -> 1]|
    |1691352|                                                                        [5539281 -> 1, 7697260 -> 1]|
    |2226450|[3040876 -> 1, 3040877 -> 1, 3040875 -> 1, 5103995 -> 1, 3040872 -> 1, 2263992 -> 1, 2425935 -> 1...|
    |2312581|                                            [3964092 -> 1, 2660237 -> 1, 2660239 -> 1, 2886851 -> 1]|
    +-------+----------------------------------------------------------------------------------------------------+
    only showing top 10 rows
    



```python
output_df.count()
```




    60956




```python
ozon_test.count()
```




    60956




```python
output_df.toPandas().to_json('project02.txt', orient='records', lines=True)
```


```python
!head -n 1 project02.txt
```

    {"item":"73933","recoms":{"154430":1,"157953":1,"75531":1,"74287":1,"853334":1,"853379":1,"853288":1,"75292":1,"75991":1,"76069":1,"77302":1,"76239":1,"846911":1,"157266":1,"75719":1}}



```python
!head -n 1 ./data/ozon_train.txt
```

    {"item": "31471303", "true_recoms": {"31471373": 1, "31471307": 3, "31471342": 1}}



```python
!head -n 1 ./data/ozon_test.txt
```

    {"item": "28759795", "recoms": null}



```python
## Грузим результат в чекер.   
## Graph дал 0,12%  при 0,0005 сек/ит. 
## Быстро. Задание выполненио малой кровью.
```


```python
## Другие результаты с потока у людей
## ALS дал 0,19% при 0,006 сек/ит
## cossim по контексту дал 0,31% при 1,368 сек/ит
```


```python
spark.stop()
```
