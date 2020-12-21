

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
from pyspark.sql.functions import col, pandas_udf, split, lower, udf
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructType, StructField, IntegerType, StringType, DoubleType
```


```python
conf = SparkConf()

spark = SparkSession.builder.config(conf=conf).appName("Spark cossim text books").getOrCreate()
```


```python
spark
```


## Постановка задачи
Имея текстовые статьи найти 10 похожих по содержанию для
courses_list = [16871, 12234, 9029, 18030, 8170, 1200]


## Грузим датасеты


```python
# наш датасет в HDFS 66.3 мб
!hdfs dfs -ls -h /labs/lab07data/DO_record_per_line.json
```

    -rw-r--r--   2 hdfs hdfs     66.3 M 2020-09-30 12:22 /labs/lab07data/DO_record_per_line.json



```python
!hdfs dfs -cat /labs/lab07data/DO_record_per_line.json | head -n1
```

    {"lang": "en", "name": "Accounting Cycle: The Foundation of Business Measurement and Reporting", "cat": "3/business_management|6/economics_finance", "provider": "Canvas Network", "id": 4, "desc": "This course introduces the basic financial statements used by most businesses, as well as the essential tools used to prepare them. This course will serve as a resource to help business students succeed in their upcoming university-level accounting classes, and as a refresher for upper division accounting students who are struggling to recall elementary concepts essential to more advanced accounting topics. Business owners will also benefit from this class by gaining essential skills necessary to organize and manage information pertinent to operating their business. At the conclusion of the class, students will understand the balance sheet, income statement, and cash flow statement. They will be able to differentiate between cash basis and accrual basis techniques, and know when each is appropriate. They\u2019ll also understand the accounting equation, how to journalize and post transactions, how to adjust and close accounts, and how to prepare key financial reports. All material for this class is written and delivered by the professor, and can be previewed here. Students must have access to a spreadsheet program to participate."}
    cat: Unable to write to output stream.



```python
df_schema = StructType(fields=[
    StructField("lang", StringType()),
    StructField("name", StringType()),
    StructField("cat", StringType()),
    StructField("provider", StringType()),
    StructField("id", IntegerType()),
    StructField("desc", StringType()),
])
```


```python
df_dir = '/labs/lab07data/DO_record_per_line.json'

#  наши таргеты
courses_list = [16871, 12234, 9029, 18030, 8170, 1200]


# «Похожесть» – в данном случае синоним «корреляции» интересов и может считаться множеством 
# способов (помимо корреляции Пирсона, есть еще косинусное расстояние, 
# есть расстояние Жаккара, расстояние Хэмминга и пр.)

# Будем использовать косинусное расстояние (косинусную близость)
```


```python
df = spark.read.json(df_dir, schema=df_schema).cache()
df.printSchema()
```

    root
     |-- lang: string (nullable = true)
     |-- name: string (nullable = true)
     |-- cat: string (nullable = true)
     |-- provider: string (nullable = true)
     |-- id: integer (nullable = true)
     |-- desc: string (nullable = true)
    



```python
# df.show(2, vertical=True, truncate=False)
```

    -RECORD 0
     lang     | ru                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
     name     | Разработка приложений для Modern UI: Windows 8                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
     cat      | 5/computer_science                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
     provider | Intuit                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
     id       | 1302                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
     desc     | Курс знакомит с инструментами и ресурсами Microsoft для разработки приложений для Windows Store.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
    -RECORD 1
     lang     | it                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
     name     | 3ds max 2014 Populate Architettura Italian                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
     cat      | 1/arts_music_film                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
     provider | Udemy                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
     id       | 8900                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
     desc     | 
    Video Master Class 3dsmax 2014 Populate in Architettura
    In questo Video Master class dedicato a 3dsmax 2014 sono illustrati i procedimenti di zione dei personaggi
    con People in visualizzazione architettonica. Argomenti: Gestione del nuovo Tools People per zione e animazione dei personaggi in visualizzazioni architettoniche.
    Gestione dei flussi di personaggi e loro modifica, impostazioni delle
    preferenze di etnia, sesso, direzione; gestione dei flussi incrociati.
    Impostazioni di gruppi di persone e modifica dello spazio occupato
    dai personaggi in relazione alla superficie architettonica, le tecniche di addizione e sottrazione boleane delle folle.
    Suggerimenti per gestione degli snap e posizionamento in precisione
    dei Flussi e delle Aree Idle.
    Gestione delle nuove Rampe singole e multiple.
    zione dei personaggi a bassa ed alta risoluzione.
    Gestione dei materiali e modifica della visualizzazione in viewport.
    Esempi di rendering statico in mental ray 3.11
    Esempi di rendering Animati in mental ray 3.11
    Gestione avanzata del final ghater e impostazioni di Rendering.
    Salvataggio delle animazioni finali nel nuovo Ram Player.
    Allegate alle lezioni sono disponibili i file 3dsmax 2014 Sono presenti 16 video risoluzione 1280 x 800
    in modalità smart focus zoom nelle zone di maggior importanza Il master ha una durata di 2 ore, 10 minuti lingua: italiano. Category:
    Design  
    only showing top 2 rows
    


## Попробуем для content-based similarity  HashingTF, CountVectorizer


```python
from pyspark.ml.feature import CountVectorizer, HashingTF, IDF, StopWordsRemover
# from pyspark.ml.feature import StopWordsRemover
```


```python
%%time
# самодельный токенайзер, сильно не мудрил, можно было использовать RegexTokenizer - было бы проще
corpus_regexp = r'[\t|\n|\$|\:|\*|\!|\?|\#|\:|\/|\\|,|\.|\(|\)|\[|\]|\{|\}|\"|\'|\$|\-|\+|\”|\“|\%|\¡|\¿|\&|\;|\s]'
df = df.withColumn('desc_tmp', lower(F.col('desc')))
df = df.withColumn('desc_tmp', F.split(F.col('desc_tmp'), corpus_regexp))
remover = StopWordsRemover(inputCol="desc_tmp", outputCol="corpus", stopWords=[""])
df = remover.transform(df)
# df = df['id', 'name', 'corpus']
df = df['id', 'name', 'lang', 'desc', 'corpus']
# df.show(3, vertical=True, truncate=False)
```

    CPU times: user 46.3 ms, sys: 13.2 ms, total: 59.5 ms
    Wall time: 623 ms



```python
%%time
# векторайзер. Так как объем данных не большой и ресурсы позволяют, берем обычный CountVectorizer
# но можно и hashingTF использовать, он быстрее.

# hashingTF = HashingTF(inputCol="corpus", outputCol="tf", numFeatures=10000)
# tf = hashingTF.transform(df)

countTF = CountVectorizer(inputCol="corpus", outputCol="tf").fit(df)
tf = countTF.transform(df)
```

    CPU times: user 25.3 ms, sys: 9 µs, total: 25.3 ms
    Wall time: 19.5 s



```python
# tf.show(3, vertical=True, truncate=False)
```


```python
%%time
# idf
idf = IDF(inputCol="tf", outputCol="raw_feature").fit(tf)
tfidf = idf.transform(tf)
```

    CPU times: user 11.1 ms, sys: 214 µs, total: 11.3 ms
    Wall time: 14.6 s



```python
# tfidf.show(3, vertical=True, truncate=False)
```


```python
%%time
# нормализуем
from pyspark.ml.feature import Normalizer
normalizer = Normalizer(inputCol="raw_feature", outputCol="feature")
data = normalizer.transform(tfidf)
```

    CPU times: user 5.84 ms, sys: 956 µs, total: 6.8 ms
    Wall time: 21.8 ms



```python
%%time
# Получили фичи
data.select('id', 'feature').show(3, vertical=True, truncate=False)
```

    -RECORD 0
     id      | 2675                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
     feature | (262144,[0,1,2,3,4,6,8,9,11,12,13,19,21,24,27,29,31,33,45,50,109,123,125,145,152,187,223,226,351,429,451,466,566,676,735,785,839,1067,1115,1196,1269,1323,1344,1362,1654,1768,1777,1975,2336,2429,3282,3867,4196,4437,4611,4900,5286,5336,5345,6155,6204,7517,8865,9510,15970,16175,17036,17120,19778,21185,21928,27712,28451,40468,114456,134034,232253],[0.028268905712859373,0.014346324394448232,0.0072840502205296295,0.025570840935428142,0.005928551836885708,0.020584948614390294,0.007235114620361124,0.006808284162188327,0.00899926471706777,0.009545821587865929,0.036019031598744464,0.04029871559817494,0.018790128667868593,0.014795035728839842,0.014767540410965663,0.03190059730310916,0.017630758727195207,0.01531684922105657,0.04824679335929818,0.020778974453176877,0.03316923389535339,0.029151670588192527,0.029865105391746888,0.03246547723299647,0.043146376246991344,0.041652403301131964,0.0372121099420596,0.03770746565035573,0.044533290175771956,0.04546449529410743,0.04747105421955832,0.05438645883132826,0.053761651794255766,0.055543828182320835,0.06107093760419432,0.05720000320006514,0.05867228362969858,0.06569896623526851,0.35399859042265464,0.06447780758439375,0.0670968960576126,0.06408893684090138,0.07341674413718553,0.07069432993505738,0.06812505220218361,0.08016715824725563,0.15136806393854849,0.4833557638741306,0.07754832226627212,0.07770429728563263,0.08915069568871921,0.09150811092709997,0.09094384770510784,0.09761987431502088,0.09577110553530256,0.0993248138128959,0.0993248138128959,0.09987036264928222,0.09761987431502088,0.10914443353393534,0.10378587908814409,0.11184932694165681,0.3478457521669094,0.11952115001816183,0.12811128902312882,0.13225922043777968,0.1272112417836053,0.1334698434689201,0.13225922043777968,0.14122271553081597,0.1347702215506959,0.14563254843641246,0.13937394675109765,0.14833744184413394,0.16050014681162633,0.16761487312494427,0.16761487312494427]) 
    -RECORD 1
     id      | 2836                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
     feature | (262144,[0,2,3,4,6,9,10,13,15,16,17,19,21,24,29,45,80,97,169,184,216,265,427,539,794,898,900,1178,1287,1773,1944,2254,2296,2404,4702,5905,9858,12289,17107,20940,27203,27290],[0.014388907628502225,0.004325522230115094,0.01822182509349406,0.014082320679537654,0.016298744085420452,0.00808599161205257,0.019789911269760707,0.02851913741587919,0.02745295485470893,0.011676717171658356,0.014058058079812972,0.015953852127826865,0.011158231588040697,0.03514322609148106,0.01894368361157663,0.057301246116954786,0.03155084346505823,0.032560573342595524,0.039774943658495376,0.044949709964900136,0.05489491130183634,0.04895895818881789,0.06127531365392316,0.060627239014570025,0.07563672388053602,0.07098080539564357,0.07371903851073541,0.07533207875176515,0.07831220346639434,0.19292427554348504,0.08999847023548972,0.09593830094182047,0.09044787040302166,0.1941547782043345,0.11068129610208366,0.11754402742030685,0.1346533649095702,0.5705398855839489,0.6283209816746281,0.15573525455813453,0.16354391295490986,0.17018052858080432])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
    -RECORD 2
     id      | 6984                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
     feature | (262144,[0,1,2,3,5,6,12,23,38,77,153,173,177,178,228,259,271,276,278,607,712,845,1556,4504,8732,31968,35267],[0.024419554246841793,0.008674958813264508,0.011011328393162805,0.015462217766469846,0.03279173925486724,0.02074556712148826,0.057721829533949456,0.04796766844712366,0.058758635395735295,0.07376846307493887,0.0977028480038336,0.1120381837269278,0.10849957282986225,0.11041276511795217,0.147230910611865,0.29659923893194495,0.13584809079387725,0.1434398720180071,0.12210769773105558,0.15552491210735428,0.1915606449333196,0.18260165849296728,0.27915086073623185,0.29563823362704733,0.3534282662038023,0.43322253051405624,0.44848463030741786])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
    only showing top 3 rows
    
    CPU times: user 3.39 ms, sys: 559 µs, total: 3.95 ms
    Wall time: 1.22 s


## Считаем косинусную близость для текстов по  спарс векторам, в обычной жизни это очень долго для прода. Но для холодного старта может подойти.


```python
%%time
# задали udf и матрицу cos_sim, лениво вычисления будут ниже...
@udf
def sim_cos(v1,v2):
    try:
        p = 2
        return float(v1.dot(v2))/float(v1.norm(p)*v2.norm(p))
    except:
        return 0

# строим лениво квадратную матрицу таргет * на все id
# очень дорогая операция если делать за раз... Но т.к. размер малый то можно :)
cossimmatrix = data.alias("i").join(data.alias("j"), F.col("i.id") != F.col("j.id"))\
    .select(
        F.col("i.id").alias("id"),
        F.col("i.name").alias("i_name"),
        F.col("i.lang").alias("i_lang"),
        F.col("i.desc").alias("i_desc"),
        F.col("i.id").alias("i"), 
        F.col("j.id").alias("j"),
        sim_cos("i.feature", "j.feature").cast("float").alias("sim_cosine"),
        F.col("j.name").alias("j_name"),
        F.col("j.lang").alias("j_lang"),
        F.col("j.desc").alias("j_desc"))\
    .filter(F.col('id').isin(courses_list))\
    .sort("i", "j")

```

    CPU times: user 18.7 ms, sys: 1.07 ms, total: 19.8 ms
    Wall time: 418 ms



```python
%%time
# тут спарк считает близость, самый тяжелый процесс
cossimmatrix.show(3, vertical=True, truncate=False)
```

    -RECORD 0
     id         | 1200                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
     i_name     | Современные операционные системы                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
     i_lang     | ru                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
     i_desc     | В курсе представлены понятия и положения теории операционных систем. Даны основные определения и классификации, рассмотрены интерфейсы операционных систем, организация вычислительного процесса, вопросы управления памятью и устройствами компьютера, организации файловых систем. Уделено внимание совместимости операционных сред и средствам ее обеспечения, в том числе виртуальным машинам. Изложена история происхождения двух наиболее распространенных представителей этого класса программных систем: семейства UNIX/Linux и компании Microsoft. Рассмотрены стандарты и лицензии на программные продукты.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
     i          | 1200                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
     j          | 4                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
     sim_cosine | 0.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
     j_name     | Accounting Cycle: The Foundation of Business Measurement and Reporting                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
     j_lang     | en                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
     j_desc     | This course introduces the basic financial statements used by most businesses, as well as the essential tools used to prepare them. This course will serve as a resource to help business students succeed in their upcoming university-level accounting classes, and as a refresher for upper division accounting students who are struggling to recall elementary concepts essential to more advanced accounting topics. Business owners will also benefit from this class by gaining essential skills necessary to organize and manage information pertinent to operating their business. At the conclusion of the class, students will understand the balance sheet, income statement, and cash flow statement. They will be able to differentiate between cash basis and accrual basis techniques, and know when each is appropriate. They’ll also understand the accounting equation, how to journalize and post transactions, how to adjust and close accounts, and how to prepare key financial reports. All material for this class is written and delivered by the professor, and can be previewed here. Students must have access to a spreadsheet program to participate. 
    -RECORD 1
     id         | 1200                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
     i_name     | Современные операционные системы                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
     i_lang     | ru                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
     i_desc     | В курсе представлены понятия и положения теории операционных систем. Даны основные определения и классификации, рассмотрены интерфейсы операционных систем, организация вычислительного процесса, вопросы управления памятью и устройствами компьютера, организации файловых систем. Уделено внимание совместимости операционных сред и средствам ее обеспечения, в том числе виртуальным машинам. Изложена история происхождения двух наиболее распространенных представителей этого класса программных систем: семейства UNIX/Linux и компании Microsoft. Рассмотрены стандарты и лицензии на программные продукты.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
     i          | 1200                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
     j          | 5                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
     sim_cosine | 0.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
     j_name     | American Counter Terrorism Law                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
     j_lang     | en                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
     j_desc     | This online course will introduce you to American laws related to terrorism and the prevention of terrorism. My approach to the topic is the case-study method. Each week, we will read a case study, along with the statutes, regulations, and other law-related materials relevant to the case. We’ll see how the case was handled in court and what reforms were enacted following the trial. Each week’s assignment will include copies of the relevant laws and court rules, a glossary of terms, background readings, and other supplementary materials. The course will commence with the first attempt by Islamic militants to bring down the World Trade Center towers with a truck bomb in 1993. From there, I'll take you through the major terrorist incidents of the past 20 years, including acts perpetrated by homegrown terrorists, such as the Oklahoma City bombing of 1995 and the trial of the SHAC Seven (animal rights) terrorists in Trenton (NJ) in 2006. Required materials: The textbook for this course is Counter Terrorism Issues: Case Studies in the Courtroom, by Jim Castagnera (estimated cost: $100) Find it at CRC Press                         
    -RECORD 2
     id         | 1200                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
     i_name     | Современные операционные системы                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
     i_lang     | ru                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
     i_desc     | В курсе представлены понятия и положения теории операционных систем. Даны основные определения и классификации, рассмотрены интерфейсы операционных систем, организация вычислительного процесса, вопросы управления памятью и устройствами компьютера, организации файловых систем. Уделено внимание совместимости операционных сред и средствам ее обеспечения, в том числе виртуальным машинам. Изложена история происхождения двух наиболее распространенных представителей этого класса программных систем: семейства UNIX/Linux и компании Microsoft. Рассмотрены стандарты и лицензии на программные продукты.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
     i          | 1200                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
     j          | 6                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
     sim_cosine | 0.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
     j_name     | Arithmétique: en route pour la cryptographie                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
     j_lang     | fr                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
     j_desc     | This course is taught in French Vous voulez comprendre l'arithmétique ? Vous souhaitez découvrir une application des mathématiques à la vie quotidienne ? Ce cours est fait pour vous ! De niveau première année d'université, vous apprendrez les bases de l'arithmétique (division euclidienne, théorème de Bézout, nombres premiers, congruence). Vous vous êtes déjà demandé comment sont sécurisées les transactions sur Internet ? Vous découvrirez les bases de la cryptographie, en commençant par les codes les plus simples pour aboutir au code RSA. Le code RSA est le code utilisé pour crypter les communications sur internet. Il est basé sur de l'arithmétique assez simple que l'on comprendra en détail. Vous pourrez en plus mettre en pratique vos connaissances par l'apprentissage de notions sur le langage de programmation Python. Vous travaillerez à l'aide de cours écrits et de vidéos, d'exercices corrigés en vidéos, des quiz, des travaux pratiques. Le cours est entièrement gratuit !                                                                                                                                                             
    only showing top 3 rows
    
    CPU times: user 9.81 ms, sys: 1.21 ms, total: 11 ms
    Wall time: 44.8 s
   



```python
# наши полученные оценки близости
cossimmatrix.select('i', 'j', 'sim_cosine').orderBy(F.col('sim_cosine').desc()).show(20)
```

    +-----+-----+----------+
    |    i|    j|sim_cosine|
    +-----+-----+----------+
    |18030| 3660|0.63669133|
    |18030| 8098|0.63669133|
    |18030|26336| 0.6249196|
    |18030|26670|0.62393713|
    |18030|20763|0.61926955|
    |18030|17838| 0.6167861|
    |18030| 7944|0.61545837|
    |18030|21053|0.61187696|
    |18030|  387|0.60660166|
    |18030| 4096| 0.6064375|
    |18030| 6864| 0.6050712|
    |18030|17200| 0.5963444|
    |18030|21337| 0.5952187|
    |18030|13275| 0.5937647|
    |18030|22680| 0.5921054|
    |18030|22284| 0.5873646|
    |18030|12413| 0.5872974|
    |18030|10035| 0.5859689|
    |18030|16924|0.58521444|
    |18030|13102|0.58461344|
    +-----+-----+----------+
    only showing top 20 rows
    



```python
# Выбираем топ 10 наших оценок близости для каждого таргета
output = {}
for elem in courses_list:
    output[elem[0]] = cossimmatrix.select(F.col('j'))\
                                    .where(F.col('id') == elem[0])\
                                    .where(F.col('j_lang') == elem[1])\
                                    .orderBy(F.desc('sim_cosine'), 
                                             F.asc('j_name'), 
                                             F.asc('j'))\
                                    .limit(10)\
                                    .rdd.flatMap(lambda x: x)\
                                    .collect()
output
```




    {16871: [20182, 19809, 12363, 12952, 20534, 13127, 20183, 19810, 13125, 7397],
     12234: [2164, 2162, 23256, 2161, 8101, 3745, 164, 3146, 12384, 2160],
     9029: [23114, 6864, 3660, 8098, 22680, 21400, 26336, 26670, 4096, 23629],
     18030: [3660, 8098, 26336, 26670, 20763, 17838, 7944, 21053, 387, 4096],
     8170: [1311, 8169, 1310, 20352, 1305, 1325, 13685, 8007, 17127, 867],
     1200: [1208, 8212, 1204, 1209, 1187, 19419, 1004, 20347, 923, 1343]}



## Имеем чекер на стороне по скрытой выборке

[[1200, 0.6], [8170, 0.7], [9029, 0.9], [12234, 0.7], [16871, 0.9], [18030, 0.7]] 
Достаточно точно


```python
import json
```


```python
# сохранили файл. Его нужно положить на сервер и отдать чекеру
with open(file='to_chk.json', mode='wt') as file:
    file.write(json.dumps(output) + '\n')
```


```python
spark.stop()
```
