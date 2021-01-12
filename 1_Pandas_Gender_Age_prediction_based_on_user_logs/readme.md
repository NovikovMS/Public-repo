

```python
import pandas as pd
import os
import pickle
import datetime
from sklearn.feature_selection import chi2, SelectKBest
from sklearn.pipeline import Pipeline, FeatureUnion
#from sklearn.impute import SimpleImputer as Imputer  # new numpy version
from sklearn.preprocessing import Imputer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
import xgboost as xgb
from sklearn.preprocessing import MaxAbsScaler, StandardScaler
from sklearn.metrics import f1_score, roc_auc_score, log_loss, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import FunctionTransformer
from functions import GetPreprocessedData, dummify_labels, undummify_labels
from functions import get_numeric_data_func, get_text_data_func, parallelize
from functions import recover_index, project_accuracy
from tqdm import tqdm

```


```python
# Используя доступный набор данных о посещении страниц у одной части пользователей, сделать прогноз относительно **пола и возрастной категории** другой части пользователей. Угадывание (hit) - правильное предсказание и пола, и возрастной категории одновременно.


# Имеем логи пользователей. Надо предсказать их пол (M, F) и возраст (18-24, 25-34, 35-44, 45-54, >=55).
!head -n 2 train_df.csv
```

    gender,age,uid,user_json
    F,18-24,d50192e5-c44e-4ae8-ae7a-7cfe67c8b777,"{""visits"": [{""url"": ""http://zebra-zoya.ru/200028-chehol-organayzer-dlja-macbook-11-grid-it.html?utm_campaign=397720794&utm_content=397729344&utm_medium=cpc&utm_source=begun"", ""timestamp"": 1419688144068}, {""url"": ""http://news.yandex.ru/yandsearch?cl4url=chezasite.com/htc/htc-one-m9-delay-86327.html&lr=213&rpt=story"", ""timestamp"": 1426666298001}, {""url"": ""http://www.sotovik.ru/news/240283-htc-one-m9-zaderzhivaetsja.html"", ""timestamp"": 1426666298000}, {""url"": ""http://news.yandex.ru/yandsearch?cl4url=chezasite.com/htc/htc-one-m9-delay-86327.html&lr=213&rpt=story"", ""timestamp"": 1426661722001}, {""url"": ""http://www.sotovik.ru/news/240283-htc-one-m9-zaderzhivaetsja.html"", ""timestamp"": 1426661722000}]}"   



```python
%%time
df = pd.read_csv('train_df.csv')  # читаем учебный датафрейм

# словарь параметров для функции преобраования
```

    CPU times: user 5.58 s, sys: 1.12 s, total: 6.7 s
    Wall time: 6.73 s



```python
df.gender.value_counts()  # одинкаковое распределение
```




    M    18698
    F    17440
    Name: gender, dtype: int64




```python
df.age.value_counts()  # неравномерное распределение
```




    25-34    15457
    35-44     9360
    18-24     4898
    45-54     4744
    >=55      1679
    Name: age, dtype: int64




```python
# !!! точность упала, не применяем. 
# удалили 69% случайных значений с '25-34' и 48% '35-44', чтобы уравновесить выборку
#df = df[df.age == '25-34'].sample(frac=.31)\
#    .append(df[df.age == '35-44'].sample(frac=.52))\
#    .append(df[df.age == '18-24'])\
#    .append(df[df.age == '45-54'])\
#    .append(df[df.age == '>=55'])   # не меняем  эту категорию, потому как их реально мало 

```


```python
# наш учебный датасет весит больше 600мб. Разом засунуть в пандас на моем пк (калькуляторе) не получится. 
# Будем делать батчами по 361 наблюдению 101 раз.
length = df.shape[0]
step = int(length/100)
iterations = int(length/step)+1
print('length: {0}, step: {1}, iterations: {2}'.format(length, step, iterations))
```

    length: 36138, step: 361, iterations: 101



```python
%%time
to_process = GetPreprocessedData().transform  # объявили функцией

# Здесь мы готовим фичи для датасета.
# Фичи: url, кол-во посещений, час, день недели. url - текст, остальные - цифры.
# Перевели колличество посещений пользователя, урл, час, день недели по uid в столбец (мелт для таргетов),
#   чтобы модель эффективно видела все посещения пользователя, а не смотрела лишь на начало строк
#   в 2000 колонок  одному параметру наблюдения. А там 2000 * 3.
```

    CPU times: user 13 µs, sys: 3 µs, total: 16 µs
    Wall time: 18.8 µs



```python
%%time

start = 0
end = start + step
processed_df = pd.DataFrame()

for i in tqdm(range(iterations)):
    processed_df = processed_df.append(parallelize(df[start:end], to_process, cores=15))
    start += step
    end += step
    if start > length:
        print('start: {0}, end: {1}'.format(start, end))
        pass
```

    100%|██████████| 101/101 [01:41<00:00,  1.01s/it]

    start: 36461, end: 36822
    CPU times: user 51.9 s, sys: 41.1 s, total: 1min 32s
    Wall time: 1min 41s


    



```python
print(processed_df.shape)
```

    (36138, 14)



```python
processed_df.head()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>gender</th>
      <th>age</th>
      <th>uid</th>
      <th>url</th>
      <th>most_frqnt_visit</th>
      <th>counter</th>
      <th>hour</th>
      <th>latest</th>
      <th>earliest</th>
      <th>std_hour</th>
      <th>dayofweek</th>
      <th>end_week</th>
      <th>bgn_week</th>
      <th>std_week</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>F</td>
      <td>18-24</td>
      <td>d50192e5-c44e-4ae8-ae7a-7cfe67c8b777</td>
      <td>zebra zoya ru news yandex ru sotovik ru news y...</td>
      <td>2.0</td>
      <td>5</td>
      <td>8.0</td>
      <td>13.0</td>
      <td>6.0</td>
      <td>2.561250</td>
      <td>2.0</td>
      <td>5.0</td>
      <td>2.0</td>
      <td>1.200000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>M</td>
      <td>25-34</td>
      <td>d502331d-621e-4721-ada2-5d30b2c3801f</td>
      <td>sweetrading ru sweetrading ru sweetrading ru 1...</td>
      <td>49.0</td>
      <td>102</td>
      <td>18.0</td>
      <td>23.0</td>
      <td>0.0</td>
      <td>4.538496</td>
      <td>1.0</td>
      <td>6.0</td>
      <td>0.0</td>
      <td>1.646504</td>
    </tr>
    <tr>
      <th>2</th>
      <td>F</td>
      <td>25-34</td>
      <td>d50237ea-747e-48a2-ba46-d08e71dddfdb</td>
      <td>ru oriflame com ru oriflame com ru oriflame co...</td>
      <td>23.0</td>
      <td>44</td>
      <td>14.0</td>
      <td>18.0</td>
      <td>14.0</td>
      <td>1.229795</td>
      <td>4.0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>1.617792</td>
    </tr>
    <tr>
      <th>3</th>
      <td>F</td>
      <td>25-34</td>
      <td>d502f29f-d57a-46bf-8703-1cb5f8dcdf03</td>
      <td>translate tattoo ru nadietah ru 1obl ru 1obl r...</td>
      <td>12.0</td>
      <td>14</td>
      <td>12.0</td>
      <td>19.0</td>
      <td>9.0</td>
      <td>3.519624</td>
      <td>2.0</td>
      <td>5.0</td>
      <td>1.0</td>
      <td>1.394230</td>
    </tr>
    <tr>
      <th>4</th>
      <td>M</td>
      <td>&gt;=55</td>
      <td>d503c3b2-a0c2-4f47-bb27-065058c73008</td>
      <td>mail rambler ru news rambler ru mail rambler r...</td>
      <td>66.0</td>
      <td>212</td>
      <td>10.0</td>
      <td>20.0</td>
      <td>6.0</td>
      <td>5.189091</td>
      <td>3.0</td>
      <td>5.0</td>
      <td>1.0</td>
      <td>1.557547</td>
    </tr>
  </tbody>
</table>
</div>




```python
%%time
# Выделяем только цифровые колонки
numeric_columns = processed_df.columns.tolist()[4:]

# Параметры модели
model_params = {'age_labels': 'age',
                'gender_labels': 'gender',
                'uid': 'uid',
                'text': 'url',
                'numeric': numeric_columns,
                'test_size': 0.2,
                'random_seed': 25
                }

# Делим на трейн и тест датафрейм для обучения и валидации
X_train, X_test, y_train, y_test = train_test_split(
    processed_df[[model_params['uid']]
                 + [model_params['text']]
                 + model_params['numeric']],
    processed_df[[model_params['gender_labels']]
                 + [model_params['age_labels']]],
    test_size=model_params['test_size'],
    random_state=model_params['random_seed'])

# переводим категориальные таргеты в бинарные слобцы
y_train_dummified = dummify_labels(y_train)
y_test_dummified = dummify_labels(y_test)

print('X_train : {}, X_test : {}, y_train : {}, y_test : {}'.format(
    X_train.shape,
    X_test.shape,
    y_train_dummified.shape,
    y_test_dummified.shape))
```

    X_train : (28910, 12), X_test : (7228, 12), y_train : (28910, 7), y_test : (7228, 7)
    CPU times: user 2.84 s, sys: 277 ms, total: 3.12 s
    Wall time: 103 ms



```python
# Ожидаем колчиство уникальных url 30 000+ дальше уменьшим их до 1800 (~5%) самых значимых по расстоянию между ними.
# В модель попадет 1800+3 = 1803 параметра на 7 бинарных классов
#   ['gender_F', 'gender_M', 'age_18-24', 'age_25-34', 'age_35-44', 'age_45-54', 'age_>=55']
#   (можно и 5 классов сделать, но тогда я не получу вероятность получения исключенного бинарного столбца,
#   он мне будет нужен дальше, после обучения модели.)


# ставим целевое колличество для выбора наиболее значимых слов-столбцов в спарс-матрице
chi_k = 1800  # 36000 * 5%

# Пайплайн
pl = Pipeline([
    ('union', FeatureUnion(
        # Объединяем подготовленные данные
        transformer_list=[

            # Подготоваливаем текстовую часть данных
            ('text_features', Pipeline([
                ('selector', FunctionTransformer(get_text_data_func, validate=False)),
                # Выбрали только текстовые столбцы
                ('vectorizer', TfidfVectorizer()),
                # создали спарс матрица по словам
                ('dim_red', SelectKBest(chi2, k=chi_k))
                # уменьшили размерность до 1800
            ])),

            # Подготоваливаем цифровую часть данных
            ('numeric_features', Pipeline([
                ('selector', FunctionTransformer(get_numeric_data_func, validate=False)),
                # Выбрали только числовые столбцы
                ('imputer', Imputer())
                # подготовка, проверка числовых данных
            ]))
        ]
    )),

    # Сама модель
    ('scale', MaxAbsScaler()),  # sparse matrix нормализация
    #('scale', StandardScaler(with_mean=False)),
    ('clf', OneVsRestClassifier(LogisticRegression(max_iter=1000, n_jobs=15, solver='saga', penalty='elasticnet')))  # multiclass-multioutput classifier
    # Старатегия на каждый класс запускать отдельный классификатор 1 за  / 7 против и выводить лучший со своим весом
    #('clf', OneVsRestClassifier(SVC(probability=True, 
#                                     max_iter=1000, 
#                                     decision_function_shape='ovr', 
#                                     random_state=43,
#                                     kernel='sigmoid')))    
    # c логистической регрессией
])
```


```python
# %%time
# # Здесь учим на всем датасете
# X = processed_df[[model_params['uid']]
#                  + [model_params['text']]
#                  + model_params['numeric']]
# y = processed_df[[model_params['gender_labels']]
#                  + [model_params['age_labels']]]

# y_dummified = dummify_labels(y)

# pl.fit(X,
#        y_dummified)  # учим модель
```


```python
%%time
# Учим модель только на трейне
pl.fit(X_train, y_train_dummified.values)  # учим модель
```

    CPU times: user 1min 41s, sys: 4.14 s, total: 1min 45s
    Wall time: 38 s





    Pipeline(memory=None,
         steps=[('union', FeatureUnion(n_jobs=1,
           transformer_list=[('text_features', Pipeline(memory=None,
         steps=[('selector', FunctionTransformer(accept_sparse=False,
              func=<function get_text_data_func at 0x7f0197fd58c8>,
              inv_kw_args=None, inverse_func=None, kw_args=None,
        ...state=None, solver='saga',
              tol=0.0001, verbose=0, warm_start=False),
              n_jobs=1))])




```python
%time
# Предсказываем и сравниваем по трейн сету, в качестве валидатора, самописный Accuracy, проверяющий совпадение пар.
y_train_pred_dummified = pl.predict_proba(X_train)  # Делаем предсказание по учебному датасету

y_train_pred_proba_recovered = recover_index(X_train,
                                       y_train_pred_dummified,
                                       X_train,
                                       y_train_dummified.columns.tolist(),
                                       'uid')

y_train_pred_proba_undummified = undummify_labels(pd.DataFrame(y_train_pred_proba_recovered,
                                                 columns=y_train_dummified.columns.tolist()))
print('Точность по учебному датасету = {0}'.format(project_accuracy(y_train, y_train_pred_proba_undummified)))
```

    CPU times: user 3 µs, sys: 1e+03 ns, total: 4 µs
    Wall time: 7.63 µs
    Точность по учебному датасету = 0.3236942234520927



```python
%time
# Предсказываем и сравниваем по тест сету, в качестве валидатора, самописный Accuracy, проверяющий совпадение пар.
y_test_pred_proba_dummified = pl.predict_proba(X_test)  # на тестовом датасете предсказали вероятности
y_test_pred_dummified = pl.predict(X_test)  # на тестовом датасете предсказали классы


y_test_pred_proba_recovered = recover_index(X_test,
                                       y_test_pred_proba_dummified,
                                       X_test,
                                       y_train_dummified.columns.tolist(),
                                       'uid')

y_test_pred_proba_undummified = undummify_labels(pd.DataFrame(y_test_pred_proba_recovered,
                                                 columns=y_test_dummified.columns.tolist()))
print('Точность по учебному датасету = {0}'.format(project_accuracy(y_test, y_test_pred_proba_undummified)))
```

    CPU times: user 4 µs, sys: 0 ns, total: 4 µs
    Wall time: 8.82 µs
    Точность по учебному датасету = 0.29676258992805754



```python
%%time
# Сохраняем модель, которая содержится в переменной pl
model_file = "project01_model.pickle"

with open('./' + model_file, 'wb') as f:
    pickle.dump(pl, f)

os.chmod('./' + model_file, 0o644)
```

    CPU times: user 3.45 s, sys: 991 ms, total: 4.44 s
    Wall time: 4.5 s

