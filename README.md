# Public-repo

Привет! Это одна из моих папочек с работами в открытом доступе.
Здесь Вы можете ознакомится с моим скилами.

### 1_Pandas_Gender_Age_prediction_based_on_user_logs
  Выпускной проект от NewProLab, Специалист по большим данным 13,0
  
####  Имея логи пользователей (id, url, timestamp) предсказываем пол и возраст
  
  Обучение с учителем на sklearn.
  
  Фича: `host, path, day, hour, std(по времени), min max (hour), counter (посещений)`
  
  Решение: `Логитическая регрессия One-vs-rest`
  
  Точность: `0,296 по Accuracy`
  
  Хоть и пандас но сделан мультипроцессинг, потому считает быстро.
  
### 2_Spark_cossim_text
  Учебный проект от NewProLab, Специалист по большим данным 13,0
  
###  По имеющимся данным портала eclass.cc построить content-based рекомендации по образовательным курсам.
  
  На Спарке ищем наиболее похожие к id из задания по CosSim 10 курсов по их описанию.
  
  Фича: `Описание, которыей преобразуем в BagOfWords и дальше в спарс вектор`
  
  Решение: `ml.CountVectorizer или ml.HashingTF + своя UDF`
  
  Точность: `от 0,6 до 0,9 по Accuracy в зависимости от размера описания статьи`
  
### 3_ALS_purchase_prediction
  Учебный проект от NewProLab, Специалист по большим данным 13,0
  
####  В вашем распоряжении имеется уже предобработанный и очищенный датасет с фактами
  покупок абонентами телепередач от компании E-Contenta. По доступным вам данным нужно предсказать вероятность покупки других передач этими, 
  а, возможно, и другими абонентами.
  
  На Спарке, коллаборативная фильтрация user-item
  
  Фича: `user-item таблица покупок`
  
  Решение: `За основу взят ml.ALS и протюнил по CV параметры`
  
  Точность `0.792 по RMSE`
  
### 4_Spark_funk_SVD  
  Учебный проект от NewProLab, Специалист по большим данным 13,0
  
####  По имеющимся данным о рейтингах фильмов (MovieLens: 100 000 рейтингов) построить коллаборативную рекомендательную систему 
  на основе коллаборативной фильтрации путём подсчёта базовых предикторов и создания с ними Item-Item Recommender System.
  
  На Спарке, коллаборативная фильтрация item-item
  
  Фича: `user-item таблица рейтингов`
  
  Решение: `За основу взят метод SVD с фиксацией предсказаний только для одного пользователя. Соревнование на Каггл по нетфликсу. funk-SVD`

### 5_Graph_social_network_recommendation_model
  Выпускной проект от NewProLab, Специалист по большим данным 13,0
  
####  Нужно построить content-based рекомендательный алгоритм товаров интернет-магазина ozon.ru наилучшей предсказательной точности по метрике NDCG@100.
  
  На Спарке, графовая модель 
  
  Фича: пары c общим мастер id 
  ```
    т.е. из {"item": "31471303", "true_recoms": {"31471373": 1, "31471307": 3, "31471342": 1}}
    делаем [31471373,31471307]
           [31471373,31471342]
           [31471307,31471342]
           ...
           [31471307, 31471373]
           [31471342, 31471373]
           [31471342, 31471307]
           полностью симметричный датасет со всеми комбинациями...
  ```         
  Решение: `За основу взят граф связей социальных сетей.`
  
  Точность: `0,13, 10мин, одним проходом для всех 1 300 000 записей`
  ```
    Решение в лоб по косинусной близости контекста занимает с батчами ~ 19 дней, точность 0,31 
    Решение факторизацией оценок ~ 4 часа с батчами, подбор параметров займет около недели. точность 0,19
  ```  

    
