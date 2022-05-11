#!/opt/anaconda/ens/bd9/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from multiprocessing import cpu_count, Pool  # Parallel
import json
from urllib.parse import urlparse, unquote
import idna
from collections import Counter
# from sklearn.preprocessing import FunctionTransformer


# Функция  запуска в несколько потоков
def parallelize(data_in, func, cores=None):
    '''
 http://blog.adeel.io/2016/11/06/parallelize-pandas-map-or-apply/
 Custom function to run process with a pool of workers
 '''
    if not cores:
        cores = cpu_count() - 1  # Number of CPU cores on your system
    partitions = cores  # Define as many partitions as you want

    data_split = np.array_split(data_in, partitions)
    pool = Pool(cores)
    data_out = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data_out


# параметры подготовки фич
preprocessing_params = {'labels': ['gender', 'age'],
                        'uid': ['uid'],
                        'numeric_columns': ['timestamp'],
                        'text_columns': ['user_json'],
                        'json_parsed_columns': ['url', 'timestamp'],
                        }


# библиотека для подготовки фич --- легкая ~ выполнение меньше 2 мин Ок
class GetPreprocessedData:
    """
    Custom class prepairing data to run on model
    """

    def __init__(self):
        """
        Constructor
        :param params dict (optional): dict with input columns data and transformation tweaks
        """
        params = preprocessing_params  # global dict with processing params

        self.labels = params['labels']
        self.uid = params['uid']
        self.numeric = params['numeric_columns']
        self.text = params['text_columns']
        self.json_parsed_columns = params['json_parsed_columns']

    def transform(self, df_in):
        """
        Основная рабочая функция класса:
        1. Делим user_json на два столбца мелтом с сырыми данными
        2. Извлекли хосты из сырых урл и положили обратно
        3. Посчитали количество посещений по timestamp добавили колонку 'counter'
        4. Привели timestamp к datetime, вытащили из timestamp колонками часы и дни недели
        5. Сформировали лист целевых колонок с которыми вернет датафрейм
        """
        df_local = df_in
        df_local = self.parse_json(df_local,
                                        self.labels + self.uid,     # idx_columns: age gender uid
                                        self.text,                  # column_in: user_json
                                        self.json_parsed_columns)   # columns_out: url timestamp
        # Делим user_json на два столбца с сырыми данными строк
        df_local, most_frqnt_cnt_col_name = self.extract_host_list(df_local, self.json_parsed_columns[0])
        # Извлекли хосты из сырых урл и положили обратно
        df_local, counter_col_name = self.add_counter_column(df_local, [self.json_parsed_columns[1]])
        # Посчитали количество посещений по timestamp добавили колонку 'counter'
        df_local, time_column_names = self.timestamps_to_mean_hour_dayofweek(df_local,
                                                                             self.numeric
                                                                             )
        # Вытащили из timestamp_* колонками часы и дни недели

        output_columns = self.labels \
                         + self.uid \
                         + [self.json_parsed_columns[0]] \
                         + [most_frqnt_cnt_col_name] \
                         + counter_col_name \
                         + time_column_names
        # Колонки которые отдаст функция
        return df_local[output_columns]

    @staticmethod
    def parse_json(df_in: pd.DataFrame, idx_columns, column_in, columns_out):
        """
        Функция берет на вход сырую df с загрузки:
        1. выделяет j-son строку и парсит ее на лист словарей
        2. из словарей вытаскивает url, timestamp и складывает новой строкой в out_df url, timestamp, где
        с входящей таблицы мы взяли 'age', 'gender', 'uid' по индексу строки, которую парсили.
        """
        df_local: pd.DataFrame = df_in.copy(deep=True)
        out_df = pd.DataFrame(data=None, columns=idx_columns+columns_out)  # age gender uid url timestamp
        # пустая временная df с колонками age gender uid url timestamp
        urls = []  # лист с урлами (уйдет в bag of words)
        timestamps = []  # лист с таймстемпами

        for row_idx, visit in df_local[column_in[0]].items():
            json_strings = json.loads(visit)['visits']
            # для каждой строки получаем массив из {'url': url-string, 'timestamp': timestamp-val}
            for json_string in json_strings:
                urls.append(json_string[columns_out[0]])
                timestamps.append(json_string[columns_out[1]])

            out_df.loc[row_idx, idx_columns] = df_local.loc[row_idx, idx_columns]  # 'age', 'gender', 'uid'
            out_df.loc[row_idx, columns_out] = pd.Series([urls, timestamps], index=columns_out)  # 'url', 'timestamp'

            urls = []  # лист с урлами (уйдет в bag of words)
            timestamps = []  # лист с таймстемпами

            # в пусстую датафрейм добавили в конец строку с {'age':, 'gender':, 'uid':, 'url', 'timestamp':},
            # где {'age':, 'gender':, 'uid':} взяты по индексу со входящей таблицы
            # в итоге получили таблицу с колонками строк url, timestamp
        return out_df

    @staticmethod
    def extract_host_list(df_in, column):
        """
        Выбирает хосты и пути из листа url таблице кладет обратно мешком слов
        """
        df_local = df_in
        counter_col_name = 'most_frqnt_visit'
        df_local[counter_col_name] = np.nan

        drop_list = [
            'http:',
            'https:',
            ' '
        ]

        replace_list = [
            '.',
            ',',
            '-',
            '_',
            '/',
            ':',
            '~',
            '(',
            ')',
            ' ',
            '!',
            '[',
            ']',
            '\t\t'
        ]

        tmp_u_host_list = []
        tmp_u_path_list = []

        for row_idx, url_list in df_local[column].items():
            # перебираем таблицу построчно, в строках листы, в листах урлы, а в урлов там тьма... и все на деревьях
            # .... игла в яйце, яйцо в утке, утка в зайце, заяц в гробу, а гробов там тьма, и все на деревьях (c)
            for url in url_list:
                url = unquote(url, encoding='utf-8')  # преобразовали Unicode в UTF-8
                u = urlparse(url)

                # приводим idna хосты к читабельному виду
                if u.hostname and 'xn--' in u.hostname:
                    try:
                        u_host = idna.decode(u.hostname)
                    except Exception:
                        u_host = 'BAD_IDNA: ' + u.hostname
                else:
                    u_host = u.hostname

                #   u_scheme = u.scheme  # Http, Https
                #   u_host = u.netloc  # искомый хост с/без www
                #   u_port = u.port  # порт запроса
                u_path = u.path  # чтото вроде '/%7Eguido/Python.html'
                #   u_params = u.params
                #   u_query = u.query
                #   u_fragment = u.fragment


                u_host = u_host.replace('www.', '')  # убираем www.

                if u_host in drop_list or u_host is np.nan:  # убираем ошибки парсинга убираем NaN
                    pass
                else:
                    tmp_u_host_list.append(u_host)  # наполняем лист значениями

                for element in replace_list:
                    u_path = u_path.replace(element, ' ')  # заменяем все что в списке

                if u_path is np.nan:  # убираем ошибки парсинга убираем NaN
                    pass
                else:
                    tmp_u_path_list.append(u_path)  # наполняем лист значениями

            df_local.loc[row_idx, counter_col_name] = max(Counter(tmp_u_host_list).values()) 
            # посчитали максимальное количество посещений 'любимого' сайта пользователя

            # собираем словарь слов из пути и хоста
            for idx, host in enumerate(tmp_u_host_list):
                for elem in replace_list:
                    tmp_u_host_list[idx] = tmp_u_host_list[idx].replace(elem, ' ')  # заменяем все что в списке

            df_local.loc[row_idx, column] = ' '.join(tmp_u_host_list + tmp_u_path_list)
            # заменили значение в таблице через пробел урлами
            tmp_u_host_list = []
            tmp_u_path_list = []

        return df_local, counter_col_name


    @staticmethod
    def add_counter_column(df_in: pd.DataFrame, column, counter_col_name = 'counter'):
        """
        делает столбец счетчик 'counter' где считает количество всех посещенных хостов по записи времени
        """
        df_local = df_in
        df_local[counter_col_name] = np.nan
        df_local[counter_col_name] = df_local[column[0]].apply(lambda x: len(x)).astype(np.int)


        # посчитали колличество записей в листе и привели столбец счетчик к int
        return df_local, [counter_col_name]

    @staticmethod
    def timestamps_to_mean_hour_dayofweek(df_in, columns):
        """
        вытаскивает из timestamp mean час дня, день недели...  на колонки
        """
        df_local = df_in
        hour_col_names = ['hour']
        # создаем список имен новых колоночек hour
        dayofweek_col_names = ['dayofweek']
        # создаем список имен новых колоночек dayofweek
        latest_visit = ['latest']
        earliest_visit = ['earliest']
        std_hour = ['std_hour']
        beginning_week = ['bgn_week']
        end_week = ['end_week']
        std_week = ['std_week']
        output_column_names = hour_col_names\
                            + latest_visit\
                            + earliest_visit\
                            + std_hour \
                            + dayofweek_col_names \
                            + end_week \
                            + beginning_week\
                            + std_week

        df_local[hour_col_names[0]] = np.nan
        df_local[dayofweek_col_names[0]] = np.nan
        df_local[latest_visit[0]] = np.nan
        df_local[earliest_visit[0]] = np.nan
        df_local[std_hour[0]] = np.nan
        df_local[beginning_week[0]] = np.nan
        df_local[end_week[0]] = np.nan
        df_local[std_week[0]] = np.nan


        for row_idx, ts_list in df_local[columns[0]].items():
            hour_lst = []
            day_of_week_lst = []

            ts_list = pd.to_datetime(ts_list, unit='ms', errors='coerce')  # применили datetime
            hour_lst.append(ts_list.hour)  # строка часов
            day_of_week_lst.append(ts_list.dayofweek)  # строка дней недели

            df_local.loc[row_idx, hour_col_names] = np.median(hour_lst).astype(
                np.int)  # медианный час активности
            df_local.loc[row_idx, latest_visit] = np.max(hour_lst).astype(
                np.int)  # самое позднее время за период наблюдений
            df_local.loc[row_idx, earliest_visit] = np.min(hour_lst).astype(
                np.int)  # самое раннее время за период наблюдений
            df_local.loc[row_idx, std_hour] = np.std(hour_lst)  # разброс часов посещений время за период наблюдений

            df_local.loc[row_idx, dayofweek_col_names] = np.median(day_of_week_lst).astype(
                np.int)  # медианный день недели активности
            df_local.loc[row_idx, end_week] = np.max(day_of_week_lst).astype(
                np.int)  # самое поздний день за период наблюдений
            df_local.loc[row_idx, beginning_week] = np.min(day_of_week_lst).astype(
                np.int)  # самое ранний день за период наблюдений
            df_local.loc[row_idx, std_week] = np.std(day_of_week_lst)  # разброс дней посещений время за период наблюдений

        return df_local, output_column_names


# бинарные кодеры\декодеры и функция подсчета общей вероятности столбцов
def dummify_labels(df_in):
    """
    разбивает y на бинарные столбцы для модели (можно пихать таблицу целиком)
    """
    out_df = pd.get_dummies(df_in, prefix_sep='_')
    # получим age_, age_, age_, age_, age_, gender_, gender_,
    return out_df


def undummify_labels(df_dimmies, ignore_list=None, top=None, prefix_sep="_"):
    """
    возвращает бинарные столбцы к виду с множеством параметров (можно пихать таблиу целиком)
    # https://stackoverflow.com/questions/50607740/reverse-a-get-dummies-encoding-in-pandas
    """
    if ignore_list is None:
        ignore_list = ['user_json', ]

    # получили словарь имен столбцов параметров в виде {gender:True, age: True}
    cols2collapse = {}
    for item in df_dimmies.columns.tolist():
        if prefix_sep in item:
            if item not in ignore_list:
                cols2collapse[item.split(prefix_sep)[0]] = True
            else:
                cols2collapse[item] = False
        else:
            cols2collapse[item] = False

    series_list = []  # пустая строка - из которой потом сделаем dataframe
    prob_score_column = 'bestprob'

    for col, needs_to_collapse in cols2collapse.items():
        if needs_to_collapse:
            undummified = df_dimmies\
                .filter(like=col, axis=1)\
                .astype(np.float)
            if top is not None:
                best_probability = undummified\
                    .max(axis=1)\
                    .rename('_'.join([prob_score_column, col]))
                series_list.append(best_probability)
            undummified = undummified.idxmax(axis=1)\
                .apply(lambda x: x.split(prefix_sep, maxsplit=1)[1])\
                .rename(col)
            series_list.append(undummified)  # добавили колонку
        else:
            series_list.append(df_dimmies[col])  # добавили колонку которая не являлась бинарной обратно
    out_df = pd.concat(series_list, axis=1)  # собрали тиблицу из листа series

    if top is not None:
        cols_to_drop = out_df.filter(like=prob_score_column, axis=1).columns.tolist()
        #  колонки которые удалим
        comon_probability = out_df[cols_to_drop].prod(axis=1).rename(prob_score_column)
        #  подсчитали общую вероятность перемножением
        out_df[prob_score_column] = comon_probability
        #  присвоили
        out_columns = [x for x in out_df.columns.tolist() if x not in cols_to_drop]
        out_df = out_df[out_columns]
        #  дроп без дропа (чекер дроп не любит почемуто)
        out_df = out_df.sort_values(by=prob_score_column, axis=0, ascending=False)
        #  отсортировали по убыванию общей вероятности
        last_row_idx = int(len(out_df)*top+0.5)
        out_df[last_row_idx:][['gender', 'age']] = str('-')
        #  выбрали top % от всей массы
    return out_df


# функция схлапывающая по uid строки
def calc_common_probability(x, prediction, original_input_df, y_dummies_columns, agg_column):
    """
    схлапывает по uid предсказанного y: np.array колонки в среднее значение
    на выход датафрейм c предсказаниями исходными данными
    """

    all_processed_columns = y_dummies_columns + x.columns.tolist()
    # объеденили все колонки c бинарными столбцами
    out_df = pd.DataFrame()  # Создали табличку на выход

    temp_df = pd.DataFrame(np.concatenate((np.array(prediction), np.array(x)), axis=1))
    temp_df.columns = all_processed_columns
    # склееный обратно из y и X датафреймов, чтобы сохранить uid и индекс
    for c in temp_df[y_dummies_columns].columns:
        temp_df[c] = temp_df[c].astype(np.float)


    uid_mean_df = temp_df[y_dummies_columns
                          + [agg_column]]\
        .groupby([agg_column], as_index=True)\
        .mean()
    # cводная таблица где uid за индекс, а значения = mean()

    for idx, uid_orig in original_input_df[agg_column].items():
        # Перебираем все индексы и uid из первоначального датафрейма

        out_df.loc[idx, all_processed_columns] = uid_mean_df.loc[uid_orig]
        out_df.loc[idx, agg_column] = uid_orig
        # заполняем табличку по индексу оригинальной и значениями из сводной и оригинальной

    out_df = out_df.sort_values(by=agg_column, ascending=True)

    return out_df


# функция схлапывающая по uid строки
def recover_index(x, prediction, original_input_df, y_dummies_columns, agg_column):
    """
    схлапывает по uid предсказанного y: np.array колонки в среднее значение
    на выход датафрейм c предсказаниями исходными данными
    """
    assert len(x) == len(original_input_df)
    all_processed_columns = y_dummies_columns + x.columns.tolist()
    # объеденили все колонки c бинарными столбцами
    out_df = pd.DataFrame(data=None, columns=all_processed_columns)  # Создали табличку на выход
    out_df = out_df.reindex_like(original_input_df)
    out_df = out_df.reset_index()  # нужнй нам индекс теперь в 1й колонке

    temp_df = pd.DataFrame(np.concatenate((np.array(prediction), np.array(x)), axis=1))
    temp_df.columns = all_processed_columns
    # склееный обратно из y и X датафреймов, чтобы сохранить uid и индекс

    out_df[all_processed_columns] = temp_df
    out_df = out_df.set_index('index')  # Объявили колонку индекса - индексом

    return out_df[all_processed_columns]


numeric_columns = ['counter', 'hour', 'dayofweek']  # processed_df.columns.tolist()[4:]
model_params = {'gender_labels': 'gender',
                'age_labels': 'age',
                'uid': 'uid',
                'text': 'url',
                'numeric': numeric_columns,
                'test_size': 0.2,
                'random_seed': 43
                }


def get_text_data_func(x):
    result = x[model_params['text']]
    return result


def get_numeric_data_func(x):
    result = x[model_params['numeric']]
    return result


def project_accuracy(y_true, y_pred):
    compare = pd.DataFrame(y_true == y_pred)
    result = len(compare[(compare.iloc[:, 0] == True) & (compare.iloc[:, 1] == True)]) / len(compare)
    return result

#get_text_data = FunctionTransformer(lambda x: x[model_params['text']])
#get_numeric_data = FunctionTransformer(lambda x: x[model_params['numeric']])

