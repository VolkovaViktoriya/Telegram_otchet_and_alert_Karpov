import telegram

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context 
from airflow.decorators import dag, task

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime,  timedelta

# создаем подключение к БД
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220920',
                      'user':'student', 
                      'password':'**'
                }

# параметры для DAG'a
default_args = {
    'owner': 'v-volkova', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 10, 16)
    }

# запускается DAG каждые 15 минут
schedule_interval = '0,15,30,45  * * * *' 

# токен моего бота
my_token = '*85:AAH8LmgdaH6wz8EgZ61S2ZE2wAH27wzUSjg' 
# id чата, в который необходимо отправлять в отчет 
chat_id = -644906564

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def aaa_volk_alert_Itog():

    @task()
    def check_anomaly_and_plot(df_cube, chat_id, x_i='views', n_seas=96,  ntimes=3,  a_razm=1.5, a_sig=2): 
        # Для одной выбранной метрики выделяет трендовую и сезонную  составляющую, остатки
        # на их базе определяет, относится ли последнее наблюдение к выбросу.
        # Если да, формирует и отправляет в chat_id alert-сообщение и поясняющие рисунки c доверительными интервалами. 
        # df_cube - общий массив с показателями, x_i - название метрики, для которой проводится анализ на выбросы
        # n_seas - число наблюдений в одном периоде сезонности (равно 96 для одних суток при 15-минутных интервалах)
        # ntimes - сколько периодов сезонности отражать на графиках
        # a_razm - коэффициент для задания ширины интервала на базе квартильного размаха
        # a_sig - коэффициент для задания ширины интервала на базе std (при a_sig=2 это правило двух сигм)
        
        # Получаем доступ к боту
        bot = telegram.Bot(token=my_token) 
        
        # Формируем названия выделяемых компонент на базе названия метрики,содержащейся в строке x_i
        seas = "seas_"+ x_i
        series=df_cube[x_i]
        tre = "trend_"+x_i

        # Выделение тренда скользящим окном по периоду (размером n_seas=96 точек)
        rolling_mean_week = df_cube[x_i].rolling(window=n_seas).mean() 
        df_cube[tre] = series-rolling_mean_week
        
        # Выделение сезонной составляющей как усреднения по периодам 
        seasons_0 = df_cube.groupby('hm')['hm'].max()
        seasons_0.reset_index(drop=True, inplace=True)
        seasons = pd.concat([seasons_0, pd.Series([0 for i in range(len(seasons_0))])], 
                            axis=1, keys= ['hm','avg'], ignore_index=True )
        seasons.columns=['hm','avg']

        df_cube[seas] = pd.Series([0 for i in range(len(df_cube[x_i]))])

        for i in range(n1):
            seasons['avg'][i] = (df_cube[tre][df_cube['hm'] ==seasons['hm'][i]]).mean()
            df_cube[seas][df_cube['hm'] ==seasons['hm'][i]] = seasons['avg'][i]

        # Получение остатков ряда (после вычитания тренда и сезонной составляющей), 
        # вычисление на их основе доверительных  интервалов 
        # на основе квартильного размаха и среднеквадратичного отклонения (std = sig)
        reset = series - rolling_mean_week - df_cube[seas]
        rolling_std = reset.std() 
        df_cube["q25_"+x_i] = reset.quantile(0.25) 
        df_cube["q75_"+x_i] = reset.quantile(0.75) 
        df_cube["iqr_"+x_i] = df_cube["q75_"+x_i] - df_cube["q25_"+x_i]  
        df_cube["up_iqr_"+x_i] =  df_cube["q75_"+x_i] + a_razm*df_cube["iqr_"+x_i]
        df_cube["low_iqr_"+x_i] = df_cube["q25_"+x_i] - a_razm*df_cube["iqr_"+x_i] 
        upper_bond_sig = rolling_mean_week + df_cube[seas] +a_sig*rolling_std 
        lower_bond_sig = rolling_mean_week + df_cube[seas] -a_sig*rolling_std 

        # Проверка - алерт выдается, если нарушен хотя бы один 
        # (на сигмах и на квартилььном размахе) доверительный интервал
        
        is_alert = 0

        if df_cube[x_i].iloc[-1] < lower_bond_sig.iloc[-1] or df_cube[x_i].iloc[-1] > upper_bond_sig.iloc[-1]:
            is_alert += 1
        if reset.iloc[-1] < df_cube["low_iqr_"+x_i].iloc[-1] or reset.iloc[-1] > df_cube["up_iqr_"+x_i].iloc[-1]:
            is_alert += 1

        # Формируем сообщение об ошибке
        if (is_alert >0):
            if df_cube[x_i].iloc[-1] > df_cube[x_i].iloc[-2]:
                kod = '📈'
            else:
                kod = '📉'
            msg = ('''!АНОМАЛИЯ! ''' + kod
                   + ''' Метрика {x_i}: \n текущее значение  {current_val:.2f}\n 
                   отклонение от предыдущего значения  
                   {last_val_diff:.2%}'''.format(x_i=x_i, current_val=df_cube[x_i].iloc[-1],                                                                                                              last_val_diff=-(1-(df_cube[x_i].iloc[-1]/df_cube[x_i].iloc[-2]))))

            # Строим композиционный график на базе исходных значений ряда
            plt.figure(figsize=(15,5)) 
            # вносим на график значения ряда анализируемых значений
            ax =  plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):],
                           df_cube[x_i][(len(df_cube[x_i])-ntimes*n_seas):],  "r",    
                           label=" Actual values") 
            # вносим на график значения ряда, представляющего выделенный тренд  
            ax =  plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):], 
                           rolling_mean_week[(len(df_cube[x_i])-ntimes*n_seas):], "y",  
                           label=" trend ") 
            # вносим на график значения ряда, представляющего прогноз (как сумму тренда и сезонной составляющей) 
            ax =  plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):], 
                           (df_cube[seas]+rolling_mean_week)[(len(df_cube[x_i])-ntimes*n_seas):], "b",  
                           label="Predict for "+x_i) 
            # вносим на график значения доверительного интервала для прогноза  
            ax =  plt.plot( df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):],
                           upper_bond_sig[(len(df_cube[x_i])-ntimes*n_seas):],"g--",  
                           label="Upper Bond / Lower Bond by by {}sig".format(a_sig)) 
            ax =  plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):], 
                           lower_bond_sig[(len(df_cube[x_i])-ntimes*n_seas):],"g--") 

            plt.legend(loc="upper left") 
            plt.grid(True)
            plt.title(x_i+" predict and boundary\n ") 
            plt.ylabel(x_i)
            plt.tight_layout()
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'a_plot.png'
            plt.close()


            # Строим композиционный график на базе выделенных остатков ряда
            plt.figure(figsize=(15,5)) 
            plt.title("Reset for "+x_i+" and boundary\n ") 
            plt.ylabel("Reset")
            plt.tight_layout()
            nul_vec = pd.Series([0 for i in range(len(reset))])

            # вносим на график значения остатков ряда
            ax = plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):],
                          reset[(len(df_cube[x_i])-ntimes*n_seas):], "r",  
                          label="REset for actual values") 
            # вносим на график значения доверительного интервала для остатков, полученного на базе std  
            ax = plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):], 
                          (nul_vec+a_sig*rolling_std)[(len(df_cube[x_i])-ntimes*n_seas):],"g--", 
                          label="Upper Bond / Lower Bond by {}sig".format(a_sig) ) 
            ax = plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):], 
                          (nul_vec-a_sig*rolling_std)[(len(df_cube[x_i])-ntimes*n_seas):],"g--") 

            # вносим на график значения доверительного интервала для остатков, полученного на базе квартильного размаха  
            ax =  plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):],
                           df_cube["up_iqr_"+x_i][(len(df_cube[x_i])-ntimes*n_seas):],"b--", 
                           label="Upper Bond / Lower Bond by {}iqr".format(a_razm)) 
            ax = plt.plot(df_cube['ts'][(len(df_cube[x_i])-ntimes*n_seas):], 
                          df_cube["low_iqr_"+x_i][(len(df_cube[x_i])-ntimes*n_seas):],"b--") 

            plt.legend(loc="upper left") 
            plt.grid(True)
            plot_object1 = io.BytesIO()
            plt.savefig(plot_object1)
            plot_object1.seek(0)
            plot_object1.name = 'e_plot.png'
            plt.close()
            
            # Отправляем сформированные сообщение и графики в чат
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object1)

        return is_alert

    @task()
    def extract_data_act():
    # Подключаеемся к БД и выполняем запрос по извлечению числа уникальных пользователей (в ленте новостей) 
    # и их действий (в срезах по типу действий и типу os) в интервалах по 15 минут за последние 3 недели 
        q1 = """
                 SELECT
                      toStartOfFifteenMinutes(time) as ts
                    , toDate(ts) as date
                    , formatDateTime(ts, '%R') as hm
                    , uniqExact(user_id) as users_lenta
                    , countIf(user_id, (action='view')) as views
                    , countIf(user_id, action='like') as likes
                    , countIf(user_id, os='iOS') as actions_iOS
                    , countIf(user_id, os=='Android') as actions_Android
                    , round(likes/views,2) as CTR
                FROM {db}.feed_actions
                WHERE ts >=  today()-7*3 and ts < toStartOfFifteenMinutes(now())
                GROUP BY ts, date, hm
                ORDER BY ts 
             """
        df = ph.read_clickhouse(q1, connection=connection)
        return df

    @task()
    def extract_data_mess():
    # Подключаеемся к БД и выполняем запрос по извлечению числа уникальных пользователей (в сервисе отправки сообщений) 
    # и их действий (в срезе по типу os) в интервалах по 15 минут за последние 3 недели 
        q2 = """
                     SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(ts) as date
                        , formatDateTime(ts, '%R') as hm
                        , uniqExact(user_id) as users_mess
                        , countIf(user_id, os='iOS') as messages_iOS
                        , countIf(user_id, os=='Android') as messages_Android
                    FROM {db}.message_actions
                    WHERE ts >=  today()-7*2 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts 
             """
        df = ph.read_clickhouse(q2, connection=connection)
        return df
    
    
    # Выгрузка и проверка на аномальность метрик по ленте новостей
    df_cube = extract_data_act()
    check_anomaly_and_plot(df_cube, chat_id,  x_i='users_lenta')
    check_anomaly_and_plot(df_cube, chat_id,  x_i='views')
    check_anomaly_and_plot(df_cube, chat_id,  x_i='likes')
    check_anomaly_and_plot(df_cube, chat_id,  x_i='CTR')
    check_anomaly_and_plot(df_cube, chat_id,  x_i='actions_iOS')
    check_anomaly_and_plot(df_cube, chat_id,  x_i='actions_Android')
    
    # Выгрузка и проверка на аномальность метрик по по сервису отправки сообщений
    df_cube_mess = extract_data_mess()
    check_anomaly_and_plot(df_cube_mess, chat_id,  x_i='messages_iOS')
    check_anomaly_and_plot(df_cube_mess, chat_id,  x_i='messages_Android')
    check_anomaly_and_plot(df_cube_mess, chat_id,  x_i='users_mess')

aaa_volk_alert = aaa_volk_alert_Itog()


