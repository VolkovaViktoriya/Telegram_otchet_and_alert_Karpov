import telegram
from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишет такси в питоне
from airflow.operators.python import get_current_context 
from airflow.decorators import dag, task
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime,  timedelta


# параметры DAG'a 
default_args = {
    'owner': 'v-volkova', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 10, 16)
    }
# отчет отправляется в телеграмм-канал каждый день в 11:00  
schedule_interval = '0 11 * * *' # cron-выражение, также можно использовать '@daily', '@weekly', а также timedelta

# id чата, в который необходимо отправлять в отчет 
chat_id = -769752736
# токен моего бота
my_token = '*85:AAH8LmgdaH6wz8EgZ61S2ZE2wAH27wzUSjg'


# создаем подключение к БД
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220920',
                      'user':'student', 
                      'password':'**'
                }


def plot_alone(y, x, title, name, chat_id,  bot):
    # Формирует график с одной линией, 
    # отправляет его в телеграм чат  с помощью бота 
    
    # Подготовка временной оси (метки)
    ax = sns.lineplot(x, y)
    plt.title(title)
    # Поворот меток, чтобы не накладывались
    ax.set_xticklabels(x, rotation=40, ha="right")
    # Опция, чтобы весь текст разместился на рисунке
    plt.tight_layout()
    # Выгружаем изображение в буфер и отттуда отправляем в телеграмм 
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = name
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

def plot_two(y_1, y_2, x, label_1, label_2, title, y_label,  name, chat_id, bot):
    # Формирует график с двумя линиями, 
    # отправляет его в телеграм чат  с помощью бота
    
    ax = sns.lineplot(x, y_1,  palette = 'blue', label =  label_1)
    ax = sns.lineplot(x, y_2, palette = 'orange', label = label_2)
    ax.set_xticklabels([x[i] for i in range(0,7)], rotation=40, ha="right")
    plt.title(title)
    plt.ylabel(y_label)
    plt.tight_layout()
    plt.legend()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = name
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

def stack_plot(x, labels,  title, y_label,  name, chat_id, bot, *args):
    # Формирует стековый график с долевыми вкладами, 
    # отправляет его в телеграм чат  с помощью бота

    fig, ax = plt.subplots()
    if (len(args) == 2) and  (len(labels) == 2):
        ax.stackplot(x, args[0], args[1], labels=labels)
    elif (len(args) == 4) and  (len(labels) == 4):
        ax.stackplot(x, args[0], args[1], args[2], args[3], labels=labels)
    else:
        raise ValueError('Размерности списков с векторами и их метками должны быть обе 2 или 4')
    plt.title(title)
    plt.ylabel(y_label)
    # Поворот меток, чтобы не накладывались
    ax.set_xticklabels(x, rotation=40, ha="right")
    plt.tight_layout()
    plt.legend()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = name
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def aaa_volk_report_all():

    @task()
    def extract_actions_gr():
        # формируем запрос по извлечению необходимых данных из таблицы по ленте новостей за предшествующую неделю, включая вчера,
        # извлекаем данные в  датафрейм
        q1 = """
            SELECT 
                count(distinct user_id) as DAU,
                sum(action = 'like') as likes,
                sum(action = 'view') as views,
                round(likes/views,2) as CTR,
                toDate(time) as event_date
            FROM {db}.feed_actions 
            GROUP BY  event_date
            HAVING toDate(time) between today()-7 and  today()-1
            ORDER BY event_date 
            """
        df = ph.read_clickhouse(q1, connection=connection)
        return df

    @task()
    def extract_mess_gr():
        # формируем запрос по извлечению необходимых данных из таблицы по сообщениям пользователей за предшествующую неделю, 
        # включая вчера,извлекаемм данные в  датафрейм
        q2 = """
            SELECT 
                count(distinct user_id) as DAU,
                count(user_id) as messages_sent,
                toDate(time) as event_date
            FROM {db}.message_actions
            GROUP BY  event_date
            HAVING toDate(time) between today()-7 and  today()-1
            ORDER BY event_date 
            """
        df = ph.read_clickhouse(q2, connection=connection)
        return df

    @task()
    def transform_report_send(df_cube_feed):
        # Функция формирует и отправляет в телеграмм 
        # первую часть отчета в соответствии с заданием 2
         
        # Получаем доступ к боту
        bot = telegram.Bot(token=my_token) 

        # Формируем строку с основными KPI  и отправляем в телеграмм 
        yesteday = datetime.now() - timedelta(days=1)
        date_string = yesteday.strftime('%Y-%m-%d')
        ctr = float(df_cube_feed['CTR'][df_cube_feed['event_date'] == date_string])
        kpi_str =  (f'Отчет за %s по ленте новостей:\n'
                    f'\t\tDAU                \t=    %d,\n'
                    f'\tЛайков          \t= %d,\n '
                    f'\tПросмотров = \t%d,\n '
                    f'\tCTR \t= {ctr}.' 
                    % (date_string, 
                       df_cube_feed['DAU'][df_cube_feed['event_date'] == date_string],
                       df_cube_feed['likes'][df_cube_feed['event_date'] == date_string],
                       df_cube_feed['views'][df_cube_feed['event_date'] == date_string])
                   )
        bot.sendMessage(chat_id=chat_id, text=kpi_str)
        
        # Подготовка временной оси (метки)
        x = [df_cube_feed['event_date'][i].strftime('%Y-%m-%d') for i in range(7) ] 
        
        # Отправка изображения c DAU
        plot_alone(df_cube_feed['DAU'], x, 
                   'Охват пользователей (DAU)', 'DAU_plot.png', chat_id, bot)
        
        #Отправка изображения c Просмотрами и лайками
        plot_two(df_cube_feed['views'], df_cube_feed['likes'], x, 
                 'views', 'likes', 'Просмотры  и лайки', 'Actions', 'actions_plot.png', chat_id, bot)
            
        #Отправка изображения c CTR
        plot_alone(df_cube_feed['CTR'], x, 
                   'Показатель CTR', 'CTR_plot.png', chat_id, bot)
    
    @task()
    def transform_report_send_p2(df_cube_feed):
        # Функция формирует и отправляет в телеграмм основные KPI и графики 
        # по сервису отправки сообщений для второй части отчета в соответствии с заданием 2
        
        bot = telegram.Bot(token=my_token) 

        # формируем строку с основными KPIи отправляем
        yesteday = datetime.now() - timedelta(days=1)
        yesteday_1 = datetime.now() - timedelta(days=2)
        date_string = yesteday.strftime('%Y-%m-%d')
        date_string_1 = yesteday_1.strftime('%Y-%m-%d')
        
        DAU = float(df_cube_feed['DAU'][df_cube_feed['event_date'] == date_string])
        DAU_1 = float(df_cube_feed['DAU'][df_cube_feed['event_date'] == date_string_1])
        DAU_pr = float(round((DAU-DAU_1)/DAU_1*100,2))
        if DAU_pr>=0:
            DAU_pr_str = f" %d \n (+{DAU_pr} процентов)\n"%(DAU)
        else:
            DAU_pr_str = f" %d \n ({DAU_pr} процентов)\n"%(DAU)
        DAU_pr_str
        
        Messages = float(df_cube_feed['messages_sent'][df_cube_feed['event_date'] == date_string])
        Messages_1 = float(df_cube_feed['messages_sent'][df_cube_feed['event_date'] == date_string_1])
        Messages_pr = float(round((Messages-Messages_1)/Messages_1*100,2))
        if Messages_pr>0:
            Messages_pr_str = f" %d \n (+{Messages_pr} процентов)\n"%(Messages)
        else:
            Messages_pr_str = f" %d \n ({Messages_pr} процентов)\n"%(Messages)
        Messages_pr_str
        
        kpi_str =  f'Отчет за %s по сервису отправки сообщений:\n\t\tDAU                       \t=       '% (date_string)
        kpi_str = kpi_str +DAU_pr_str+  '\n\t\tЧисло сообщений \t=  ' + Messages_pr_str
        
        bot.sendMessage(chat_id=chat_id, text=kpi_str)

        #Подготовка временной оси (метки)
        x = [df_cube_feed['event_date'][i].strftime('%Y-%m-%d') for i in range(7) ] 

        #Отправка изображения c DAU
        plot_alone(df_cube_feed['DAU'], x, 
                   'Охват пользователей (DAU)', 'DAU_plot.png', chat_id, bot)
            
        #Отправка изображения c 
        plot_alone(df_cube_feed['messages_sent'], x, 
                   'Число отправленных сообщений', 'CTR_plot.png', chat_id, bot)

   

    @task()

    def extract_actions_all():
        # подключились к БД и выполнили запрос по извлечению совокупных данных   
        q10 = """
                SELECT 
                    sum(actions) actions,
                    event_date, 
                    sum(DAU_act) DAU_act, 
                    sum(DAU_mes) DAU_mes,

                    sum(messages_sent) messages_sent,
                    source_a,
                    os_a,
                    source_m, 
                    os_m

                 FROM
                 (
                    SELECT 
                        count(user_id) as actions,
                        toDate(time) as event_date,
                        count(distinct user_id) as DAU_act,
                        MAX(source) as source_a, 
                        MAX(os) as os_a
                    FROM {db}.feed_actions 
                    GROUP BY  event_date, source, os 
                    HAVING toDate(time) between today()-7 and  today()-1
                    ORDER BY event_date

                 ) t1
                   JOIN
                     (SELECT 
                        count(user_id) as messages_sent,
                        MAX(source) as source_m, 
                        MAX(os) as os_m,
                        count(distinct user_id) as DAU_mes,
                        
                        toDate(time) as event_date
                    FROM {db}.message_actions
                    GROUP BY  event_date, source, os 
                    HAVING toDate(time) between today()-7 and  today()-1
                    ORDER BY event_date) t2 
                ON t1.event_date =t2.event_date
                GROUP BY  event_date, source_a, os_a, source_m, os_m
                ORDER BY  event_date
            """

        df = ph.read_clickhouse(q10, connection=connection)
        return df

    @task()
    def transform_report_send_p3(df_cube_feed):
        # Функция формирует и отправляет в телеграмм графики долей совокупно  
        # по ленте новостей и сервису оправки сообщений, в различных срезах,
        # для второй части отчета в соответствии с заданием 2

        bot = telegram.Bot(token=my_token) # получаем доступ

        y1 = pd.Series([1 for i in range(7)])   
        y2 = pd.Series([1 for i in range(7)])   
        y3 = pd.Series([1 for i in range(7)])   
        y4 = pd.Series([1 for i in range(7)])  
        y_all  = pd.Series([1 for i in range(7)])  
        x = pd.Series([1 for i in range(7)])   
 
        for i in range(7):
            yesteday = datetime.now() - timedelta(days=(7-i))
            date_string = yesteday.strftime('%Y-%m-%d')
            x[i] = date_string
            y1[i] = df_cube_feed['actions'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['source_a']=='organic')].sum()
            y2[i] = df_cube_feed['actions'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['source_a']=='ads')].sum()
            y3[i] = df_cube_feed['messages_sent'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['source_m']=='organic')].sum()
            y4[i] = df_cube_feed['messages_sent'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['source_m']=='ads')].sum()
            y_all[i] = y1[i]+y2[i]+y3[i]+y4[i]


        labels = ['action', 'messages']
        stack_plot(x, labels,  'Доля числа действий', 'Actions',  'actions_plot.png', chat_id, bot, 
                   (y1+y2)/y_all, (y3+y4)/y_all)    

        for i in range(7):
            yesteday = datetime.now() - timedelta(days=(7-i))
            date_string = yesteday.strftime('%Y-%m-%d')
            y1[i] = df_cube_feed['DAU_act'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['source_a']=='organic')].sum()
            y2[i] = df_cube_feed['DAU_act'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['source_a']=='ads')].sum()
            y3[i] = df_cube_feed['DAU_mes'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['source_m']=='organic')].sum()
            y4[i] = df_cube_feed['DAU_mes'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['source_m']=='ads')].sum()
            y_all[i] = y1[i]+y2[i]+y3[i]+y4[i]

        labels = ['DAU action', 'DAU messages']
        stack_plot(x, labels, 'Доли DAU', 'DAU', 'DAU_plot.png', chat_id, bot, 
                   (y1+y2)/y_all, (y3+y4)/y_all)    


        # Графики долей по срезам 
        labels = ['action_organic', 'action_ads', 'messages_organic', 'messages_ads']
        stack_plot(x, labels, 'Доля  числа действий', 'Actions', 'actions_stack_plot.png', chat_id, bot, 
                   y1/y_all, y2/y_all, y3/y_all, y4/y_all)    

        for i in range(7):
            yesteday = datetime.now() - timedelta(days=(7-i))
            date_string = yesteday.strftime('%Y-%m-%d')
            y1[i] = df_cube_feed['actions'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['os_a']=='iOS')].sum()
            y2[i] = df_cube_feed['actions'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['os_a']=='Android')].sum()
            y3[i] = df_cube_feed['messages_sent'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['os_m']=='iOS')].sum()
            y4[i] = df_cube_feed['messages_sent'][(df_cube_feed['event_date']==date_string)&(df_cube_feed['os_m']=='Android')].sum()
            y_all[i] = y1[i]+y2[i]+y3[i]+y4[i]

        labels = ['action_iOS', 'action_Android', 'messages_iOS', 'messages_Android']
        stack_plot(x, labels, 'Доля  числа действий', 'Actions', 'actions_stack_plot_1.png', chat_id, bot, 
                   y1/y_all, y2/y_all, y3/y_all, y4/y_all)    
        

    df_cube_feed = extract_actions_gr()
    transform_report_send(df_cube_feed)
    
    df_cube_mess = extract_mess_gr()
    transform_report_send_p2(df_cube_mess)
       
    df_cube_all = extract_actions_all()
    transform_report_send_p3(df_cube_all)
    
    
aaa_volk_report_v2 = aaa_volk_report_all()


