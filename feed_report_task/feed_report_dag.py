import io
import os
import telegram
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from matplotlib.lines import Line2D

# Безопасное получение всех параметров из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
DB_HOST = os.getenv("DB_HOST", "http://clickhouse.lab.karpov.courses:8123")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Инициализация бота
bot = telegram.Bot(token=BOT_TOKEN)
chat_id = int(CHAT_ID) if CHAT_ID else None

# Дефолтные параметры DAG
default_args = {
    'owner': 'i.rusina',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 19),
}

# Интервал запуска DAG (каждый день в 08:00 по UT)
schedule_interval = '0 8 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_report_dag_IR():
    @task()
    def get_data():
        connection = {
            'host': DB_HOST,
            'database': DB_NAME,
            'user': DB_USER,
            'password': DB_PASSWORD
        }

        query = '''
        SELECT toDate(time) as date,
            count(distinct user_id) as dau,
            sum(action = 'view') as views,
            sum(action = 'like') as likes,
            likes/views as ctr
        FROM
            {db}.feed_actions
        WHERE
          toDate(time) between yesterday() - 7 AND yesterday()
        GROUP BY
          toDate(time)
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def send_text_message(df):
        yesterday_data = df.iloc[-1, :]
        text_report = (
            f"Вчера, {yesterday_data[0].date()}, ключевые метрики составляли:\n"
            f"DAU - {yesterday_data[1]},\n"
            f"Просмотры - {yesterday_data[2]},\n"
            f"Лайки - {yesterday_data[3]},\n"
            f"CTR - {round(yesterday_data[4] * 100, 2)}%."
        )
        bot.sendMessage(chat_id=chat_id, text=text_report)

    @task()
    def send_picture(df):
        df['views_thousands'] = df['views'] / 1000
        df['likes_thousands'] = df['likes'] / 1000
        df['ctr_percent'] = df['ctr'] * 100

        fig, axes = plt.subplots(3, figsize=(10, 15))
        fig.suptitle('Метрики за предыдущую неделю', fontsize=16, fontweight='bold', y=1.01)

        # График 1: Просмотры и лайки (с двумя осями)
        line1 = sns.lineplot(data=df, x='date', y='views_thousands', ax=axes[0],
                             marker='o', color='green', label='Просмотры (тыс.)')
        axes[0].set_title('Просмотры и лайки', fontsize=12)
        axes[0].set_xlabel('Дата')
        axes[0].set_ylabel('Просмотры (тыс.)', color='green')
        axes[0].tick_params(axis='y', labelcolor='green')
        axes[0].grid(True, alpha=0.1, linestyle='--')
        axes[0].spines['top'].set_visible(False)

        ax2 = axes[0].twinx()
        line2 = sns.lineplot(data=df, x='date', y='likes_thousands', ax=ax2,
                             marker='o', color='blue')
        ax2.set_ylabel('Лайки (тыс.)', color='blue')
        ax2.tick_params(axis='y', labelcolor='blue')
        ax2.spines['top'].set_visible(False)

        custom_lines = [Line2D([0], [0], color='green', marker='o', linestyle='-'),
                        Line2D([0], [0], color='blue', marker='o', linestyle='-')]
        axes[0].legend(custom_lines, ['Просмотры (тыс.)', 'Лайки (тыс.)'],
                       loc='upper right', frameon=True, fancybox=True, shadow=True)

        # График 2: DAU
        sns.lineplot(data=df, x='date', y='dau', ax=axes[1], marker='o',
                     color='blue', linewidth=2, markersize=8)
        axes[1].set_title('Количество уникальных пользователей', fontsize=12, fontweight='bold')
        axes[1].set_xlabel('Дата', fontsize=11)
        axes[1].set_ylabel('DAU', fontsize=11)
        axes[1].grid(True, alpha=0.3, linestyle='--')
        for spine in ['top', 'right']:
            axes[1].spines[spine].set_visible(False)
        for x, y in zip(df['date'], df['dau']):
            axes[1].annotate(f'{int(y):,}', (x, y), textcoords="offset points",
                             xytext=(0, 10), ha='center', fontsize=9,
                             bbox=dict(boxstyle='round,pad=0.2', facecolor='lightblue', alpha=0.3))

            # График 3: CTR
        sns.lineplot(data=df, x='date', y='ctr_percent', ax=axes[2], marker='o',
                     color='green', linewidth=2, markersize=8)
        axes[2].set_title('Кликабельность (CTR)', fontsize=12, fontweight='bold')
        axes[2].set_xlabel('Дата', fontsize=11)
        axes[2].set_ylabel('CTR (%)', fontsize=11)
        axes[2].grid(True, alpha=0.3, linestyle='--')

        for spine in ['top', 'right']:
            axes[2].spines[spine].set_visible(False)

        for x, y in zip(df['date'], df['ctr_percent']):
            axes[2].annotate(f'{y:.2f}%', (x, y), textcoords="offset points",
                             xytext=(0, 10), ha='center', fontsize=9,
                             bbox=dict(boxstyle='round,pad=0.2', facecolor='lightgreen', alpha=0.3))

        plt.tight_layout()
        plt.subplots_adjust(top=0.95)

        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=100, bbox_inches='tight', pad_inches=0.3)
        plot_object.seek(0)
        plot_object.name = 'report_plots.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # Pipeline
    df = get_data()
    send_text_message(df)
    send_picture(df)


feed_report_dag_IR = feed_report_dag_IR()