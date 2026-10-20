import io
import os
import telegram
import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt

from airflow.decorators import dag, task
from datetime import datetime, timedelta

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
    'start_date': datetime(2024, 2, 26),
}

# Интервал запуска DAG (каждый день в 08:00)
schedule_interval = '0 8 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def app_report_dag_IR():
    @task()
    def get_data():
        connection = {
            'host': DB_HOST,
            'database': DB_NAME,
            'user': DB_USER,
            'password': DB_PASSWORD
        }

        total_query = ''' 
                -- собираем все данные за 2 недели
                with all_data as (
                  SELECT
                    toDate(time) as date,
                    user_id,
                    source,
                    os
                FROM
                  {db}.feed_actions
                WHERE
                  toDate(time) between yesterday() - 13
                  AND yesterday()
                UNION all
                SELECT
                  toDate(time) as date,
                  user_id,
                  source,
                  os
                FROM
                  {db}.message_actions
                WHERE
                  toDate(time) between yesterday() - 13
                  AND yesterday()
                ),

                -- собираем данные о первом посещении
                first_visits as (
                  SELECT
                    user_id,
                    min(toDate(time)) as first_visit_date
                  FROM (
                    SELECT toDate(time) as time, user_id FROM {db}.feed_actions
                    UNION ALL
                    SELECT toDate(time) as time, user_id FROM {db}.message_actions
                  ) as all_history
                  GROUP BY user_id
                ),

                -- собираем данные о пользователях по дням
                active_users as (
                  select
                    date,
                    user_id,
                    any(source) as source,
                    any(os) as os,
                    max(if(f.first_visit_date = a.date, 1, 0)) as new_user
                  FROM all_data a
                  left join first_visits f on a.user_id = f.user_id
                  group by
                    a.date,
                    a.user_id
                )
                --считаем нужные показатели
                select
                  date,
                  count(user_id) as total_dau,
                  countIf(source != 'organic') as adv,
                  countIf(source = 'organic') as organic,
                  countIf(os = 'Android') as android,
                  countIf(os = 'iOS') as iOS,
                  sum(new_user) as new_users, 
                  sum(if(new_user = 1 and source != 'organic', 1, 0)) as new_adv_users
                from
                  active_users
                group by
                  date
                '''
        total_df = ph.read_clickhouse(total_query, connection=connection)

        feed_query = '''
                SELECT toDate(time) as date,
                    count(distinct user_id) as feed_dau,
                    sum(action = 'view') as views,
                    sum(action = 'like') as likes,
                    likes/views as ctr,
                    views/feed_dau as views_per_user,
                    likes/feed_dau as likes_per_user
                FROM
                    {db}.feed_actions
                WHERE
                  toDate(time) between yesterday() - 13 AND yesterday()
                GROUP BY
                  toDate(time)
                '''
        feed_df = ph.read_clickhouse(feed_query, connection=connection)

        message_query = '''
                SELECT toDate(time) as date,
                    count(distinct user_id) as message_dau,
                    count(user_id) as messages,
                    messages/ message_dau as messages_per_user
                FROM
                    {db}.message_actions
                WHERE
                  toDate(time) between yesterday() - 13 AND yesterday()
                GROUP BY
                  toDate(time)
                '''
        message_df = ph.read_clickhouse(message_query, connection=connection)

        return {
            'total': total_df.to_dict(),
            'feed': feed_df.to_dict(),
            'message': message_df.to_dict()
        }

    @task()
    def send_text_message(data):
        total_df = pd.DataFrame(data['total'])
        feed_df = pd.DataFrame(data['feed'])
        message_df = pd.DataFrame(data['message'])

        def calculate_diff(current, previous):
            if previous == 0:
                return "⚪ N/A"
            res = round((current / previous - 1) * 100, 1)
            if res > 0:
                return f"🟢 ↑ +{res}%"
            elif res < 0:
                return f"🔴 ↓ {res}%"
            else:
                return f"⚪ {res}%"

        def calculate_value_with_diff(df, column):
            yesterday = df[column].iloc[-1]
            previous_day_diff = calculate_diff(yesterday, df[column].iloc[-2])
            week_ago_diff = calculate_diff(yesterday, df[column].iloc[-8])
            return yesterday, previous_day_diff, week_ago_diff

        # общие показатели
        dau, dau_day_diff, dau_week_diff = calculate_value_with_diff(total_df, 'total_dau')
        adv, adv_day_diff, adv_week_diff = calculate_value_with_diff(total_df, 'adv')
        new, new_day_diff, new_week_diff = calculate_value_with_diff(total_df, 'new_users')
        new_adv, new_adv_day_diff, new_adv_week_diff = calculate_value_with_diff(total_df, 'new_adv_users')
        android = total_df['android'].iloc[-1]
        ios = total_df['iOS'].iloc[-1]
        and_share = round(android / (android + ios) * 100, 1)
        ios_share = round(ios / (android + ios) * 100, 1)

        # лента
        feed_dau, feed_dau_day_diff, feed_dau_week_diff = calculate_value_with_diff(feed_df, 'feed_dau')
        views, views_day_diff, views_week_diff = calculate_value_with_diff(feed_df, 'views')
        likes, likes_day_diff, likes_week_diff = calculate_value_with_diff(feed_df, 'likes')
        ctr, ctr_day_diff, ctr_week_diff = calculate_value_with_diff(feed_df, 'ctr')
        views_per_user, views_per_user_day_diff, views_per_user_week_diff = calculate_value_with_diff(feed_df,
                                                                                                      'views_per_user')
        likes_per_user, likes_per_user_day_diff, likes_per_user_week_diff = calculate_value_with_diff(feed_df,
                                                                                                      'likes_per_user')

        # сообщения
        message_dau, message_dau_day_diff, message_dau_week_diff = calculate_value_with_diff(message_df, 'message_dau')
        message, message_day_diff, message_week_diff = calculate_value_with_diff(message_df, 'messages')
        messages_per_user, messages_per_user_day_diff, messages_per_user_week_diff = calculate_value_with_diff(
            message_df, 'messages_per_user')

        REPORT_DATE = total_df.iloc[-1, 0].date()
        day_diff = 'ДΔ'
        week_diff = 'НΔ'

        text_report = (
            f"<b>📊 ОТЧЕТ О РАБОТЕ ПРИЛОЖЕНИЯ НА {REPORT_DATE}</b>\n\n"

            f"\n<b>👥 ОБЩИЕ ПОКАЗАТЕЛИ</b>\n"
            f"├─ 👤 <b>DAU:</b> {dau:,} чел.\n"
            f"│   ├─ {day_diff} {dau_day_diff} | {week_diff} {dau_week_diff}\n"
            f"├─ 📢 <b>из них привлечены\n"
            f"│   │  рекламой:</b> {adv:,} чел.\n"
            f"│   ├─ {day_diff} {adv_day_diff} | {week_diff} {adv_week_diff}\n"
            f"├─ 🆕 <b>Новые:</b> {new:,} чел.\n"
            f"│   ├─ {day_diff} {new_day_diff} | {week_diff} {new_week_diff}\n"
            f"├─ 📢 <b>Новые рекламные:</b> {new_adv:,} чел.\n"
            f"│   ├─ {day_diff} {new_adv_day_diff} | {week_diff} {new_adv_week_diff}\n"
            f"└─ 📱 <b>Платформы:</b> 🤖 {and_share}% | 🍎 {ios_share}%\n"

            f"\n<b>📱 ЛЕНТА</b>\n"
            f"├─ 👤 <b>DAU:</b> {feed_dau:,} чел.\n"
            f"│   ├─ {day_diff} {feed_dau_day_diff} | {week_diff} {feed_dau_week_diff}\n"
            f"├─ 👁 <b>Просмотры:</b> {views:,}\n"
            f"│   ├─ {day_diff} {views_day_diff} | {week_diff} {views_week_diff}\n"
            f"├─ ❤️ <b>Лайки:</b> {likes:,}\n"
            f"│   ├─ {day_diff} {likes_day_diff} | {week_diff} {likes_week_diff}\n"
            f"├─ 📊 <b>CTR:</b> {round(ctr * 100, 2)}%\n"
            f"│   ├─ {day_diff} {ctr_day_diff} | {week_diff} {ctr_week_diff}\n"
            f"├─ 👁/👤 <b>Просм. на пользов.:</b> {round(views_per_user, 1)}\n"
            f"│   ├─ {day_diff} {views_per_user_day_diff} | {week_diff} {views_per_user_week_diff}\n"
            f"└─ ❤️/👤 <b>Лайков на пользователя:</b> {round(likes_per_user, 1)}\n"
            f"    ├─ {day_diff} {likes_per_user_day_diff} | {week_diff} {likes_per_user_week_diff}\n"

            f"\n<b>💬 СООБЩЕНИЯ</b>\n"
            f"├─ 👤 <b>DAU:</b> {message_dau:,} чел.\n"
            f"│   ├─ {day_diff} {message_dau_day_diff} | {week_diff} {message_dau_week_diff}\n"
            f"├─ 💬 <b>Всего сообщений:</b> {message:,}\n"
            f"│   ├─ {day_diff} {message_day_diff} | {week_diff} {message_week_diff}\n"
            f"└─ 💬/👤 <b>На пользователя:</b> {round(messages_per_user, 1)}\n"
            f"    ├─ {day_diff} {messages_per_user_day_diff} | {week_diff} {messages_per_user_week_diff}\n\n"
            f"<b>{day_diff} - к предыдущему дню, {week_diff} - за неделю</b>"
        )
        bot.sendMessage(chat_id=chat_id, text=text_report, parse_mode="HTML")

    @task()
    def send_picture(data):
        total_df = pd.DataFrame(data['total'])
        feed_df = pd.DataFrame(data['feed'])
        message_df = pd.DataFrame(data['message'])

        fig, axes = plt.subplots(5, figsize=(10, 25))
        fig.suptitle('Динамика показателей за последние две недели', fontsize=16, fontweight='bold', y=0.99)

        # График 1: Количество активных пользователей
        sns.lineplot(data=total_df, x='date', y='total_dau', ax=axes[0],
                     marker='o', color='green', label='приложение')
        sns.lineplot(data=feed_df, x='date', y='feed_dau', ax=axes[0],
                     marker='o', color='blue', label='лента')
        axes[0].set_title('Активных пользователей', fontsize=12, fontweight='bold')
        axes[0].set_xlabel('Дата')
        axes[0].set_ylabel('человек')
        axes[0].tick_params(axis='y')
        axes[0].grid(True, alpha=0.3, linestyle='--')

        # График 2: Новые пользователи
        sns.lineplot(data=total_df, x='date', y='new_users', ax=axes[1],
                     marker='o', color='green', label='новых всего')
        sns.lineplot(data=total_df, x='date', y='new_adv_users', ax=axes[1],
                     marker='o', color='blue', label='привлечены рекламой')
        axes[1].set_title('Новых пользователей', fontsize=12, fontweight='bold')
        axes[1].set_xlabel('Дата')
        axes[1].set_ylabel('человек')
        axes[1].tick_params(axis='y')
        axes[1].grid(True, alpha=0.3, linestyle='--')

        # График 3: Сообщения
        sns.lineplot(data=message_df, x='date', y='message_dau', ax=axes[2], marker='o',
                     color='green', linewidth=2, markersize=8, label='Отправителей')
        axes[2].set_title('Сообщения', fontsize=12, fontweight='bold')
        axes[2].set_xlabel('Дата', fontsize=11)
        axes[2].set_ylabel('Количество отправителей, чел.', fontsize=11, color='green')
        axes[2].tick_params(axis='y', labelcolor='green')
        axes[2].grid(True, alpha=0.3, linestyle='--')
        # Вторая ось для столбцов
        ax2 = axes[2].twinx()
        ax2.bar(message_df['date'], message_df['messages'],
                alpha=0.1, color='green', label='Всего сообщений', width=0.7)
        ax2.set_ylabel('Количество сообщений', fontsize=11, color='blue')
        ax2.tick_params(axis='y', labelcolor='blue')
        # Убрал жесткий лимит, чтобы столбцы были видны всегда
        # Объединенная легенда
        lines1, labels1 = axes[2].get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        axes[2].legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        # График 4: Действий на одного пользователя
        sns.lineplot(data=feed_df, x='date', y='views_per_user', ax=axes[3],
                     marker='o', color='green', label='просмотров на пользователя')
        sns.lineplot(data=feed_df, x='date', y='likes_per_user', ax=axes[3],
                     marker='o', color='blue', label='лайков на пользователя')
        sns.lineplot(data=message_df, x='date', y='messages_per_user', ax=axes[3],
                     marker='o', color='orange', label='сообщений на пользователя')
        axes[3].set_title('Действий на одного пользователя', fontsize=12, fontweight='bold')
        axes[3].set_xlabel('Дата')
        axes[3].set_ylabel('штук')
        axes[3].tick_params(axis='y')
        axes[3].grid(True, alpha=0.3, linestyle='--')

        # График 5: пользователи в разбивке по операционным системам
        dates = total_df['date']
        android_vals = total_df['android']
        ios_vals = total_df['iOS']
        x = range(len(dates))

        axes[4].bar(x, android_vals, color='green', alpha=0.8, edgecolor='black', linewidth=0.5, label='Android')
        axes[4].bar(x, ios_vals, bottom=android_vals, color='blue', alpha=0.8, edgecolor='black', linewidth=0.5,
                    label='iOS')

        axes[4].set_title('Пользователи по операционным системам', fontsize=12, fontweight='bold')
        axes[4].set_xlabel('Дата', fontsize=11)
        axes[4].set_ylabel('Количество пользователей', fontsize=11)
        formatted_dates = [d.strftime('%d.%m') for d in dates]
        axes[4].set_xticks(x)
        axes[4].set_xticklabels(formatted_dates, ha='right')

        y_max = max([a + i for a, i in zip(android_vals, ios_vals)])
        axes[4].set_ylim(0, y_max * 1.15)
        axes[4].grid(True, alpha=0.3, linestyle='--')
        axes[4].legend(loc='upper left', fontsize=9)

        # Добавляем общее количество сверху
        for i, (android, ios) in enumerate(zip(android_vals, ios_vals)):
            total = android + ios
            axes[4].text(i, total + 100, f'{int(total):,}',
                         ha='center', va='bottom', fontsize=8, fontweight='bold')

        # Добавляем доли внутри столбцов
        for i, (android, ios) in enumerate(zip(android_vals, ios_vals)):
            total = android + ios
            if android > 0:
                axes[4].text(i, android / 2, f'{(android / total) * 100:.0f}%',
                             ha='center', va='center', color='white', fontweight='bold', fontsize=8)
            if ios > 0:
                axes[4].text(i, android + ios / 2, f'{(ios / total) * 100:.0f}%',
                             ha='center', va='center', color='white', fontweight='bold', fontsize=8)

        plt.tight_layout()
        plt.subplots_adjust(top=0.95)

        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=100, bbox_inches='tight', pad_inches=0.3)
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # Pipeline
    data = get_data()
    send_text_message(data)
    send_picture(data)


app_report_dag_IR = app_report_dag_IR()
    
    
    







