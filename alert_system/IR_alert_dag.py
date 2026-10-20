python
import os
import pandas as pd
import pandahouse as ph
import telegram
import seaborn as sns
import matplotlib.pyplot as plt
import io

from airflow.decorators import dag, task
from datetime import datetime, timedelta

# Безопасное получение параметров
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID")) if os.getenv("CHAT_ID") else None
GROUP_ID = int(os.getenv("GROUP_ID")) if os.getenv("GROUP_ID") else None
DB_HOST = os.getenv("DB_HOST", "http://clickhouse.lab.karpov.courses:8123")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

bot = telegram.Bot(token=BOT_TOKEN)

# Английские названия колонок (для логики)
metric_list = ['feed_dau', 'views', 'likes', 'CTR', 'message_dau', 'messages']

# Словарь для красивых русских подписей (для отображения)
metric_labels = {
    'feed_dau': 'Пользователи ленты',
    'views': 'Просмотры',
    'likes': 'Лайки',
    'CTR': 'CTR',
    'message_dau': 'Пользователи сообщений',
    'messages': 'Сообщения'
}


def check_anomaly_simple_diff(df, metric, threshold=0.35):
    current_ts = df['ts'].max()
    day_ago_ts = current_ts - pd.DateOffset(days=1)
    current_value = df[df['ts'] == current_ts][metric].iloc[0]

    day_ago_data = df[df['ts'] == day_ago_ts]
    if day_ago_data.empty:
        return 0, 0.0

    day_ago_value = day_ago_data[metric].iloc[0]
    diff = (current_value / day_ago_value - 1)
    is_alert = 1 if abs(diff) > threshold else 0
    return is_alert, round(diff * 100, 1)


def check_anomaly_quantiles(df, metric, a=3, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    iqr = df['q75'] - df['q25']
    df['lower_quantile'] = df['q25'] - iqr * a
    df['upper_quantile'] = df['q75'] + iqr * a

    df['lower_quantile'] = df['lower_quantile'].rolling(n, center=True, min_periods=1).mean()
    df['upper_quantile'] = df['upper_quantile'].rolling(n, center=True, min_periods=1).mean()

    current_value = df[metric].iloc[-1]
    if current_value > df['upper_quantile'].iloc[-1] or current_value < df['lower_quantile'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df[['lower_quantile', 'upper_quantile']]


def check_anomaly_three_sigmas(df, metric, sigmas=3, periods=5):
    df["mean"] = df[metric].shift(1).rolling(periods).mean()
    df["std"] = df[metric].shift(1).rolling(periods).std()

    df['lower_sigma'] = df["mean"] - df["std"] * sigmas
    df['upper_sigma'] = df["mean"] + df["std"] * sigmas

    df['lower_sigma'] = df['lower_sigma'].rolling(periods, center=True, min_periods=1).mean()
    df['upper_sigma'] = df['upper_sigma'].rolling(periods, center=True, min_periods=1).mean()

    current_value = df[metric].iloc[-1]
    if current_value > df['upper_sigma'].iloc[-1] or current_value < df['lower_sigma'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df[['lower_sigma', 'upper_sigma']]


def check_anomaly_ensemble(df, metric):
    is_alert_quantiles, df_quant = check_anomaly_quantiles(df.copy(), metric, a=3, n=4)
    is_alert_sigmas, df_sig = check_anomaly_three_sigmas(df.copy(), metric, sigmas=3, periods=5)
    is_alert_simple_diff, one_day_diff = check_anomaly_simple_diff(df, metric)

    if is_alert_quantiles + is_alert_sigmas + is_alert_simple_diff == 3:
        is_alert = 1
        confidence = "высокая"
    elif is_alert_quantiles + is_alert_sigmas + is_alert_simple_diff == 2:
        is_alert = 1
        confidence = "средняя"
    else:
        is_alert = 0
        text = 'no anomalies'

    if is_alert == 1:
        current = df[metric].iloc[-1]
        previous = df[metric].iloc[-2]
        last_value_diff = round((current / previous - 1) * 100, 1)
        if metric == "CTR":
            current = f'{round(current * 100, 2)}%'

        display_name = metric_labels.get(metric, metric)

        text = (f"⚠️ АНОМАЛИЯ: <b>{display_name}</b>\n"
                f"Значение на {df['hm'].iloc[-1]}: <b>{current}</b>\n"
                f"Вероятность: {confidence}\n"
                f"Δ за 15 мин: {last_value_diff}%\n"
                f"Δ за день: {one_day_diff}%\n"
                f'<a href="https://superset.lab.karpov.courses/superset/dashboard/8324/">🔗 ссылка</a> на дашборд\n')

    full_data = pd.concat([df.copy(), df_quant], axis=1)
    full_data = pd.concat([full_data, df_sig], axis=1)
    return is_alert, full_data, text


default_args = {
    'owner': 'i.rusina',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 3),
}

schedule_interval = '*/15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_dag_IR():
    @task()
    def get_data():
        connection = {
            'host': DB_HOST,
            'database': DB_NAME,
            'user': DB_USER,
            'password': DB_PASSWORD
        }
        query = '''
        with feed as(
            SELECT toStartOfFifteenMinutes(time) as ts,
            toDate(time) as date,
            formatDateTime(ts, '%R') as hm,
            count(distinct user_id) as feed_dau,
            sum(action = 'view') as views,
            sum(action = 'like') as likes,
            likes/views as CTR
            FROM {db}.feed_actions
            where toDate(time) >= today() - 2 AND time < toStartOfFifteenMinutes(now())
            group by ts, date, hm), 

        messages as (
            SELECT toStartOfFifteenMinutes(time) as ts,
            toDate(time) as date,
            formatDateTime(ts, '%R') as hm,
            count(distinct user_id) as message_dau,
            count(user_id) as messages
            FROM {db}.message_actions
            where toDate(time) >= today() - 2 AND time < toStartOfFifteenMinutes(now())
            group by ts, date, hm)

        select ts, date, hm, feed_dau, views, likes, CTR, message_dau, messages
        from feed f 
        Join messages m on f.ts = m.ts AND f.date = m.date AND f.hm = m.hm
        order by ts 
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df.to_dict()

    @task()
    def run_alerts(data):
        df = pd.DataFrame(data)
        for metric in metric_list:
            filtered_df = df[['ts', 'date', 'hm', metric]].copy()
            is_alert, data_plot, text = check_anomaly_ensemble(filtered_df, metric)

            if is_alert == 1:
                sns.set(rc={'figure.figsize': (15, 10)})
                plt.tight_layout()
                data_for_plot = data_plot.copy()

                display_name = metric_labels.get(metric, metric)

                ax = sns.lineplot(x=data_for_plot['ts'], y=data_for_plot[metric], label=display_name, linewidth=2.5,
                                  color='blue')
                ax = sns.lineplot(x=data_for_plot['ts'], y=data_for_plot['lower_sigma'], label='сигмы (ниж. гр.)',
                                  linewidth=1.5, color='red', linestyle='--')
                ax = sns.lineplot(x=data_for_plot['ts'], y=data_for_plot['upper_sigma'], label='сигмы (верх. гр.)',
                                  linewidth=1.5, color='green', linestyle='--')
                ax = sns.lineplot(x=data_for_plot['ts'], y=data_for_plot['lower_quantile'], label='квантили (ниж. гр.)',
                                  linewidth=1.5, color='red', linestyle='dotted')
                ax = sns.lineplot(x=data_for_plot['ts'], y=data_for_plot['upper_quantile'],
                                  label='квантили (верх. гр.)', linewidth=1.5, color='green', linestyle='dotted')

                ax.set_title(display_name, fontsize=12, fontweight='bold')
                ax.set_xlabel('время')
                ax.set_ylabel(display_name)
                ax.tick_params(axis='y')
                ax.set(ylim=(0, None))
                ax.grid(True, alpha=0.3, linestyle='--')

                plot_object = io.BytesIO()
                plt.savefig(plot_object, format='png', dpi=100, bbox_inches='tight', pad_inches=0.3)
                plot_object.seek(0)
                plot_object.name = 'alert_plot.png'

                if GROUP_ID:
                    bot.sendPhoto(chat_id=GROUP_ID, photo=plot_object, caption=text, parse_mode='HTML')
                    plot_object.seek(0)
                if CHAT_ID:
                    bot.sendPhoto(chat_id=CHAT_ID, photo=plot_object, caption=text, parse_mode='HTML')

                plt.close()

    df_dict = get_data()
    run_alerts(df_dict)


alert_dag_IR = alert_dag_IR()