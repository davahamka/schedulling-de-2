import tweepy
import time
from sqlalchemy import create_engine
import pandas as pd

counter = 0


def start_etl():
    class Listener(tweepy.StreamingClient):
        def on_tweet(self, tweet):
            global counter
            try:
                data = {
                    'tweet_id': tweet.data['id'],
                    'text': tweet.data['text'],
                    'user_id': tweet.author_id,
                    'created_at': tweet.created_at,
                    'source': tweet.source
                }
                counter += 1
                if counter % 1 == 0:
                    load(data)
                if counter < 10:
                    return True
                else:
                    stream.disconnect()
            except BaseException as e:
                print(e)
                time.sleep(5)

    # kurang ngeload data sqlite ke dalam folder
    engine = create_engine(
        'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

    def load(data):
        data_last = pd.DataFrame([data])
        data_last.to_sql('tweets', con=engine, if_exists='append', index=False)

    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAB0%2BjgEAAAAASvDflBSjrqYO5%2Fhq3TcbnAj3PIk%3D57eJ03STpdcVW6I2xK0Snv5LaPcqssQVVq20AlFUZIA8NHTAGW"

    stream = Listener(BEARER_TOKEN)
    stream.add_rules(tweepy.StreamRule("Gempa Cianjur"))
    stream.filter(tweet_fields=['author_id', 'created_at', 'source'])

    datasql = pd.read_sql_table('tweets', engine)
    print(datasql.tail())
