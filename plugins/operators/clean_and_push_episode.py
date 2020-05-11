from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
import time
import pandas as pd
import logging
from google.cloud import pubsub


class CleanAndPushEpisodeOperator(BaseOperator):
    template_fields = ("execution_date",)

    @apply_defaults
    def __init__(self,
                 topic_name,
                 source_path,
                 execution_date,
                 project_id,
                 gcp_conn_id,
                 *args, **kwargs):
        super(CleanAndPushEpisodeOperator, self).__init__(*args, **kwargs)
        self.topic_name = topic_name
        self.source_path = source_path
        self.execution_date = execution_date
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        date = self.execution_date.format(**context)
        clean_and_filtered_episodes = self.clean_and_filter_dataset(date)

        publisher = pubsub.PublisherClient()

        self.simulate_streaming(clean_and_filtered_episodes, publisher)

    def clean_and_filter_dataset(self, date):
        dates_by_episode = pd.date_range('2020-01-01', '2020-01-30')

        def get_date_for_episode(row):
            return dates_by_episode[(row['seasonNumber'] - 1) * 10 + row['episodeNumber'] - 1]

        episodes = pd.read_csv(self.source_path)
        episodes['episodeNumber'] = episodes['episode no.']
        episodes['seasonNumber'] = episodes['season no.']
        episodes['episodeName'] = episodes['episode name']
        episodes['characterName'] = episodes['name']
        episodes['line'] = episodes.replace({'\'': ''}, regex=True).line
        episodes = episodes.drop(columns=['season no.', 'episode no.', 'episode name', 'name'])
        episodes['date'] = episodes.apply(get_date_for_episode, axis=1)
        episodes_current_date = episodes[episodes['date'] == date]
        logging.info(f'currenDate {date}')
        logging.info(
            f'data extracted and filtered for season {episodes_current_date.loc[0].seasonNumber} and episode {episodes_current_date.loc[0].episodeNumber}')
        episodes_current_date = episodes_current_date.drop(columns=['date'])

        return episodes_current_date

    def simulate_streaming(self, episodes, publisher):
        index_count = 0
        for index, episode in episodes.iterrows():
            event = str(episode.values.flatten().tolist())
            event = event[1:-1]
            publisher.publish("projects/{}/topics/{}".format(self.project_id, self.topic_name), \
                              event.encode('utf-8'))
            if index_count == 50:
                logging.info(f'published {index_count} events to the topic {self.topic_name}')
                index_count = 0
                time.sleep(5)
            index_count = index_count + 1

        logging.info('simulation of streaming finished')
