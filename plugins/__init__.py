from airflow.plugins_manager import AirflowPlugin

import operators

class ContentNetflixPlugin(AirflowPlugin):
    name = "content_dialogs"
    operators = [
    	operators.CleanAndPushEpisodeOperator
    ]