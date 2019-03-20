import psycopg2, json, requests, sys
from datetime import datetime, timedelta
import ah_config


class AppbotAPI:

    def __init__(self):
        self.auth = (ah_config.get('appbot.user'), ah_config.get('appbot.key'))
        self.columns = ah_config.get('appbot.columns')
        self.app_ids = {}
        self.app_ids['Android'] = ah_config.get('appbot.app_ids.Android')
        self.app_ids['iOS'] = ah_config.get('appbot.app_ids.iOS')

    def get_reviews_for_date(self, date):
        return self.get_reviews_for_date_range(date, date)

    # date is a string in format YYYY-MM-DD
    # yields lists of reviews until none in range left
    def get_reviews_for_date_range(self, start_date, end_date):
        url = 'https://api.appbot.co/api/v2/apps/{0}/reviews?start={1}&end={2}&page={3}'

        for platform in self.app_ids:
            # get the first page
            formatted_url = url.format(self.app_ids[platform], start_date, end_date, 1)
            response = requests.get(formatted_url, auth=self.auth)
            print('Requested: {0}'.format(formatted_url))
            if response.status_code != 200:
                raise Exception('Error on page {0}'.format(1))

            response = response.json()

            for review in response['results']:
                yield review

            end_page = response['total_pages']

            for page in range(2, end_page + 1):
                formatted_url = url.format(self.app_ids[platform], start_date, end_date, page)
                response = requests.get(formatted_url, auth=self.auth)
                print('Requested: {0}'.format(formatted_url))
                if response.status_code != 200:
                    raise Exception('Error on page {0}'.format(page))

                response = response.json()
                for review in response['results']:
                    yield review


class Database:

    def __init__(self, appbot_instance):
        into_string = ','.join(appbot_instance.columns)
        self.sql = 'INSERT INTO customerreview.reviews (' \
                + into_string  \
                + ') VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
        self.appbot_instance = appbot_instance

    def _login(self):
        
        self.conn = psycopg2.connect(
                host=ah_config.get('database.endpoint'), 
                database=ah_config.get('database.database'), 
                user=ah_config.get('database.user'),
                password=ah_config.get('database.password')
                )

        self.cursor = self.conn.cursor()

    def _logout(self):
        self.cursor.close()
        self.conn.close()

    # updates the database with reviews from a given date
    def update(self, date):
        self._login()
        for review in self.appbot_instance.get_reviews_for_date(date):
            row = []
            for col in self.appbot_instance.columns:
                row.append(review[col])

            try:
                self.cursor.execute(self.sql, row)
            except Exception as e:
                print(e)
                continue
            print('Added new review')
        self.conn.commit()
        self._logout()


# updates with reviews from yesterday
def main():
    ah_config.initialize()

    yesterday = datetime.today() - timedelta(days=1)

    yesterday_string = yesterday.strftime('%Y-%m-%d')

    #print(yesterday_string)

    appbot = AppbotAPI()
    database = Database(appbot)
    database.update(yesterday_string)


if __name__ == '__main__':
    main()
