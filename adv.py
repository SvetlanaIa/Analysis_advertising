import datetime as dt
import json
import os

import pandas as pd
from loguru import logger


class Analysis:
    CLASS_CONFIG = {
        'AMO_CITY_FIELD_ID': 512318,
        'DRUPAL_UTM_FIELD_ID': 632884,
        'TILDA_UTM_SOURCE_FIELD_ID': 648158,
        'TILDA_UTM_MEDIUM_FIELD_ID': 648160,
        'TILDA_UTM_CAMPAIGN_FIELD_ID': 648310,
        'TILDA_UTM_CONTENT_FIELD_ID': 648312,
        'TILDA_UTM_TERM_FIELD_ID': 648314,
        'CT_UTM_SOURCE_FIELD_ID': 648256,
        'CT_UTM_MEDIUM_FIELD_ID': 648258,
        'CT_UTM_CAMPAIGN_FIELD_ID': 648260,
        'CT_UTM_CONTENT_FIELD_ID': 648262,
        'CT_UTM_TERM_FIELD_ID': 648264,
        'CT_TYPE_COMMUNICATION_FIELD_ID': 648220,
        'CT_DEVICE_FIELD_ID': 648276,
        'CT_OS_FIELD_ID': 648278,
        'CT_BROWSER_FIELD_ID': 648280,
        'AMO_ITEMS_2019_FIELD_ID': 562024,
        'AMO_ITEMS_2020_FIELD_ID': 648028,
    }
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    WEEK_OFFSET = dt.timedelta(hours=24 + 24 + 6)
    LEAD_UTM_FIELDS = [
        'source',
        'medium',
        'campaign',
        'content',
        'term'
    ]

    def __init__(self, config=None):
        self.CLASS_CONFIG = dict()
        if config:
            self.CLASS_CONFIG = config
        self.CLASS_CONFIG.update(Analysis.CLASS_CONFIG)
        self.transform_data = []

    def extract(self, file_name):
        with open(os.path.join(os.path.dirname(__file__), file_name),
                  encoding='utf-8') as f:
            data = json.load(f)
        return data

    def transform(self):
        data = self.extract(file_name)
        for row in data:
            self.transform_data.append(self.transform_row(row))

        self.logging_check(self.transform_data)

        return self.transform_data

    def transform_row(self, row):
        created_at_datetime = dt.datetime.fromtimestamp(row['created_at'])
        res = {
            'id': row['id'],
            'amo_updated_at': (None if 'updated_at' not in row else
                               row['updated_at']),
            'amo_trashed_at': (None if 'trashed_at' not in row else
                               row['trashed_at']),
            'amo_closed_at': (None if 'closed_at' not in row else
                              row['closed_at']),
            'amo_pipeline_id': row['pipeline_id'],
            'amo_status_id': row['status_id'],

            # какие данные должны быть в amo_items_2019, amo_items_2020?

            'created_at_bq_timestamp': created_at_datetime.strftime(
                self.TIME_FORMAT),
            'created_at_year': created_at_datetime.strftime('%Y'),
            'created_at_month': created_at_datetime.strftime('%m'),
            'created_at_week': ((created_at_datetime + self.WEEK_OFFSET)
                                .isocalendar()[1])
        }

        for field in self.CLASS_CONFIG:
            field_new = field.lower()[:(len(field)-9)]
            res[field_new] = self.get_custom_field(
                row, self.CLASS_CONFIG[field])

        for field in self.LEAD_UTM_FIELDS:
            res[f'lead_utm_{field}'] = self.get_lead_utm(
                res['drupal_utm'],
                res[f'ct_utm_{field}'],
                res[f'tilda_utm_{field}'],
                field)

        return res

    def get_custom_field(self, row, field_id):
        for custom_field in row['custom_fields_values']:
            if custom_field['field_id'] == field_id:
                return custom_field['values'][0].get('value', None)
        return None

    def get_lead_utm(self, drupal_utm, ct_utm, tilda_utm, field):
        if drupal_utm:
            drupal_utm_list = drupal_utm.split(', ')
            drupal_utm_dict = dict([item.split('=')
                                    for item in drupal_utm_list])
            if field in drupal_utm_dict:
                if field == 'source':
                    for item in ['=yandex', '=google']:
                        if item in drupal_utm:
                            return item[1:]
                elif field == 'medium':
                    for item in ['=context', '=context-cpc', '=search']:
                        if item in drupal_utm:
                            return item[1:]
                return drupal_utm_dict[field]
        elif ct_utm:
            return ct_utm
        return tilda_utm

    def logging_check(self, data):
        logger.add('info.log', mode='w')
        for res in data:
            for field in self.LEAD_UTM_FIELDS:
                if ((res[f'ct_utm_{field}'] and
                     res[f'ct_utm_{field}'] != res[f'lead_utm_{field}']) or
                    (res[f'tilda_utm_{field}'] and
                     res[f'tilda_utm_{field}'] != res[f'lead_utm_{field}'])):
                    logger.info(f'Конфликт utm_{field} в сделке {res["id"]}')

    def create_dataframe(self):
        self.transform()
        frame = pd.DataFrame(self.transform_data)
        return frame

    def load(self):
        frame = self.create_dataframe()
        frame.to_csv('result.tsv', sep='\t')


if __name__ == '__main__':
    file_name = 'amo_json_2020_40.json'
    adv = Analysis()
    load_inform = adv.extract(file_name)
    adv.load()
