import datetime as dt
import json
from loguru import logger

import pandas as pd

from config import CONFIG


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
        'CT_BROWSER_FIELD_ID': 648280
    }
    LEAD_UTM_FIELDS = [
        'source',
        'medium',
        'campaign',
        'content',
        'term'
        ]

    def __init__(self, config):
        self.config = config
        self.config.update(Analysis.CLASS_CONFIG)

    def extract(self, file_name):
        with open(file_name, encoding='utf-8') as f:
            data = json.load(f)
        return data

    def transform(self, tranform_row):
        result = []

        for row in tranform_row:
            res = {}
            res['id'] = row['id']
            res['updated_at'] = row['updated_at']
            res['trashed_at'] = row['trashed_at'] if 'trashed_at' in row.keys() else None
            res['amo_pipeline_id'] = row['pipeline_id']
            res['amo_status_id'] = row['status_id']
            res['amo_closed_at'] = row['closed_at']

            # какие данные должны быть в amo_items_2019, amo_items_2020?

            res['created_at_bq_timestamp'] = dt.datetime.fromtimestamp(
                row['created_at']).strftime(self.config['TIME_FORMAT'])
            res['created_at_year'] = dt.datetime.fromtimestamp(
                row['created_at']).strftime('%Y')
            res['created_a_month'] = dt.datetime.fromtimestamp(
                row['created_at']).strftime('%m')
            res['created_at_week'] = dt.datetime.fromtimestamp(
                row['created_at']).isocalendar()[1]

            for field in self.CLASS_CONFIG:
                field_new = field.lower()[:(len(field)-9)]
                res[field_new] = self.get_custom_field(row, self.config[field])

            for field in self.LEAD_UTM_FIELDS:
                res[f'lead_utm_{field}'] = self.get_lead_utm(
                    res['drupal_utm'], res[f'ct_utm_{field}'], res[f'tilda_utm_{field}'], field)

            result.append(res)
        return result

    def get_custom_field(self, row, field_id):
        for custom_field in row['custom_fields_values']:
            if custom_field['field_id'] == field_id:
                return custom_field['values'][0]['value']
        return None

    # по моим ощущениям логика в задании прописана не для поля drupal_utm(field_id = 632884),
    #  а для field_id = 648168
    def get_lead_utm(self, drupal_utm, ct_utm, tilda_utm, field):
        if drupal_utm is not None:
            if field in drupal_utm:
                # разобраться, пока не совсем понимаю почему только яндекс
                if '=yandex' in drupal_utm and field == 'source':
                    return 'yandex'
                elif '=context' in drupal_utm and field == 'medium':
                    return 'context'
                # не понимаю почему конец вывода:ближайшим символом '& или концом строки',
                # возможно имеется ввиду запятая(сделала пока запятую)
                drupal_utm_res = drupal_utm[drupal_utm.find(
                    f'{field}=')+len(f'{field}='):]
                if ',' in drupal_utm_res:
                    return drupal_utm_res[:(drupal_utm_res.find(','))]
                return drupal_utm_res
            elif ct_utm is not None:
                return ct_utm
        elif tilda_utm is not None:
            return tilda_utm
        return None

    def logging_check(self, data):
        logger.add('info.log', mode='w')
        for res in data:
            for field in self.LEAD_UTM_FIELDS:
                if res[f'ct_utm_{field}'] is not None and res[f'ct_utm_{field}'] != res[f'lead_utm_{field}'] or res[f'tilda_utm_{field}'] is not None and res[f'tilda_utm_{field}'] != res[f'lead_utm_{field}']:
                    logger.info(f'Конфликт utm_{field} в сделке {res["id"]}')

    def create_dataframe(self, dict):
        frame = pd.DataFrame(dict)
        return frame

    def load(self, frame):
        frame.to_csv('result.tsv', sep='\t')


if __name__ == '__main__':
    file_name = 'amo_json_2020_40.json'
    adv = Analysis(CONFIG)
    load_inform = adv.extract(file_name)
    transform_info = adv.transform(load_inform)
    logging_check = adv.logging_check(transform_info)
    frame = adv.create_dataframe(transform_info)
    adv.load(frame)
