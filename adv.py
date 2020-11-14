import datetime as dt
import json
import logging

import pandas as pd

from config import CONFIG


class Analysis():

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

    def __init__(self, config):
        self.config = config
        self.config.update(Analysis.CLASS_CONFIG)

    def extract(self, file_name):
        with open(file_name, encoding='utf-8') as f:
            data = json.load(f)
        return data

    def transform(self, tranform_row):
        result = []

        for row in tranform_row:  # преобоазование каждой записи
            res = {}
            # аналогичным образом добавить столбцы, которые не требуют обработки
            res['id'] = row['id']
            res['created_at'] = row['created_at']
            res['created_at_bq_timestamp'] = dt.datetime.fromtimestamp(
                row["created_at"]).strftime(self.config['TIME_FORMAT'])
            res['created_at_year'] = dt.datetime.fromtimestamp(
                row["created_at"]).strftime('%Y')
            res['created_a_month'] = dt.datetime.fromtimestamp(row["created_at"]).strftime(
                '%m')  

            # Неделя начинается в пятницу в 18:00 по московскому времени.
            # Первая неделя года - та, к которой относится первый четверг года.
            # сделала для 2020 года, вопрос 5 января 18 часов это уже 2 неделя?(откорректировать в зависимости от ответа)
            res['created_at_week'] = ((dt.datetime.fromtimestamp(
                row["created_at"])-dt.datetime(2020, 1, 1)+self.config['WEEK_OFFSET']).days // 7) + 1

            # Колонки из раздела 'custom_fields_values'
            for field in self.config['CUSTOM_FIELDS']:
                res[field] = self.get_custom_field(
                    row, self.config[f"{field.upper()}_FIELD_ID"])

            res['lead_utm_source'] = self.get_lead_utm_source(
                res['drupal_utm'], res['ct_utm_source'], res['tilda_utm_source'])
            # Проверка поля lead_utm_source
            if res['ct_utm_source'] is not None and res['ct_utm_source'] != res['lead_utm_source'] or res['tilda_utm_medium'] is not None and res['tilda_utm_medium'] != res['lead_utm_source']:
                logging.info(f'Конфликт utm_source в сделке {res["id"]}')

            result.append(res)
        return result

    # вытаcкиваем все , поля , которые нужны из  'custom_fields_values'
    def get_custom_field(self, row, field_id):
        for custom_field in row["custom_fields_values"]:
            if custom_field['field_id'] == field_id:
                return custom_field['values'][0]['value']
                # подумать много значений ?, возможно их тоже заменить на None
        return None

    def get_lead_utm_source(self, drupal_utm, ct_utm_source, tilda_utm_source):
        if drupal_utm is not None and 'source' in drupal_utm:
            if '=yandex' in drupal_utm:
                return 'yandex'
            return ''
        elif drupal_utm is not None:
            if ct_utm_source is not None:
                return ct_utm_source
            elif tilda_utm_source is not None:
                return tilda_utm_source
        return None

    def create_dataframe(self, dict):  # создания Dataframe (добавить столбцы)
        frame = pd.DataFrame(dict)
        return frame

    # def load(self):
    #     pass


if __name__ == '__main__':
    logging.basicConfig(filename="info.log", level=logging.INFO)
    file_name = "amo_json_2020_40.json"
    adv = Analysis(CONFIG)
    load_inform = adv.extract(file_name)
    transform_info = adv.transform(load_inform)
    frame = adv.create_dataframe(transform_info)
    print(frame['lead_utm_source'])
