import pytest

from adv import Analysis
from config import CONFIG

class TestClass:
    def test_fields(self):
        file_name = 'amo_json_2020_40.json'
        adv = Analysis(CONFIG)
        data = adv.extract(file_name)
        transform_info = adv.transform(data)

        assert transform_info[0]['id'] == data[0]['id'], 'Проверить id'
        assert transform_info[0]['created_at_year'] == '2020', 'Неверный расчет created_at_year'
        assert transform_info[0]['created_a_month'] == '10', 'Неверный расчет created_a_month'
        assert transform_info[0]['created_at_week'] == 41, 'Неверный расчет created_at_week'
        assert transform_info[0]['lead_utm_source'] == None, 'Неверный расчет lead_utm_source'
        assert transform_info[0]['lead_utm_medium'] == None, 'Неверный расчет lead_utm_medium'
        assert transform_info[0]['lead_utm_campaign'] == None, 'Неверный расчет lead_utm_campaign'
        assert transform_info[0]['lead_utm_content'] == None, 'Неверный расчет lead_utm_content'
        assert transform_info[0]['lead_utm_term'] == None, 'Неверный расчет lead_utm_term'
        assert transform_info[3]['lead_utm_source'] == 'google', 'Неверный расчет lead_utm_source'
        assert transform_info[3]['lead_utm_medium'] == 'context', 'Неверный расчет lead_utm_medium'
        assert transform_info[3]['lead_utm_campaign'] == 'blue', 'Неверный расчет lead_utm_campaign'
        assert transform_info[3]['lead_utm_content'] == 'cntx', 'Неверный расчет lead_utm_content'
        assert transform_info[3]['lead_utm_term'] == None, 'Неверный расчет lead_utm_term'


    def test_custom_fields(self):
        file_name = 'amo_json_2020_40.json'
        adv = Analysis(CONFIG)
        data = adv.extract(file_name)
        transform_info = adv.transform(data)
        assert transform_info[3]['amo_city'] == 'Москва', 'Неверный расчет amo_city'
        assert transform_info[3]['drupal_utm'] == 'source=google, medium=context, campaign=blue, keyword=1351759424-77001425413-378733477874--none--doneto.ru--, content=cntx', 'Неверный расчет drupal_utm'
        assert transform_info[3]['tilda_utm_source'] == None, 'Неверный расчет tilda_utm_source'
        assert transform_info[3]['tilda_utm_medium'] == None, 'Неверный расчет tilda_utm_medium'
        assert transform_info[3]['tilda_utm_campaign'] == None, 'Неверный расчет tilda_utm_campaign'
        assert transform_info[3]['tilda_utm_content'] == None, 'Неверный расчет tilda_utm_content'
        assert transform_info[3]['tilda_utm_term'] == None, 'Неверный расчет tilda_utm_term'
        assert transform_info[3]['ct_utm_source'] == None, 'Неверный расчет ct_utm_source'
        assert transform_info[3]['ct_utm_medium'] == None, 'Неверный расчет ct_utm_medium'
        assert transform_info[3]['ct_utm_campaign'] == None, 'Неверный расчет ct_utm_campaign'
        assert transform_info[3]['ct_utm_content'] == None, 'Неверный расчет ct_utm_content'
        assert transform_info[3]['ct_utm_term'] == None, 'Неверный расчет ct_utm_term'
        assert transform_info[3]['ct_type_communication'] == None, 'Неверный расчет ct_type_communication'
        assert transform_info[3]['ct_device'] == None, 'Неверный расчет ct_device'
        assert transform_info[3]['ct_os'] == None, 'Неверный расчет ct_os'
        assert transform_info[3]['ct_browser'] == None, 'Неверный расчет ct_browser'

        for i in data[3]['custom_fields_values']:
            for field in adv.CLASS_CONFIG:
                if i['field_id'] == adv.CLASS_CONFIG[field]:
                    assert transform_info[3][field.lower()[:(
                        len(field)-9)]] == i['values'][0]['value']
