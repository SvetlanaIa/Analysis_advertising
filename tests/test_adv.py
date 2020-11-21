import json
import os
from pprint import pprint

import pytest


@pytest.fixture
def analysis():
    from adv import Analysis
    return Analysis()


def test_transform_row(analysis):
    with open(os.path.join(os.path.dirname(__file__), 'amo_json_2020_40.json'),
              encoding='utf-8') as test_file:
        data = json.loads(test_file.read())

    source_row = [row for row in data if row['id'] == 26887728][0]

    result_row = analysis.transform_row(source_row)

    pprint(result_row)

    breakpoint()

    assert result_row['amo_city'] == 'Красноярск', 'Неверный расчет amo_city'
    assert result_row['drupal_utm'] is None, 'Неверный расчет drupal_utm'
    assert result_row['tilda_utm_source'] == 'google', 'Неверный расчет tilda_utm_source'
    assert result_row['tilda_utm_medium'] == 'search', 'Неверный расчет tilda_utm_medium'
    assert result_row['tilda_utm_campaign'] == 'anka', 'Неверный расчет tilda_utm_campaign'
    assert result_row['tilda_utm_content'] == 'cntx', 'Неверный расчет tilda_utm_content'
    assert result_row['tilda_utm_term'] == '773989869-39333114463-254200627636------+реклама +в +интернете', 'Неверный расчет tilda_utm_term'
    assert result_row['ct_utm_source'] is None, 'Неверный расчет ct_utm_source'
    assert result_row['ct_utm_medium'] is None, 'Неверный расчет ct_utm_medium'
    assert result_row['ct_utm_campaign'] is None, 'Неверный расчет ct_utm_campaign'
    assert result_row['ct_utm_content'] is None, 'Неверный расчет ct_utm_content'
    assert result_row['ct_utm_term'] is None, 'Неверный расчет ct_utm_term'
    assert result_row['ct_type_communication'] is None, 'Неверный расчет ct_type_communication'
    assert result_row['ct_device'] is None, 'Неверный расчет ct_device'
    assert result_row['ct_os'] is None, 'Неверный расчет ct_os'
    assert result_row['ct_browser'] is None, 'Неверный расчет ct_browser'
    assert result_row['amo_items_2020'] == 'Яндекс,Google', 'Неверный расчет amo_items_2020'

    for i in source_row['custom_fields_values']:
        for field in analysis.CLASS_CONFIG:
            if (field != 'AMO_ITEMS_2019_FIELD_ID' and
                    field != 'AMO_ITEMS_2020_FIELD_ID' and
                    i['field_id'] == analysis.CLASS_CONFIG[field]):
                assert result_row[field.lower()[:(
                    len(field)-9)]] == i['values'][0]['value']
