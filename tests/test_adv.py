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

    source_row = [row for row in data if row['id'] == 26895186][0]

    result_row = analysis.transform_row(source_row)

    pprint(result_row)

    breakpoint()

    assert True
