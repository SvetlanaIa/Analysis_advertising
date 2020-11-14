import datetime as dt

CONFIG = {
    'TIME_FORMAT': '%Y-%m-%d %H:%M:%S',
    'WEEK_OFFSET': dt.timedelta(hours=24 + 24 + 6),
    'CUSTOM_FIELDS': [
        'amo_city',
        'drupal_utm',
        'tilda_utm_source',
        'tilda_utm_medium',
        'tilda_utm_campaign',
        'tilda_utm_content',
        'tilda_utm_term',
        'ct_utm_source',
        'ct_utm_medium',
        'ct_utm_campaign',
        'ct_utm_content',
        'ct_utm_term',
        'ct_type_communication',
        'ct_device',
        'ct_os',
        'ct_browser'
    ]

}
