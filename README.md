# Analysis_advertising
Analysis_advertising

Есть JSON - выгрузка из CRM
Нужно создать на его основе датафрейм c колонками:
id, created_at, updated_at, trashed_at, amo_city, amo_pipeline_id, amo_status_id, amo_items_2019, amo_items_2020, ct_browser, ct_os, ct_device
'id' - id из JSON
'created_at' - created_at из JSON
'amo_pipeline_id' - pipeline_id из JSON
'amo_status_id' - status_id из JSON
'amo_updated_at' - updated_at из JSON
'amo_trashed_at' - trashed_at из JSON
'amo_closed_at' - closed_at из JSON
Колонки, значения которых берём из раздела 'custom_fields_values' JSON-файла:
'amo_city' - первое значение параметра Values поля field_id = 512318
'drupal_utm' - первое значение параметра Values поля field_id = 632884
'tilda_utm_source': 648158 'tilda_utm_medium': 648160 'tilda_utm_campaign': 648310 'tilda_utm_content': 648312 'tilda_utm_term': 648314 'ct_utm_source': 648256 'ct_utm_medium': 648258 'ct_utm_campaign': 648260 'ct_utm_content': 648262 'ct_utm_term': 648264
'ct_type_communication': 648220
'ct_device': 648276 'ct_os': 648278 'ct_browser': 648280
Добавить колонки, вычисляемые из даты (формат UNIX timestamp) поля created_at:
'created_at_bq_timestamp' - дата и время, приведённые к формату DATETIME Google BigQuery
created_at_year - год в формате YYYY, к которому относится дата в created_at
created_a_month - месяц в формате MM
created_at_week - номер недели в году. Неделя начинается в пятницу в 18:00 по московскому времени. (Хорошо бы вынести в конфиг время разделения недели, чтобы можно было это быстро менять). Первая неделя года - та, к которой относится первый четверг года.
Добавить колонки, собираемые из других полей (парсим utm_метки):
lead_utm_source
Если поле drupal_utm (field_id = 632884) содержит ‘utm_source=’
Если в поле drupal_utm есть фрагмент 'utm_source=yandex' или 'utm_medium=yandex', то lead_utm_source = 'yandex'
Иначе lead_utm_source = значение фрагмента между 'utm_source=' и ближайшим символом '& или концом строки'
Eсли в поле drupal_utm нет фрагмента 'utm_source=', и поле 'ct_utm_source' не пустое lead_utm_source = ct_utm_source
Иначе
lead_utm_source = tilda_utm_source
Дополнительно сделать проверку: Если поля ct_utm_source или tilda_utm_source не пустые и по итогу обработки значения в них отличаются от значения lead_utm_source, добавить в лог-файл info.log запись "Конфликт utm_source в сделке {id}", где id - номер сделки - поле 'id' соответствующей записи

lead_utm_medium
Если поле drupal_utm содержит ‘utm_medium=’
Если в поле drupal_utm есть фрагмент 'utm_source=context' или 'utm_medium=context', то lead_utm_medium = 'context'
Иначе lead_utm_medium = значение фрагмента между 'utm_medium=' и ближайшим символом '& или концом строки'
Если в поле drupal_utm нет фрагмента 'utm_medium=', и поле 'ct_utm_medium' не пустое
lead_utm_medium = ct_utm_medium
Иначе
lead_utm_medium = tilda_utm_medium
Дополнительно сделать проверку: Если поля ct_utm_medium или tilda_utm_medium не пустые и по итогу обработки значения в них отличаются от значения lead_utm_medium, добавить в лог-файл info.log запись "Конфликт utm_medium в сделке {id}", где id - номер сделки - поле 'id' соответствующей записи.

lead_utm_campaign
Если поле drupal_utm содержит ‘utm_campaign=’
lead_utm_campaign = значение фрагмента между 'utm_campaign=' и ближайшим символом '& или концом строки'
Если в поле drupal_utm нет фрагмента 'utm_campaign=', и поле 'ct_utm_campaign' не пустое
lead_utm_campaign = ct_utm_campaign
Иначе
lead_utm_campaign = tilda_utm_campaign
Дополнительно сделать проверку: Если поля ct_utm_campaign или tilda_utm_campaign не пустые и по итогу обработки значения в них отличаются от значения lead_utm_campaign, добавить в лог-файл info.log запись "Конфликт utm_campaign в сделке {id}", где id - номер сделки - поле 'id' соответствующей записи.

Поля lead_utm_content и lead_utm_term создать по аналогии с lead_utm_campaign

Итоговый датафрейм хорошо бы загрузить в таблицу Google BigQuery, но можно пока и просто сохранить в tsv-файл на жесткий диск.
