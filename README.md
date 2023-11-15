# Analytics new building

Сервис для анализа новостроек на данные момент в базе 3 крупных застройщика ПИК, Самолет, А101  
Собираем данные по ЖК для дальнейшего иследования динамики цены, скидок.  
Для разворачивания инфраструктуры использовал Docker, docker-compose  

#### ETL 
- инструмент Airflow
- парсим данный с сайтов застройщика данные по квартирам и жк, объединяем все в одну плоскую таблицу и загружаем во временныю таблицу, которая служик как staging слой
- у застройщика ПИК порядка ~19000 кварртир, делаем код асинхронным для ускорения загрузки данных
- из слоя staging забираем данные и добаволем в слой core

#### БД
- В качестве БД использовал Postgresql, pgAdmin для работы с БД
- Core слой состоит из 4 нормализированных отношений

![dwh](https://github.com/xorxi12/Analytics-new-building/assets/147392409/696e1eda-230c-49f3-b503-cec4200bd855)

#### Визуализация 
 - Для визуализации данных поднял Superset
