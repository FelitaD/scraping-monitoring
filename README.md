# Scraping Monitoring

Scraping of administrative documents. THe logs are analysed to monitor any incongurencies.

## What the tool does

### Input
`extractor.py` `pensieve_interface.py`
Extracts collection data from Pensieve database via the endpoint 
GET/by_collect_date.
It is here that we specify for which dates we want to collect data.

### Transformations
`exhaustive_transformer.py`
This transformation was used for the tests. It basically reproduces Pensieve's database by normalizing the infos field. It also calculates rates for errors.

`first_indicator_transformer.py`
For each territory, detects collects with issues bases on these criterias :
* `updated_at` interval is higher than 12 days
* `item_scraped_count` changes from a count higher than 10 to nothing 

### Output
[Google Sheet Output Test](https://docs.google.com/spreadsheets/d/1KKbu4FAAwwm0aECxCBjqaur6JA7zDD-mJBisTf_bF9k/edit#gid=0) <br>
[Google Sheet Output](https://docs.google.com/spreadsheets/d/1mEEhldYWgLryzYl1KMiNhblOCepctQuWieNnC8BgNkM/edit#gid=0) <br>

|Date de l'alerte|  Id de l'alerte| Nom département |      Code      | Nom             |Url dans Pensieve|Collect status     |
|---------------:|:---------------|:----------------|:---------------|-----------------|-----------------|-------------------|
|     18-03-2021 |        S1-0001 |     Normandie   |   FRCOMM76600  |  Le Havre       |    http://      |   finished        |
|     18-03-2021 |        S1-0001 |     Normandie   |   FRCOMM76290  |   Montivilliers |    http://      |closespider_timeout|
|     18-03-2021 |        S1-0001 |     Normandie   |   FRCOMM76000  |    Rouen        |    http://      |   finished        |

## Test the tool
In ws-pensieve (données test en prod)
`export DATABASE_HOST=rdb.pensieve.explain.fr`
In monitoring_collect_docs_admins (local)
`unset PENSIEVE_URL`













