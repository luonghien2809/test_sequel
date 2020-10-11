from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator
from google.cloud.bigquery import QueryJobConfig

billing_project = "momovn-dev"
conf = QueryJobConfig()
conf.use_query_cache = True
conf.use_legacy_sql = False
checkpointDate = None
start_date = datetime.strptime('20201002', '%Y%m%d').date()
end_date = datetime.strptime('20201002', '%Y%m%d').date()
day_count = (end_date - start_date).days + 1

for checkpointDate in (start_date + timedelta(n) for n in range(day_count)):
    try:
        # checkpointDate = datetime.strptime(single_date, '%Y%m%d').date()
        checkpointDateWithoutDash = checkpointDate.strftime("%Y%m%d")
        checkpointDateWithDash = checkpointDate.strftime("%Y-%m-%d")

        query = f"""WITH A AS( SELECT GPS.reference PHONE FROM `momovn-prod.HERMES.HERMES_LOCATIONS` GPS WHERE DATE(GPS.event_timestamp,'Asia/Bangkok') = {checkpointDateWithDash})
        SELECT COUNT(DISTINCT T1.USER_ID), 'HERMES LOCATION'
        FROM `momovn-prod.BITEAM_INTERN.{checkpointDateWithoutDash}_CHECK_LOCATION` T1 
        LEFT JOIN A T2 
        ON T1.USER_ID = T2.PHONE 
        WHERE T2.PHONE IS NULL
        UNION ALL
        SELECT COUNT(DISTINCT T1.USER_ID), 'USER_LOCATION'
        FROM `momovn-prod.BITEAM_INTERN.{checkpointDateWithoutDash}_CHECK_LOCATION` T1 
        LEFT JOIN `momovn-prod.HERMES.USER_LOCATIONS_{checkpointDateWithoutDash}` T2 
        ON T1.USER_ID = T2.USER_ID 
        WHERE T2.USER_ID IS NULL"""

        print(query)
        client = bigquery.Client(project=billing_project)
        result : RowIterator = client.query(query, conf).result()
        if result.max_results > 0:
            print(f"""{checkpointDate}\n""")
    except Exception as ex:
        print(f"""error date: {checkpointDate}: {str(ex)[0:1000]}\n""")