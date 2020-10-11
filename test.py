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
end_date = datetime.strptime('20201003', '%Y%m%d').date()
day_count = (end_date - start_date).days + 1

for checkpointDate in (start_date + timedelta(n) for n in range(day_count)):
    try:
        # checkpointDate = datetime.strptime(single_date, '%Y%m%d').date()
        checkpointDateWithoutDash = checkpointDate.strftime("%Y%m%d")
        checkpointDateWithDash = checkpointDate.strftime("%Y-%m-%d")
        query = f"""SELECT t.* from `project-5400504384186300846.UMARKETADM.AGENT_REF_{checkpointDateWithoutDash}` t
        FULL JOIN (select d.* from UNNEST(`momovn-scd-prod.UMARKETADM.AGENT_REF_CHECK`("{checkpointDateWithDash}")) d) scd
        on t.ID = scd.id
        WHERE scd.id is null or t.id is null"""
        print(query)
        # client = bigquery.Client(project=billing_project)
        # result : RowIterator = client.query(query, conf).result()
        # if result.total_rows > 0:
            # print(f"""{checkpointDate}\n""")
    except Exception as ex:
        print(f"""error date: {checkpointDate}: {str(ex)[0:1000]}\n""")