from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator
from google.cloud.bigquery import QueryJobConfig

billing_project = "momovn-dev"
conf = QueryJobConfig()
conf.use_query_cache = True
conf.use_legacy_sql = False
checkpointDate = None
start_date = datetime.strptime('20201001', '%Y%m%d').date()
end_date = datetime.strptime('20201009', '%Y%m%d').date()
day_count = (end_date - start_date).days + 1

for checkpointDate in (start_date + timedelta(n) for n in range(day_count)):
    try:
        # checkpointDate = datetime.strptime(single_date, '%Y%m%d').date()
        checkpointDateWithoutDash = checkpointDate.strftime("%Y%m%d")
        checkpointDateWithDash = checkpointDate.strftime("%Y-%m-%d")

        query = f"""
        
        MERGE INTO
        `project-5400504384186300846.TMP.HERMES_LOCATIONS` locations
        USING
        (
        WITH
        today_data AS (
        SELECT
            user_id,
            lat,
            lon,
            TIMESTAMP(event_timestamp) event_timestamp
        FROM
            `project-5400504384186300846.HERMES.USER_LOCATIONS_{checkpointDateWithoutDash}`
        where lat >= -90 and lat <= 90 and lon >= -180 and lon <= 180
        ),
        grid AS (
            select ID, POLYGON
            from `project-5400504384186300846.HERMES.LOCATION_GRID_POLYGON`
        )
        SELECT
            user_id,
            event_timestamp,
            id loc_id,
            lat,
            lon
        FROM
            today_data
        INNER JOIN GRID ON ST_WITHIN(ST_GEOGPOINT(lon, lat), GRID.POLYGON)
        ) new_loc
        ON
        locations.reference = new_loc.user_id
        AND locations.event_timestamp = TIMESTAMP(PARSE_DATE("%Y%m%d", "{checkpointDateWithoutDash}"))
        WHEN NOT MATCHED
        THEN INSERT
        ( event_timestamp,
            reference,
            loc_id,
            lat,
            lon,
            point
        )
        VALUES
        ( event_timestamp, user_id, loc_id, lat, lon, ST_GEOGPOINT(lon, lat) )
        """
        print(query)
        client = bigquery.Client(project=billing_project)
        result = client.query(query, conf).result()
    except Exception as ex:
        print(f"""error date: {checkpointDate}: {str(ex)[0:1000]}\n""")