from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True, retries=3)
def extract_from_gcs(year:int, month:int) -> Path:
    """Extract data from GCS and download."""
    if month < 10:
        file = f'fhv_tripdata_{year}-0' + str(month) + '.csv.gz'
    else:
        file = f'fhv_tripdata_{year}-' + str(month) + '.csv.gz'
    gcs_path = f"data/{file}"
    gcs_block = GcsBucket.load("week-3-zoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"download/")
    return Path(f"download/data/{file}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Transform data.
    
    Args:
        path (Path): path to file to transform
    """
    df = pd.read_csv(path, compression="gzip")
    
    df['pickup_datetime'] = pd.to_datetime(df["pickup_datetime"])
    df['dropOff_datetime'] = pd.to_datetime(df["dropOff_datetime"])

    print(f"rows: {len(df)}")
    
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    
    #df["passenger_count"].fillna(0, inplace=True)
    
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True, retries=3)
def write_to_bq(df: pd.DataFrame) -> None:
    """Write data to BigQuery.
    
    Args:
        df (pandas.DataFrame): dataframe to write
    """
    credentials = GcpCredentials.load("week-3-zoomcamp")
    
    df.to_gbq(
        destination_table="2019_data.trips",
        project_id="intense-jet-375817",
        if_exists="append",
        credentials=credentials.get_credentials_from_service_account(),
        chunksize=100000
    )


@flow(name='ETL GCS to BQ', log_prints=True)
def etl_gcs_to_bq( 
    month: int = 1,
    year: int = 2021,
):
    """ETL data from GCS to BigQuery."""
    path = extract_from_gcs(year, month)
    df = transform(path)
    write_to_bq(df)
    
@flow()
def etl_parent_flow(
    months: list[int] = [1,2],
    year: int = 2021,
):
    for month in months:
        print(f"Running ETL for month {month}...")
        etl_gcs_to_bq(month, year)


if __name__ == "__main__":
    year=2019
    months = list(range(1,13))
    etl_parent_flow(months, year)