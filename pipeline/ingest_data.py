import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


# dataset URL
PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"


# column types
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


@click.command()
@click.option("--pg-user", default="root", help="Postgres user")
@click.option("--pg-pass", default="root", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5433, type=int, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", help="Postgres database name")
@click.option("--target-table", default="yellow_taxi_data", help="Target table")
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):

    print("Starting ingestion pipeline...")

    url = PREFIX + "yellow_tripdata_2021-01.csv.gz"

    # connect to Postgres
    # engine = create_engine(
    #     f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    # )
    engine = create_engine(
    f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
)


    print("Connected to Postgres")

    # read CSV in chunks
    df_iter = pd.read_csv(
        url,
        compression="gzip",
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000,
    )

    # first chunk: create schema
    df = next(df_iter)

    print("Creating table schema...")

    df.head(n=0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
    )

    print("Inserting first chunk...")

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists="append",
    )

    # loop through remaining chunks
    print("Loading remaining chunks...")

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
        )

    print("Ingestion completed successfully!")


if __name__ == "__main__":
    run()
