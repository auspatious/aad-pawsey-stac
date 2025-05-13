import os
from concurrent.futures import ThreadPoolExecutor
from json import dumps
import logging

import pandas as pd
import typer
from obstore.fsspec import FsspecStore
from obstore.store import S3Store
from rio_stac import create_stac_item

PARQUET_PATH = os.getenv(
    "PARQUET",
    "https://projects.pawsey.org.au/idea-objects/idea-curated-objects.parquet",
)
BUCKET = os.getenv("BUCKET", "ideatest")
N_THREADS = int(os.getenv("SLURM_JOB_CPUS_PER_NODE", "16"))

# Typer app
app = typer.Typer()

# Get a logger
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
ch.setFormatter(formatter)
log.addHandler(ch)


@app.command()
def stac_create_cli(
    dataset: str = typer.Argument(
        ..., help="The name of the dataset to create STAC items for"
    ),
    limit: int | None = typer.Option(
        default=None, help="The number of items to stop after"
    ),
    destination: str = typer.Option(
        default="file",
        help="The path to write the STAC items to. Should be either file or pawsey.",
    ),
    destination_prefix: str | None = typer.Option(
        default=None,
        help="The prefix to use for the destination path",
    ),
    parquet_path: str = typer.Option(
        PARQUET_PATH,
        help="The path to the parquet file containing the dataset metadata",
    ),
    n_threads: int = typer.Option(
        N_THREADS,
        help="The number of threads to use for processing",
    )
):
    """
    Create STAC items for a given dataset.
    """
    # Configure out destination
    log.info(f"Using destination: {destination}")
    if destination == "file":
        store = FsspecStore(destination, mkdir=True)
    elif destination == "pawsey":
        log.info(f"Using bucket: {BUCKET}")
        store = S3Store(BUCKET)
    else:
        raise ValueError("Destination must be either 'file' or 'pawsey'.")

    # Read the parquet file
    fs = FsspecStore("https")
    with fs.open(parquet_path) as f:
        df = pd.read_parquet(f)

    # Filter the dataframe for the given dataset
    df = df[df.Dataset == dataset]

    # Limit the number of items
    if limit is not None:
        log.info(f"Limiting to {limit} items.")
        df = df.iloc[:limit]

    def process_row(row):
        item = create_stac_item(
            f"{row.Host}/{row.Bucket}/{row.Key}",
            row.date,
            collection=dataset,
            asset_name="data",
            with_eo=True,
            with_proj=True,
            with_raster=True,
        )

        item_dict = item.to_dict()
        out_path = row.Key.replace(".tif", ".json")

        if destination == "file":
            # Local file system for testing
            out_path = os.path.join(os.getcwd(), "stac", out_path)
            store.write_text(out_path, dumps(item_dict, indent=2))
        else:
            # Write to an object store
            if destination_prefix is not None:
                out_path = destination_prefix + "/" + out_path
            else:
                out_path = os.path.join("stac", out_path)
            store.put(out_path, dumps(item_dict, indent=2).encode("utf-8"))

    # Use ThreadPoolExecutor to parallelize the processing of rows
    log.info(f"Processing {len(df)} tifs in parallel with {n_threads} threads.")
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        executor.map(process_row, [row for _, row in df.iterrows()])
    log.info("Finished processing tifs.")


if __name__ == "__main__":
    app()
