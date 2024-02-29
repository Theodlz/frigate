import os

import pandas as pd

def validate_output_options(output_format, output_compression, output_compression_level, output_directory=None):
    if output_format not in ["parquet", "feather", "csv"]:
        raise ValueError(
            f"Invalid output format: {output_format}, must be one of ['parquet', 'feather', 'csv']"
        )

    if output_format == "parquet" and output_compression not in [
        None,
        "gzip",
        "snappy",
        "brotli",
    ]:
        raise ValueError(
            f"Invalid output compression with parquet: {output_compression}, must be one of [None, 'gzip', 'snappy', 'brotli']"
        )
    if output_format == "csv" and output_compression not in [
        None,
        "infer",
        "gzip",
        "bz2",
        "zip",
        "xz",
    ]:
        raise ValueError(
            f"Invalid output compression with csv: {output_compression}, must be one of [None, 'infer', 'gzip', 'bz2', 'zip', 'xz']"
        )
    if output_format == "feather" and output_compression not in [
        None,
        "lz4",
        "zstd",
        "uncompressed",
    ]:
        raise ValueError(
            f"Invalid output compression with feather: {output_compression}, must be one of [None, 'lz4', 'zstd', 'uncompressed']"
        )

    if output_compression_level is not None and output_format not in ["feather"]:
        print(f"Compression level is only supported with feather, not {output_format}. Argument will be ignored")

    if output_directory is not None and not os.path.exists(output_directory):
        try:
            os.makedirs(output_directory)
        except Exception as e:
            raise ValueError(f"Failed to create output directory: {e}")

def save_dataframe(df, filename, output_format, output_compression, output_compression_level, output_directory=None):
    # validate the output options
    validate_output_options(output_format, output_compression, output_compression_level, output_directory)

    # if the filename already have the extension, remove it
    if any(filename.endswith(ext) for ext in [".parquet", ".feather", ".csv"]):
        filename = filename.rsplit(".", 1)[0]

    # if the output directory is specified and its not already in the filename, add it
    if output_directory is not None and not filename.startswith(output_directory):
        filename = os.path.join(output_directory, filename)

    # save the dataframe
    if output_format == "parquet":
        filename = filename + ".parquet"
        df.to_parquet(filename, index=False, compression=output_compression)
    elif output_format == "feather":
        filename = filename + ".feather"
        df.to_feather(filename, compression=output_compression)
    elif output_format == "csv":
        filename = filename + ".csv"
        df.to_csv(filename, index=False, compression=output_compression)

    # we return the finale filename
    return filename
