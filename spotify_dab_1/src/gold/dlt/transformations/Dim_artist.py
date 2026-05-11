import dlt

@dlt.table

def DimartistStg():
    df = spark.readStream.table("spotify_cata.silver.dim_artist")
    return df


dlt.create_streaming_table("dim_artist")

dlt.create_auto_cdc_flow(
  target = "dim_artist",
  source = "DimartistStg",
  keys = ["artist_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)