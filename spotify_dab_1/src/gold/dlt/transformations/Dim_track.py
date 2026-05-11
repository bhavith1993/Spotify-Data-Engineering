import dlt

@dlt.table

def DimTrackStg():
    df = spark.readStream.table("spotify_cata.silver.dim_track")
    return df


dlt.create_streaming_table("dim_track")

dlt.create_auto_cdc_flow(
  target = "dim_track",
  source = "DimTrackStg",
  keys = ["track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)