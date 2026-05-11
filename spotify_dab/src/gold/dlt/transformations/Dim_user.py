import dlt

expectations = {
  "rule_1" : "user_id IS NOT NULL"
}

@dlt.table

def DimUserStg():
    df = spark.readStream.table("spotify_cata.silver.dim_user")
    return df


dlt.create_streaming_table(
  name = "dim_user",
  expect_all_or_drop= expectations
  )

dlt.create_auto_cdc_flow(
  target = "dim_user",
  source = "DimUserStg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)