from pyspark.sql.functions import col

def get_valid_records(df):

    return (
        df.filter(col("event_id").isNotNull())
          .filter(col("user_id").isNotNull())
          .filter(col("amount") >= 0)
          .filter(
              col("event_type").isin(
                  "view",
                  "add_to_cart",
                  "purchase"
              )
          )
    )


def get_invalid_records(df):

    return df.filter(
        (col("event_id").isNull()) |
        (col("user_id").isNull()) |
        (col("amount") < 0) |
        (
            ~col("event_type").isin(
                "view",
                "add_to_cart",
                "purchase"
            )
        )
    )