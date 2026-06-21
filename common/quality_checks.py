from pyspark.sql.functions import col

def validate_records(df):

    valid_df = df.filter(
        col("event_id").isNotNull()
    )

    valid_df = valid_df.filter(
        col("user_id").isNotNull()
    )

    valid_df = valid_df.filter(
        col("amount") >= 0
    )

    # valid_df = valid_df.filter(
    #     col("event_type").isin(
    #         "view",
    #         "add_to_cart",
    #         "purchase"
    #     )
    # )

    return valid_df


def quarantine_records(df):

    invalid_df = df.filter(
        (col("event_id").isNull()) |
        (col("user_id").isNull()) |
        (col("amount") < 0)
    )

    return invalid_df