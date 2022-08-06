#!/usr/bin/env python
# coding: utf-8

import batch


def write_test_dataset_tos3(df, output_file):

    options = batch.localstack_check()

    df.to_parquet(
        output_file,
        engine="pyarrow",
        compression=None,
        index=False,
        storage_options=options,
    )


year = 2021
month = 1

# Write test data
output_file = batch.get_output_path(year, month)
test_df = batch.test_dataset()
categorical = ["PUlocationID", "DOlocationID"]
test_df_transformed = batch.prepare_data(test_df, categorical)
write_test_dataset_tos3(test_df_transformed, output_file)
#


input_file = batch.get_input_path(year, month)
options = batch.localstack_check()
df_input = batch.read_data(input_file, options)


print("test_DF", test_df_transformed.info())
print("df_input", df_input.info())

print("test_DF", test_df_transformed)
print("df_input", df_input)


def test_integration():
    assert test_df_transformed.equals(df_input)
