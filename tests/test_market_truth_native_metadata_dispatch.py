from tank.analysis import truth


def test_native_metadata_boundaries_have_compiled_dispatch():
    assert {"_row_time", "_row_available", "_scalar_decimal", "_fixed_decimal_column"} <= set(
        truth.__pyx_capi__
    )
