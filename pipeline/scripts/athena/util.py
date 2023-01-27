def expect_empty_result(cursor):
    desc = cursor.description
    if len(desc) != 0:
        raise ValueError(f"Expected `cursor.description` to be empty but contained a value: {desc}")
    all = cursor.fetchall()
    if len(all) != 0:
        raise ValueError(f"Expected `cursor.fetchall()` to be empty but contained a value: {desc}")
