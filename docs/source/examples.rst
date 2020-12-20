Examples
======================================================================================================================

JSON Files to Partitioned Hive Table
----------------------------------------------------------------------------------------------------------------------

Sample Input Data:
**********************************************************************************************************************

.. literalinclude:: data/schema_v1 sample_beautified.json
    :language: json

Sample Output Tables
**********************************************************************************************************************

.. table:: Table **"user"**
    :align: center
    :width: 100%
    :widths: auto
    :class: longtable

    +----+---------------+-------------+-------------+----------+-----------+------------+
    | id |  guid         |  forename   | surname     |  gender  | has_email | created_at |
    +====+===============+=============+=============+==========+===========+============+
    | 18 | "b12b59ba..." | "Jeannette" | "O"Loghlen" | "F"      | "1"       | 1547204429 |
    +----+---------------+-------------+-------------+----------+-----------+------------+
    | ...| ...           | ...         | ...         | ...      | ...       | ...        |
    +----+---------------+-------------+-------------+----------+-----------+------------+

.. table:: Table **"friends_mapping"**
    :align: center
    :width: 100%
    :widths: auto
    :class: longtable


    +-----+-------------+-----------+------------+
    | id  | guid        | friend_id | created_at |
    +=====+=============+===========+============+
    | 18  | b12b59ba... | 9952      | 1547204429 |
    +-----+-------------+-----------+------------+
    | 18  | b12b59ba... | 3391      | 1547204429 |
    +-----+-------------+-----------+------------+
    | 18  | b12b59ba... | 9637      | 1547204429 |
    +-----+-------------+-----------+------------+
    | 18  | b12b59ba... | 9939      | 1547204429 |
    +-----+-------------+-----------+------------+
    | 18  | b12b59ba... | 18994     | 1547204429 |
    +-----+-------------+-----------+------------+
    | ... | ...         | ...       | ...        |
    +-----+-------------+-----------+------------+


Application Code for Updating the Users Table
**********************************************************************************************************************

::

    from spooq2.pipeline import Pipeline
    import spooq2.extractor as E
    import spooq2.transformer as T
    import spooq2.loader as L

    users_mapping = [
        ("id",              "id",                     "IntegerType"),
        ("guid",            "guid",                   "StringType"),
        ("forename",        "attributes.first_name",  "StringType"),
        ("surename",        "attributes.last_name",   "StringType"),
        ("gender",          "attributes.gender",      "StringType"),
        ("has_email",       "attributes.email",       "StringBoolean"),
        ("created_at",      "meta.created_at_ms",     "timestamp_ms_to_s"),
    ]

    users_pipeline = Pipeline()

    users_pipeline.set_extractor(E.JSONExtractor(input_path="tests/data/schema_v1/sequenceFiles"))

    users_pipeline.add_transformers(
        [
            T.Mapper(mapping=users_mapping),
            T.ThresholdCleaner(
                range_definitions={"created_at": {"min": 0, "max": 1580737513, "default": None}}
            ),
            T.NewestByGroup(group_by="id", order_by="created_at"),
        ]
    )

    users_pipeline.set_loader(
        L.HiveLoader(
            db_name="users_and_friends",
            table_name="users",
            partition_definitions=[
                {"column_name": "dt", "column_type": "IntegerType", "default_value": 20200201}
            ],
            repartition_size=10,
        )
    )

    users_pipeline.execute()


Application Code for Updating the Friends_Mapping Table
**********************************************************************************************************************

::

    from spooq2.pipeline import Pipeline
    import spooq2.extractor as E
    import spooq2.transformer as T
    import spooq2.loader as L


    friends_mapping = [
        ("id",          "id",                  "IntegerType"),
        ("guid",        "guid",                "StringType"),
        ("friend_id",   "friend.id",           "IntegerType"),
        ("created_at",  "meta.created_at_ms",  "timestamp_ms_to_s"),
    ]

    friends_pipeline = Pipeline()

    friends_pipeline.set_extractor(E.JSONExtractor(input_path="tests/data/schema_v1/sequenceFiles"))

    friends_pipeline.add_transformers(
        [
            T.NewestByGroup(group_by="id", order_by="meta.created_at_ms"),
            T.Exploder(path_to_array="attributes.friends", exploded_elem_name="friend"),
            T.Mapper(mapping=friends_mapping),
            T.ThresholdCleaner(
                range_definitions={"created_at": {"min": 0, "max": 1580737513, "default": None}}
            ),
        ]
    )

    friends_pipeline.set_loader(
        L.HiveLoader(
            db_name="users_and_friends",
            table_name="friends_mapping",
            partition_definitions=[
                {"column_name": "dt", "column_type": "IntegerType", "default_value": 20200201}
            ],
            repartition_size=20,
        )
    )

    friends_pipeline.execute()


Application Code for Updating Both, the Users and Friends_Mapping Table, at once
**********************************************************************************************************************
This script extracts and transforms the common activities for both tables as they share the same input data set.
Caching the dataframe avoids redundant processes and reloading when an action is executed (the load step f.e.).
This could have been written with pipeline objects as well (by providing the Pipeline an ``input_df`` and/or ``output_df`` to bypass
extractors and loaders) but would have led to unnecessary verbosity. This example should also show the flexibility of
Spooq2 for activities and steps which are not directly supported.

::

    import spooq2.extractor as E
    import spooq2.transformer as T
    import spooq2.loader as L

    mapping = [
        ("id",              "id",                     "IntegerType"),
        ("guid",            "guid",                   "StringType"),
        ("forename",        "attributes.first_name",  "StringType"),
        ("surename",        "attributes.last_name",   "StringType"),
        ("gender",          "attributes.gender",      "StringType"),
        ("has_email",       "attributes.email",       "StringBoolean"),
        ("created_at",      "meta.created_at_ms",     "timestamp_ms_to_s"),
        ("friends",         "attributes.friends",     "as_is"),
    ]

    """Transformations used by both output tables"""
    common_df = E.JSONExtractor(input_path="tests/data/schema_v1/sequenceFiles").extract()
    common_df = T.Mapper(mapping=mapping).transform(common_df)
    common_df = T.ThresholdCleaner(
        range_definitions={"created_at": {"min": 0, "max": 1580737513, "default": None}}
    ).transform(common_df)
    common_df = T.NewestByGroup(group_by="id", order_by="created_at").transform(common_df)
    common_df.cache()

    """Transformations for users_and_friends table"""
    L.HiveLoader(
        db_name="users_and_friends",
        table_name="users",
        partition_definitions=[
            {"column_name": "dt", "column_type": "IntegerType", "default_value": 20200201}
        ],
        repartition_size=10,
    ).load(common_df.drop("friends"))

    """Transformations for friends_mapping table"""
    friends_df = T.Exploder(path_to_array="friends", exploded_elem_name="friend").transform(
        common_df
    )
    friends_df = T.Mapper(
        mapping=[
            ("id",          "id",          "IntegerType"),
            ("guid",        "guid",        "StringType"),
            ("friend_id",   "friend.id",   "IntegerType"),
            ("created_at",  "created_at",  "IntegerType"),
        ]
    ).transform(friends_df)
    L.HiveLoader(
        db_name="users_and_friends",
        table_name="friends_mapping",
        partition_definitions=[
            {"column_name": "dt", "column_type": "IntegerType", "default_value": 20200201}
        ],
        repartition_size=20,
    ).load(friends_df)
