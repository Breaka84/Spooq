Get Started
======================================================================================================================

This section will guide you through a simple ETL pipeline built with Spooq to showcase how to use this library.

Sample Input Data:
**********************************************************************************************************************

.. literalinclude:: data/schema_v1 sample_beautified.json
    :language: json


Application Code for Creating a User Table
**********************************************************************************************************************

.. code-block:: python

    from pyspark.sql import functions as F, types as T
    from spooq.extractor import JSONExtractor
    from spooq.transformer import Mapper, ThresholdCleaner, NewestByGroup
    from spooq.loader import HiveLoader
    from spooq.transformer import mapper_transformations as spq

    users_mapping = [
        ("id",              "id",                     spq.to_num),
        ("guid",            "guid",                   "string"),
        ("forename",        "attributes.first_name",  "string"),
        ("surename",        "attributes.last_name",   "string"),
        ("gender",          "attributes.gender",      spq.apply(func=F.lower)),
        ("has_email",       "attributes.email",       spq.has_value),
        ("has_university",  "attributes.university",  spq.has_value),
        ("created_at",      "meta.created_at_ms",     spq.to_timestamp),
    ]

    # Extract
    source_df = JSONExtractor(input_path="tests/data/schema_v1/sequenceFiles").extract()

    # Transform
    mapped_df = Mapper(users_mapping).transform(source_df)
    cleaned_df = ThresholdCleaner(
        thresholds={"created_at": {"min": "2019-01-01", "max": F.current_date(), "default": None}},
        column_to_log_cleansed_values="cleansed_values",
        store_as_map=True,
    ).transform(mapped_df)
    deduplicated_df = NewestByGroup(group_by="id", order_by="created_at").transform(cleaned_df)

    # Load
    HiveLoader(
        db_name="users_and_friends",
        table_name="users",
        partition_definitions=[
            {"column_name": "dt", "column_type": "IntegerType", "default_value": 20200201}
        ],
        repartition_size=10,
    )


.. table:: Table **"user"**
    :align: center
    :width: 100%
    :widths: auto
    :class: longtable

    +----+--------------------------------------+----------+----------+--------+-----------+----------------+------------+-------------------------------------+
    | id | guid                                 | forename | surename | gender | has_email | has_university | created_at | cleansed_values                     |
    +====+======================================+==========+==========+========+===========+================+============+=====================================+
    | 1  | 799eb359-2e98-4526-a2ec-455b03e57b5c | Orran    | Haug     | m      | false     | false          | null       | {created_at -> 2018-11-30 11:19:43} |
    +----+--------------------------------------+----------+----------+--------+-----------+----------------+------------+-------------------------------------+
    | N  | ...                                  | ...      | ...      | ...    | ...       | ...            | ...        | ...                                 |
    +----+--------------------------------------+----------+----------+--------+-----------+----------------+------------+-------------------------------------+


Application Code for Creating a Friends_Mapping Table
**********************************************************************************************************************

.. code-block:: python

    from pyspark.sql import functions as F, types as T
    from spooq.extractor import JSONExtractor
    from spooq.transformer import Mapper, ThresholdCleaner, NewestByGroup, Exploder
    from spooq.loader import HiveLoader
    from spooq.transformer import mapper_transformations as spq

    friends_mapping = [
        ("id",          "id",                  spq.to_num),
        ("guid",        "guid",                "string"),
        ("friend_id",   "friend.id",           spq.to_num),
        ("created_at",  "meta.created_at_ms",  spq.to_timestamp),
    ]

    # Extract
    source_df = JSONExtractor(input_path="tests/data/schema_v1/sequenceFiles").extract()

    # Transform
    deduplicated_friends_df = NewestByGroup(group_by="id", order_by="meta.created_at_ms").transform(source_df)
    exploded_friends_df = Exploder(path_to_array="attributes.friends", exploded_elem_name="friend").transform(deduplicated_friends_df)
    mapped_friends_df = Mapper(mapping=friends_mapping).transform(exploded_friends_df)
    cleaned_friends_df = ThresholdCleaner(
        thresholds={"created_at": {"min": "2019-01-01", "max": F.current_date(), "default": None}},
        column_to_log_cleansed_values="cleansed_values",
        store_as_map=True,
    ).transform(mapped_friends_df)

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


.. table:: Table **"friends_mapping"**
    :align: center
    :width: 100%
    :widths: auto
    :class: longtable

    +----+--------------------------------------+-----------+---------------------+
    | id | guid                                 | friend_id |created_at           |
    +====+======================================+===========+=====================+
    | 26 | bd666d9d-9bb2-494e-8dc3-ab1c29a67ab2 | 8001      | 2019-05-29 23:25:27 |
    +----+--------------------------------------+-----------+---------------------+
    | 26 | bd666d9d-9bb2-494e-8dc3-ab1c29a67ab2 | 5623      | 2019-05-29 23:25:27 |
    +----+--------------------------------------+-----------+---------------------+
    | 26 | bd666d9d-9bb2-494e-8dc3-ab1c29a67ab2 | 17713     | 2019-05-29 23:25:27 |
    +----+--------------------------------------+-----------+---------------------+
    | 65 | 2a5fd4e4-2cfa-41c6-9771-46c666e7c2eb | 4428      | null                |
    +----+--------------------------------------+-----------+---------------------+
    | 65 | 2a5fd4e4-2cfa-41c6-9771-46c666e7c2eb | 13011     | null                |
    +----+--------------------------------------+-----------+---------------------+
    | N  | ...                                  | ...       | ...                 |
    +----+--------------------------------------+-----------+---------------------+


Application Code for Updating Both, the Users and Friends_Mapping Table, at once
**********************************************************************************************************************
This script extracts and transforms the common activities for both tables as they share the same input data set.
Caching the dataframe avoids redundant processes and reloading when an action is executed (the load step f.e.).
This could have been written with pipeline objects as well (by providing the Pipeline an ``input_df`` and/or ``output_df`` to bypass
extractors and loaders) but would have led to unnecessary verbosity. This example should also show the flexibility of
Spooq for activities and steps which are not directly supported.

.. code-block:: python

    from pyspark.sql import functions as F, types as T
    from spooq.extractor import JSONExtractor
    from spooq.transformer import Mapper, ThresholdCleaner, NewestByGroup, Exploder
    from spooq.loader import HiveLoader
    from spooq.transformer import mapper_transformations as spq

    mapping = [
        ("id",              "id",                     spq.to_num),
        ("guid",            "guid",                   "string"),
        ("forename",        "attributes.first_name",  "string"),
        ("surename",        "attributes.last_name",   "string"),
        ("gender",          "attributes.gender",      spq.apply(func=F.lower)),
        ("has_email",       "attributes.email",       spq.has_value),
        ("has_university",  "attributes.university",  spq.has_value),
        ("created_at",      "meta.created_at_ms",     spq.to_timestamp),
        ("friends",         "attributes.friends",     "as_is"),
    ]

    # Transformations used by both output tables
    common_df = JSONExtractor(input_path="tests/data/schema_v1/sequenceFiles").extract()
    common_df = Mapper(mapping=mapping).transform(common_df)
    common_df = ThresholdCleaner(
        thresholds={"created_at": {"min": "2019-01-01", "max": F.current_date(), "default": None}},
        column_to_log_cleansed_values="cleansed_values",
        store_as_map=True,
    ).transform(common_df)
    common_df = NewestByGroup(group_by="id", order_by="created_at").transform(common_df)
    common_df.cache()

    # Loading of users table
    HiveLoader(
        db_name="users_and_friends",
        table_name="users",
        partition_definitions=[
            {"column_name": "dt", "column_type": "IntegerType", "default_value": 20200201}
        ],
        repartition_size=10,
    ).load(common_df.drop("friends"))

    # Transformations for friends_mapping table
    friends_df = Exploder(path_to_array="friends", exploded_elem_name="friend").transform(
        common_df
    )
    friends_df = Mapper(
        mapping=[
            ("id",          "id",          "string"),
            ("guid",        "guid",        "string"),
            ("friend_id",   "friend.id",   spq.to_num),
            ("created_at",  "created_at",  "TimestampType"),
        ]
    ).transform(friends_df)

    # Loading of friends_mapping table
    HiveLoader(
        db_name="users_and_friends",
        table_name="friends_mapping",
        partition_definitions=[
            {"column_name": "dt", "column_type": "IntegerType", "default_value": 20200201}
        ],
        repartition_size=20,
    ).load(friends_df)

Dataflow Chart
--------------

.. uml:: ./diagrams/from_thesis/data_flow.puml
    :caption: Typical Data Flow of a Spooq Data Pipeline
