def get_context_vars():
    return {"entity_type": "business", "date": "2018-10-20", "time_range": "last_week"}


def get_metadata():
    return {
        "extractor": {
            "params": {
                "input_path": "business/p_year=2018/p_month=10/p_day=20,business/p_year=2018/p_month=10/p_day=19,business/p_year=2018/p_month=10/p_day=18,business/p_year=2018/p_month=10/p_day=17,business/p_year=2018/p_month=10/p_day=16,business/p_year=2018/p_month=10/p_day=15,business/p_year=2018/p_month=10/p_day=14"
            },
            "name": "JSONExtractor",
        },
        "transformers": [
            {
                "params": {
                    "mapping": [
                        ["business_id", "business_id", "StringType"],
                        ["name", "name", "StringType"],
                        ["address", "address", "StringType"],
                        ["city", "city", "StringType"],
                        ["state", "state", "StringType"],
                        ["postal_code", "postal_code", "StringType"],
                        ["latitude", "latitude", "DoubleType"],
                        ["longitude", "longitude", "DoubleType"],
                        ["stars", "stars", "LongType"],
                        ["review_count", "review_count", "LongType"],
                        ["categories", "categories", "json_string"],
                        ["open_on_monday", "hours.Monday", "StringType"],
                        ["open_on_tuesday", "hours.Tuesday", "StringType"],
                        ["open_on_wednesday", "hours.Wednesday", "StringType"],
                        ["open_on_thursday", "hours.Thursday", "StringType"],
                        ["open_on_friday", "hours.Friday", "StringType"],
                        ["open_on_saturday", "hours.Saturday", "StringType"],
                        ["attributes", "attributes", "json_string"],
                    ]
                },
                "name": "Mapper",
            },
            {
                "params": {
                    "thresholds": {
                        "latitude": {"max": 90.0, "min": -90.0},
                        "stars": {"max": 5, "min": 1},
                        "longitude": {"max": 180.0, "min": -180.0},
                    }
                },
                "name": "ThresholdCleaner",
            },
        ],
        "context_variables": {
            "pipeline_type": "ad_hoc",
            "entity_type": "business",
            "value_ranges": {
                "latitude": {"max": 90.0, "min": -90.0},
                "stars": {"max": 5, "min": 1},
                "longitude": {"max": 180.0, "min": -180.0},
            },
            "level_of_detail": "all",
            "level_of_detail_int": 10,
            "mapping": [
                {"triviality": 1, "name": "business_id", "desc": "22 character unique string"},
                {"name": "name", "triviality": 1},
                {"name": "address", "triviality": 10},
                {"name": "city"},
                {"name": "state"},
                {"name": "postal_code"},
                {"name": "latitude", "target_type": "DoubleType"},
                {"name": "longitude", "target_type": "DoubleType"},
                {
                    "target_type": "LongType",
                    "triviality": 1,
                    "name": "stars",
                    "desc": "star rating, rounded to half-stars",
                },
                {"triviality": 1, "name": "review_count", "target_type": "LongType"},
                {"triviality": 1, "name": "categories", "target_type": "json_string"},
                {"path": "hours.Monday", "name": "open_on_monday", "triviality": 10},
                {"path": "hours.Tuesday", "name": "open_on_tuesday", "triviality": 10},
                {"path": "hours.Wednesday", "name": "open_on_wednesday", "triviality": 10},
                {"path": "hours.Thursday", "name": "open_on_thursday", "triviality": 10},
                {"path": "hours.Friday", "name": "open_on_friday", "triviality": 10},
                {"path": "hours.Saturday", "name": "open_on_saturday", "triviality": 10},
                {"path": "attributes", "triviality": 10, "name": "attributes", "target_type": "json_string"},
            ],
            "batch_size": "no",
            "time_range": "last_week",
            "filter_expressions": ["is_open = 1"],
            "output": {
                "partition_definitions": [
                    {"column_name": "p_year"},
                    {"column_name": "p_month"},
                    {"column_name": "p_day"},
                ],
                "locality": "internal",
                "format": "table",
            },
            "date": "2018-10-20",
            "input": {"locality": "internal", "container": "text", "base_path": "business", "format": "json"},
        },
        "loader": {"params": {}, "name": "ByPass"},
    }
