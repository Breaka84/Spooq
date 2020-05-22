def get_context_vars():
    return {
        "entity_type": "user",
        "date": "2018-10-20",
        "batch_size": "daily"
    }


def get_metadata():
    return {
        "extractor": {
            "params": {
                "input_path": "user/p_year=2018/p_month=10/p_day=20"
            }, 
            "name": "JSONExtractor"
        }, 
        "transformers": [
            {
                "params": {
                    "exploded_elem_name": "friends_element", 
                    "path_to_array": "friends"
                }, 
                "name": "Exploder"
            }, 
            {
                "params": {
                    "filter_expression": "average_stars >= 2.5"
                }, 
                "name": "Sieve"
            }, 
            {
                "params": {
                    "filter_expression": "isnotnull(friends_element) and friends_element <> \"None\""
                }, 
                "name": "Sieve"
            }, 
            {
                "params": {
                    "mapping": [
                        [
                            "user_id", 
                            "user_id", 
                            "StringType"
                        ], 
                        [
                            "review_count", 
                            "review_count", 
                            "LongType"
                        ], 
                        [
                            "average_stars", 
                            "average_stars", 
                            "DoubleType"
                        ], 
                        [
                            "elite_years", 
                            "elite", 
                            "json_string"
                        ], 
                        [
                            "friend", 
                            "friends_element", 
                            "StringType"
                        ]
                    ]
                }, 
                "name": "Mapper"
            }, 
            {
                "params": {
                    "thresholds": {
                        "average_stars": {
                            "max": 5, 
                            "min": 1
                        }
                    }
                }, 
                "name": "ThresholdCleaner"
            }, 
            {
                "params": {
                    "group_by": [
                        "user_id", 
                        "friend"
                    ], 
                    "order_by": [
                        "review_count"
                    ]
                }, 
                "name": "NewestByGroup"
            }
        ], 
        "context_variables": {
            "pipeline_type": "batch", 
            "entity_type": "user", 
            "value_ranges": {
                "average_stars": {
                    "max": 5, 
                    "min": 1
                }
            }, 
            "level_of_detail": "std", 
            "level_of_detail_int": 5, 
            "mapping": [
                {
                    "target_type": "StringType", 
                    "triviality": 1, 
                    "name": "user_id", 
                    "desc": "22 character unique user id, maps to the user in user.json"
                }, 
                {
                    "path": "name", 
                    "target_type": "StringType", 
                    "has_pii": "yes", 
                    "name": "first_name", 
                    "desc": "the user's first name - anonymized"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 1, 
                    "name": "review_count", 
                    "desc": "the number of reviews they've written"
                }, 
                {
                    "target_type": "StringType", 
                    "name": "yelping_since", 
                    "desc": "when the user joined Yelp, formatted like YYYY-MM-DD"
                }, 
                {
                    "target_type": "DoubleType", 
                    "triviality": 1, 
                    "name": "average_stars", 
                    "desc": "average rating of all reviews"
                }, 
                {
                    "path": "elite", 
                    "target_type": "json_string", 
                    "triviality": 5, 
                    "name": "elite_years", 
                    "desc": "the years the user was elite"
                }, 
                {
                    "path": "friends_element", 
                    "target_type": "StringType", 
                    "triviality": 1, 
                    "name": "friend", 
                    "desc": "the user's friend as user_ids"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "useful", 
                    "desc": "number of useful votes sent by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "funny", 
                    "desc": "number of funny votes sent by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "cool", 
                    "desc": "number of cool votes sent by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "name": "fans", 
                    "desc": "number of fans the user has"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_hot", 
                    "desc": "number of hot compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_more", 
                    "desc": "number of more compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_profile", 
                    "desc": "number of profile compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_cute", 
                    "desc": "number of cute compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_list", 
                    "desc": "number of list compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_note", 
                    "desc": "number of note compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_plain", 
                    "desc": "number of plain compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_cool", 
                    "desc": "number of cool compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_funny", 
                    "desc": "number of funny compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_writer", 
                    "desc": "number of writer compliments received by the user"
                }, 
                {
                    "target_type": "LongType", 
                    "triviality": 10, 
                    "name": "compliment_photos", 
                    "desc": "number of photo compliments received by the user"
                }
            ], 
            "batch_size": "daily", 
            "time_range": "last_day", 
            "filter_expressions": [
                "average_stars >= 2.5", 
                "isnotnull(friends_element) and friends_element <> \"None\""
            ], 
            "output": {
                "partition_definitions": [
                    {
                        "column_name": "p_year"
                    }, 
                    {
                        "column_name": "p_month"
                    }, 
                    {
                        "column_name": "p_day"
                    }
                ], 
                "locality": "internal", 
                "repartition_size": 10, 
                "format": "table"
            }, 
            "date": "2018-10-20", 
            "input": {
                "locality": "internal", 
                "container": "text", 
                "base_path": "user", 
                "format": "json"
            }, 
            "schema": {
                "arrays_to_explode": [
                    "friends"
                ], 
                "grouping_keys": [
                    "user_id", 
                    "friend"
                ], 
                "needs_deduplication": "yes", 
                "sorting_keys": [
                    "review_count"
                ]
            }
        }, 
        "loader": {
            "params": {
                "auto_create_table": True, 
                "partition_definitions": [
                    {
                        "default_value": 2018, 
                        "column_type": "IntegerType", 
                        "column_name": "p_year"
                    }, 
                    {
                        "default_value": 10, 
                        "column_type": "IntegerType", 
                        "column_name": "p_month"
                    }, 
                    {
                        "default_value": 20, 
                        "column_type": "IntegerType", 
                        "column_name": "p_day"
                    }
                ], 
                "overwrite_partition_value": True, 
                "repartition_size": 10, 
                "clear_partition": True, 
                "db_name": "user", 
                "table_name": "users_daily_partitions"
            }, 
            "name": "HiveLoader"
        }
    }