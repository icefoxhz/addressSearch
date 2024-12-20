import copy

schemaMain = {
    "mappings": {
        "properties": {
            "location": {
                "type": "geo_point"
            }
        }
    }
}


def add_schema_field(field_name, field_type="keyword"):
    schemaMain["mappings"]["properties"][field_name] = {
        "type": "keyword",
        "store": True,
        "fields": {
            "keyword": {
                "type": field_type
            }
        }
    }


def add_schema_field_ex(field_name, field_type="keyword"):
    schemaMain["mappings"]["properties"][field_name] = {
        "type": field_type,
    }


# def add_schema_field(schema, field_name, field_type="keyword"):
#     schema["mappings"]["properties"][field_name] = {
#         "type": "keyword",
#         "store": True,
#         "fields": {
#             "keyword": {
#                 "type": field_type
#             }
#         }
#     }
#
#
# def add_schema_field_ex(schema, field_name, field_type="keyword"):
#     schema["mappings"]["properties"][field_name] = {
#         "type": field_type,
#     }


# def copy_schema():
#     return copy.deepcopy(schemaMain)


es_schema_fields_region = "region"
es_schema_fields_street = "street"
es_schema_fields_fir = ["fir_1", "fir_2", "fir_3", "fir_4", "fir_5", "fir_6", "fir_7", "fir_8"]
es_schema_fields_main = ["f_main"]
es_schema_fields_mid = ["mid_1", "mid_2", "mid_3", "mid_4"]
es_schema_fields_last = ["last_1", "last_2", "last_3", "last_4", "last_5", "last_6"]
# 单独存一下数值型的mid_1，为的是查询的时候可以查接近的数字
es_schema_field_building_number = "building_number"
es_fullname_field = "fullname"
es_schema_fields = (
        [es_schema_fields_region, es_schema_fields_street]
        +
        [es_fullname_field]
        +
        es_schema_fields_fir
        +
        es_schema_fields_main
        +
        es_schema_fields_mid
        +
        es_schema_fields_last
)

for field in es_schema_fields:
    add_schema_field(field)
add_schema_field_ex(es_schema_field_building_number, "long")

# print("schemaMain: ", schemaMain)
