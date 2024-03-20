schemaMain = {
    "mappings": {
        "properties": {
            "location": {
                "type": "geo_point"
            }
        }
    }
}

# __fields = ["fullname", "province", "city", "region", "street", "community", "group_number", "natural_village", "road",
#           "address_number", "building_site", "unit", "floor", "room", "courtyard", "building_name", "company"]

es_schema_fields_fir = ["fir_1", "fir_2", "fir_3", "fir_4", "fir_5", "fir_6", "fir_7", "fir_8"]
es_schema_fields_main = ["f_main"]
es_schema_fields_mid = ["mid_1", "mid_2", "mid_3"]

es_schema_fields = ["fullname"] + es_schema_fields_fir + es_schema_fields_main + es_schema_fields_mid

for field in es_schema_fields:
    schemaMain["mappings"]["properties"][field] = {
        "type": "keyword",
        "store": True,
        "fields": {
            "keyword": {
                "type": "keyword"
            }
        }
    }

# print("schemaMain: ", schemaMain)
