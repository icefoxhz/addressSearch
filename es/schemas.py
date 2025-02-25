import copy

es_fullname_field = "fullname"

es_schema_text_fields = [
    "prov",
    "city",
    "district",
    "devzone",
    "town",
    "community",
    "village_group",
    "road",
    "poi",
    "subpoi",
    "detail",
    "distance",
    "intersection",
    "redundant",
    "others"
]

es_schema_number_fields = [
    "roadno",
    "houseno",
    "cellno",
    "floorno",
    "roomno"
]

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


def add_fields():
    for field in es_schema_text_fields:
        add_schema_field(field)

    for field in es_schema_number_fields:
        add_schema_field_ex(field, "long")


add_fields()
