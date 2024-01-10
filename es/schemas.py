schemaMain = {
    "mappings": {
        "properties": {
            "location": {
                "type": "geo_point"
            }
        }
    }
}

__fields = ["fullname", "province", "city", "region", "street", "community", "group_number", "natural_village", "road",
          "address_number", "building_site", "unit", "floor", "room", "courtyard", "building_name", "company"]

for field in __fields:
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
