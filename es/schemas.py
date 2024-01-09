schemaMain = {
    "mappings": {
        "properties": {
            "location": {
                "type": "geo_point"
            }
        }
    }
}

fields = ["fullname", "province", "city", "region", "street", "community", "group_number", "natural_village", "road",
          "address_number", "building_site", "unit", "floor", "room", "courtyard", "building_name", "company"]

for field in fields:
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
