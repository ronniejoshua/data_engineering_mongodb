from itertools import groupby
from operator import itemgetter
from collections import OrderedDict


def aggregation_pipeline(mongo_client):
    """
    """
    db = mongo_client["nobel"]

    # Translate cursor to aggregation pipeline
    pipeline = [
        {"$match": {"gender": {"$ne": "org"}}},
        {"$project": {"bornCountry": 1, "prizes.affiliations.country": 1}},
        {"$limit": 3}
    ]

    for doc in db.laureates.aggregate(pipeline):
        print("{bornCountry}: {prizes}".format(**doc))


def aggregation_pipeline2(mongo_client):
    """
    """
    db = mongo_client["nobel"]

    original_categories = set(db.prizes.distinct("category", {"year": "1901"}))

    # Save an pipeline to collect original-category prizes
    pipeline = [
        {"$match": {"category": {"$in": list(original_categories)}}},
        {"$project": {"category": 1, "year": 1}},
        {"$sort": OrderedDict([("year", -1)])}
    ]
    cursor = db.prizes.aggregate(pipeline)
    for key, group in groupby(cursor, key=itemgetter("year")):
        missing = original_categories - {doc["category"] for doc in group}
        if missing:
            print("{year}: {missing}".format(year=key, missing=", ".join(sorted(missing))))


def aggregation_pipeline3(mongo_client):
    """
    """
    db = mongo_client["nobel"]

    # Count prizes awarded (at least partly) to organizations as a sum over sizes of "prizes" arrays.
    pipeline = [
        {"$match": {"gender": "org"}},
        {"$project": {"n_prizes": {"$size": "$prizes"}}},
        {"$group": {"_id": None, "n_prizes_total": {"$sum": "$n_prizes"}}}
    ]

    print(list(db.laureates.aggregate(pipeline)))


def gap_years_aggregated(mongo_client):
    """
        1. Filters for original prize categories (i.e. sans economics),
        2. Projects category and year,
        3. Groups distinct prize categories awarded by year,
        4. Projects prize categories not awarded by year,
        5. Filters for years with missing prize categories, and
        6. Returns a cursor of documents in reverse chronological order, one per year, each
           with a list of missing prize categories for that year.

        Remember to use field paths (precede field names with "$") to extract field values
        in expressions.
    """
    db = mongo_client["nobel"]

    original_categories = sorted(set(db.prizes.distinct("category", {"year": "1901"})))

    pipeline = [
        {"$match": {"category": {"$in": original_categories}}},
        {"$project": {"category": 1, "year": 1}},

        # Collect the set of category values for each prize year.
        {"$group": {"_id": "$year", "categories": {"$addToSet": "$category"}}},

        # Project categories *not* awarded (i.e., that are missing this year).
        {"$project": {"missing": {"$setDifference": [original_categories, "$categories"]}}},

        # Only include years with at least one missing category
        {"$match": {"missing.0": {"$exists": True}}},

        # Sort in reverse chronological order. Note that "_id" is a distinct year at this stage.
        {"$sort": OrderedDict([("_id", -1)])},
    ]

    for doc in db.prizes.aggregate(pipeline):
        print("{year}: {missing}".format(year=doc["_id"], missing=", ".join(sorted(doc["missing"]))))


def aggregation_pipeline4(mongo_client):
    """
    What proportion of laureates won a prize while affiliated with an institution in their country
    of birth? Build an aggregation pipeline to get the count of laureates who either did or did
    not win a prize with an affiliation country that is a substring of their country of birth
    -- for example, the prize affiliation country "Germany" should match the country of birth
    "Prussia (now Germany)".
    """
    db = mongo_client["nobel"]

    key_ac = "prizes.affiliations.country"
    key_bc = "bornCountry"
    pipeline = [
        {"$project": {key_bc: 1, key_ac: 1}},

        # Ensure a single prize affiliation country per pipeline document
        {"$unwind": "$prizes"},
        {"$unwind": "$prizes.affiliations"},

        # Ensure values in the list of distinct values (so not empty)
        {"$match": {key_ac: {"$in": db.laureates.distinct(key_ac)}}},
        {"$project": {"affilCountrySameAsBorn": {
            "$gte": [{"$indexOfBytes": ["$" + key_ac, "$" + key_bc]}, 0]}}},

        # Count by "$affilCountrySameAsBorn" value (True or False)
        {"$group": {"_id": "$affilCountrySameAsBorn",
                    "count": {"$sum": 1}}},
    ]
    for doc in db.laureates.aggregate(pipeline): print(doc)


def aggregation_pipeline5(mongo_client):
    """
    Some prize categories have laureates hailing from a greater number of countries than do
    other categories. You will build an aggregation pipeline for the prizes collection to
    collect these numbers, using a $lookup stage to obtain laureate countries of birth.
    """
    db = mongo_client["nobel"]

    pipeline = [
        # Unwind the laureates array
        {"$unwind": "$laureates"},
        {"$lookup": {
            "from": "laureates", "foreignField": "id",
            "localField": "laureates.id", "as": "laureate_bios"}},

        # Unwind the new laureate_bios array
        {"$unwind": "$laureate_bios"},
        {"$project": {"category": 1,
                      "bornCountry": "$laureate_bios.bornCountry"}},

        # Collect bornCountry values associated with each prize category
        {"$group": {"_id": "$category",
                    "bornCountries": {"$addToSet": "$bornCountry"}}},

        # Project out the size of each category's (set of) bornCountries
        {"$project": {"category": 1,
                      "nBornCountries": {"$size": "$bornCountries"}}},
        {"$sort": {"nBornCountries": -1}},
    ]
    for doc in db.prizes.aggregate(pipeline): print(doc)


def aggregation_pipeline6(mongo_client):
    """
    How many prizes were awarded to people who had no affiliation in their
    country of birth at the time of the award?
    """
    db = mongo_client["nobel"]

    pipeline = [
        # Limit results to people; project needed fields; unwind prizes
        {"$match": {"gender": {"$ne": "org"}}},
        {"$project": {"bornCountry": 1, "prizes.affiliations.country": 1}},
        {"$unwind": "$prizes"},

        # Count prizes with no country-of-birth affiliation
        {"$addFields": {"bornCountryInAffiliations": {"$in": ["$bornCountry", "$prizes.affiliations.country"]}}},
        {"$match": {"bornCountryInAffiliations": False}},
        {"$count": "awardedElsewhere"},
    ]

    print(list(db.laureates.aggregate(pipeline)))


def aggregation_pipeline7(mongo_client):
    """
    How many prizes were awarded to people who had no affiliation in their
    country of birth at the time of the award? Also filter out "unaffiliated" people
    """
    db = mongo_client["nobel"]

    pipeline = [
        {"$match": {"gender": {"$ne": "org"}}},
        {"$project": {"bornCountry": 1, "prizes.affiliations.country": 1}},
        {"$unwind": "$prizes"},
        {"$addFields": {"bornCountryInAffiliations": {"$in": ["$bornCountry", "$prizes.affiliations.country"]}}},
        {"$match": {"bornCountryInAffiliations": False}},
        {"$count": "awardedElsewhere"},
    ]

    # Construct the additional filter stage
    added_stage = {
        "$match": {"prizes.affiliations.country": {"$in": db.laureates.distinct("prizes.affiliations.country")}}}

    # Insert this stage into the pipeline
    pipeline.insert(3, added_stage)
    print(list(db.laureates.aggregate(pipeline)))

"""
Field paths in operator expressions are prepended by "$" to distinguish them from literal 
string values, and JSON/MongoDB "sets" are delimited by square brackets, just like lists.
"""
