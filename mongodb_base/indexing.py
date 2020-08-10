from collections import Counter


def creating_index(mongo_client):
    """
    A prize might be awarded to a single laureate or to several. For each prize category,
    report the most recent year that a single laureate -- rather than several -- received
    a prize in that category. As part of this task, you will ensure an index that speeds
    up finding prizes by category and then sorting results by decreasing year.
    """
    db = mongo_client["nobel"]

    # Specify an index model for compound sorting
    index_model = [("category", 1), ("year", -1)]
    db.prizes.create_index(index_model)

    # Collect the last single-laureate year for each category
    report = ""
    for category in sorted(db.prizes.distinct("category")):
        doc = db.prizes.find_one(
            {"category": category, "laureates.share": "1"},
            sort=[("year", -1)]
        )
        report += "{category}: {year}\n".format(**doc)

    print(report)


def born_affiliated(mongo_client):
    """
    Some countries are, for one or more laureates, both their country of birth ("bornCountry")
    and a country of affiliation for one or more of their prizes ("prizes.affiliations.country").
    You will find the five countries of birth with the highest counts of such laureates.
    """
    db = mongo_client["nobel"]

    # Ensure an index on country of birth
    db.laureates.create_index([("bornCountry", 1)])

    # Collect a count of laureates for each country of birth
    # Dictionary comprehension
    n_born_and_affiliated = {
        country: db.laureates.count_documents({
            "bornCountry": country,
            "prizes.affiliations.country": country
        })
        for country in db.laureates.distinct("bornCountry")
    }

    five_most_common = Counter(n_born_and_affiliated).most_common(5)
    print(five_most_common)