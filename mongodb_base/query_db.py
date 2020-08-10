from bson import Regex
from operator import itemgetter
from pprint import pprint


def filter_non_operator(mongo_client):
    db = mongo_client["nobel"]

    # Create a filter for laureates who died in the USA
    criteria = {"diedCountry": "USA"}
    count = db.laureates.count_documents(criteria)
    print(count)

    # Create a filter for laureates who died in the USA but were born in Germany
    criteria = {"diedCountry": "USA",
                "bornCountry": "Germany"}
    count = db.laureates.count_documents(criteria)
    print(count)

    # Create a filter for Germany-born laureates who died in the USA and with the first name "Albert"
    criteria = {"bornCountry": "Germany",
                "diedCountry": "USA",
                "firstname": "Albert"}
    count = db.laureates.count_documents(criteria)
    print(count)


def filter_operators(mongo_client):
    db = mongo_client["nobel"]

    # Filter for laureates with at least three prizes
    # Find one laureate with at least three prizes
    criteria = {"prizes.2": {"$exists": True}}
    doc = db.laureates.find_one(criteria)
    print(doc)

    # Filter and count for documents without a "born" field
    criteria = {"born": {"$exists": False}}
    count = db.laureates.count_documents(criteria)
    print(count)

    # Filter and count for laureates born in Austria with non-Austria prize affiliation
    criteria = {"bornCountry": "Austria",
                "prizes.affiliations.country": {"$ne": "Austria"}}
    count = db.laureates.count_documents(criteria)
    print(count)

    # Filter and count for laureates who died in the USA and were not born there
    criteria = {"diedCountry": "USA",
                "bornCountry": {"$ne": "USA"},
                }
    count = db.laureates.count_documents(criteria)
    print(count)

    # Filter and count for laureates born in the USA, Canada, or Mexico
    criteria = {"bornCountry":
                    {"$in": ["USA", "Canada", "Mexico"]}
                }
    count = db.laureates.count_documents(criteria)
    print(count)

    # Number of laureates with recorded dates of birth earlier than the year 1900
    criteria = {"born": {"$lt": "1900"}}
    count = db.laureates.count_documents(criteria)
    print(count)


def distinct_assertion(mongo_client):
    db = mongo_client["nobel"]
    print(db.prizes.distinct("category"))
    print(db.laureates.distinct("prizes.category"))
    assert set(db.prizes.distinct("category")) == set(db.laureates.distinct("prizes.category"))


def distinct_set_operation(mongo_client):
    db = mongo_client["nobel"]
    # Countries recorded as countries of death but not as countries of birth
    countries = set(db.laureates.distinct("diedCountry")) - set(db.laureates.distinct("bornCountry"))
    print(countries)


def distinct_count(mongo_client):
    db = mongo_client["nobel"]
    count = len(db.laureates.distinct("prizes.affiliations.country"))
    print(count)


def distinct_filter(mongo_client):
    db = mongo_client["nobel"]

    # In which countries have USA-born laureates had affiliations for their prizes?
    return db.laureates.distinct('prizes.affiliations.country', {'bornCountry': 'USA'})


def distinct_filter_set(mongo_client):
    """
        Confirm via an assertion that "literature" is the only prize category with
        no prizes shared by three or more laureates.
    """
    db = mongo_client["nobel"]

    # Save a filter for prize documents with three or more laureates
    criteria = {"laureates.2": {"$exists": True}}

    # Save the set of distinct prize categories in documents satisfying the criteria
    triple_play_categories = set(db.prizes.distinct("category", criteria))
    assert set(db.prizes.distinct("category")) - triple_play_categories == {"literature"}


def element_match(mongo_client):
    """
        What is the approximate ratio of the number of laureates who won an unshared
        ({"share": "1"}) prize in physics after World War II ({"year": {"$gte": "1945"}})
        to the number of laureates who won a shared prize in physics after World War II?
    """
    db = mongo_client["nobel"]

    db.laureates.count_documents({
        "prizes": {"$elemMatch": {
            "category": "physics",
            "share": {"$ne": "1"},
            "year": {"$gte": "1945"}}}})


def element_match_ratio(mongo_client):
    """
    What is this(Unshared/Shared) ratio for prize categories other than
    physics, chemistry, and medicine?
    """
    db = mongo_client["nobel"]

    # Save a filter for laureates with unshared prizes
    unshared = {
        "prizes": {"$elemMatch": {
            "category": {"$nin": ["physics", "chemistry", "medicine"]},
            "share": "1",
            "year": {"$gte": "1945"},
        }}}

    # Save a filter for laureates with shared prizes
    shared = {
        "prizes": {"$elemMatch": {
            "category": {"$nin": ["physics", "chemistry", "medicine"]},
            "share": {"$ne": "1"},
            "year": {"$gte": "1945"},
        }}}

    ratio = db.laureates.count_documents(unshared) / db.laureates.count_documents(shared)
    print(ratio)


def comparision_operator(mongo_client):
    db = mongo_client["nobel"]

    # Save a filter for organization laureates with prizes won before 1945
    before = {
        "gender": "org",
        "prizes.year": {"$lt": "1945"},
    }

    # Save a filter for organization laureates with prizes won in or after 1945
    in_or_after = {
        "gender": "org",
        "prizes.year": {"$gte": "1945"},
    }

    n_before = db.laureates.count_documents(before)
    n_in_or_after = db.laureates.count_documents(in_or_after)
    ratio = n_in_or_after / (n_in_or_after + n_before)
    print(ratio)


def mongodb_regex(mongo_client):
    """
    How many laureates in total have a first name beginning with "G" and a surname beginning with "S"?
    """
    db = mongo_client["nobel"]
    count = db.laureates.count_documents({"firstname": Regex("^G"), "surname": Regex("^S")})
    print(count)

    # Filter for laureates with "Germany" in their "bornCountry" value
    criteria = {"bornCountry": Regex("Germany")}
    print(set(db.laureates.distinct("bornCountry", criteria)))

    # Filter for laureates with a "bornCountry" value starting with "Germany"
    criteria = {"bornCountry": Regex("^Germany")}
    print(set(db.laureates.distinct("bornCountry", criteria)))

    # Fill in a string value to be sandwiched between the strings "^Germany " and "now"
    criteria = {"bornCountry": Regex("^Germany " + "\(" + "now")}
    print(set(db.laureates.distinct("bornCountry", criteria)))

    # Fill in a string value to be sandwiched between the strings "now" and "$"
    criteria = {"bornCountry": Regex("now" + " Germany\)" + "$")}
    print(set(db.laureates.distinct("bornCountry", criteria)))


def count_docs_collection(mongo_client):
    db = mongo_client["nobel"]

    # count documents in a collection
    d_filter = {}

    n_prizes = db.prizes.count_documents(d_filter)
    n_laureates = db.laureates.count_documents(d_filter)

    print(f'Number of Prizes Awarded: {n_prizes}')
    print(f'Number of Nobel Laureates: {n_laureates}')


def mongodb_projections(mongo_client):
    """
    """
    db = mongo_client["nobel"]

    # Find laureates whose first name starts with "G" and last name starts with "S"
    # Use projection to select only firstname and surname
    docs = db.laureates.find(
        filter={"firstname": {"$regex": "^G"},
                "surname": {"$regex": "^S"}},
        projection=["firstname", "surname"])

    # Iterate over docs and concatenate first name and surname
    full_names = [doc["firstname"] + " " + doc["surname"] for doc in docs]

    # Print the full names
    print(full_names)


def data_validation(mongo_client):
    """
    check that for each prize, all the shares of all the laureates add up to 1!
    """
    db = mongo_client["nobel"]

    # Save documents, projecting out laureates share
    prizes = db.prizes.find({}, ['laureates.share'])

    # Iterate over prizes
    for prize in prizes:
        # Initialize total share
        total_share = 0

        # Iterate over laureates for the prize
        for laureate in prize["laureates"]:
            # convert the laureate's share to float and add the reciprocal to total_share
            total_share += 1 / float(laureate["share"])

        # Print the total share
        print(total_share)


def mongodb_sorting(mongo_client):
    """
    """
    db = mongo_client["nobel"]

    docs = list(db.laureates.find(
        {"born": {"$gte": "1900"}, "prizes.year": {"$gte": "1954"}},
        {"born": 1, "prizes.year": 1, "_id": 0},
        sort=[("prizes.year", 1), ("born", -1)]))
    for doc in docs[:5]:
        print(doc)


def all_laureates(prize):
    # sort the laureates by surname
    sorted_laureates = sorted(prize["laureates"], key=itemgetter("surname"))

    # extract surnames
    surnames = [laureate["surname"] for laureate in sorted_laureates]

    # concatenate surnames separated with " and "
    all_names = " and ".join(surnames)

    return all_names


def sort_projection(mongo_client):
    """
    """
    db = mongo_client["nobel"]

    # find physics prizes, project year and name, and sort by year
    docs = db.prizes.find(
        filter={"category": "physics"},
        projection=["year", "laureates.firstname", "laureates.surname"],
        sort=[("year", 1)])
    return docs


def all_laureates_sorted(docs):
    # print the year and laureate names (from all_laureates)
    for doc in docs:
        print("{year}: {names}".format(year=doc["year"], names=all_laureates(doc)))


def gap_years(mongo_client):
    """
    utilize sorting by multiple fields to see which categories are missing in which years.
    """
    db = mongo_client["nobel"]
    # original categories from 1901
    original_categories = db.prizes.distinct("category", {"year": "1901"})
    print(original_categories)

    # project year and category, and sort
    docs = db.prizes.find(
        filter={},
        projection={"year": 1, "category": 1, "_id": 0},
        sort=[("year", -1), ("category", 1)]
    )

    # print the documents
    for doc in docs:
        print(doc)


def filter_projection_sort_limit(mongo_client):
    """
    Find the first five prizes with one or more laureates sharing 1/4 of the prize. Project
    our prize category, year, and laureates' motivations.
    """
    db = mongo_client["nobel"]

    # Fetch prizes with quarter-share laureate(s)
    filter_ = {'laureates.share': '4'}

    # Save the list of field names
    projection = ['category', 'year', 'laureates.motivation']

    # Save a cursor to yield the first five prizes
    cursor = db.prizes.find(filter_, projection).sort("year").limit(5)
    pprint(list(cursor))


def get_particle_laureates(mongo_client, page_number=1, page_size=3):
    """
    You and a friend want to set up a website that gives information on Nobel laureates with awards
    relating to particle phenomena. You want to present these laureates one page at a time, with
    three laureates per page. You decide to order the laureates chronologically by award year.
    When there is a "tie" in ordering (i.e. two laureates were awarded prizes in the same year),
    you want to order them alphabetically by surname.

    Write a function to retrieve a page of data
    """
    db = mongo_client["nobel"]

    if page_number < 1 or not isinstance(page_number, int):
        raise ValueError("Pages are natural numbers (starting from 1).")
    particle_laureates = list(
        db.laureates.find(
            {"prizes.motivation": {"$regex": "particle"}},
            ["firstname", "surname", "prizes"])
            .sort([("prizes.year", 1), ("surname", 1)])
            .skip(page_size * (page_number - 1))
            .limit(page_size)
    )
    return particle_laureates


def get_particle_pages():
    """
    """
    # Collect and save the first nine pages
    pages = [get_particle_laureates(page_number=page) for page in range(1, 9)]
    pprint(pages[0])
