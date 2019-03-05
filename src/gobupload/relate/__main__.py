from gobcore.model import GOBModel

from gobupload.relate.relate import relate


def results():
    # results = relate("gebieden", "wijken", "ligt_in_stadsdeel")  # has-states - has-states
    # results = relate("nap", "peilmerken", "ligt_in_bouwblok")  # no-states - has-states
    # has-states - no-states ??
    # results = relate("meetbouten", "metingen", "hoort_bij_meetbout") # no-states - no-states

    # many reference
    results = relate("meetbouten", "metingen", "refereert_aan_referentiepunten")

    for result in results:
        print(result)


def tables():
    model = GOBModel()

    for catalog_name in model.get_catalog_names():
        catalog = model.get_catalog(catalog_name)
        for collection_name in model.get_collection_names(catalog_name):
            collection = model.get_collection(catalog_name, collection_name)
            for reference, value in collection['references'].items():
                dst_catalog_name, dst_collection_name = value['ref'].split(':')
                dst_catalog = model.get_catalog(dst_catalog_name)
                if dst_catalog is None:
                    continue
                dst_collection = model.get_collection(dst_catalog_name, dst_collection_name)
                if dst_collection is None:
                    continue
                name = f"{catalog['abbreviation']}_{collection['abbreviation']}_" + \
                       f"{dst_catalog['abbreviation']}_{dst_collection['abbreviation']}_" + \
                       f"{reference}"
                print(name, len(name))


results()
#  tables()
