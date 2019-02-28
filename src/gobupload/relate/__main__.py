from gobupload.relate import relate

# relate("gebieden", "wijken", "ligt_in_stadsdeel") # has-states - has-states
relate("nap", "peilmerken", "ligt_in_bouwblok")  # no-states - has-states
# has-states - no-states ??
# relate("meetbouten", "metingen", "hoort_bij_meetbout") # no-states - no-states

# many reference
# relate("meetbouten", "metingen", "refereert_aan_referentiepunten")
