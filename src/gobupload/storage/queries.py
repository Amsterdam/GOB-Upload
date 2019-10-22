def get_comparison_query(current, temporary, fields, mode):
    # The using part of the statements contains the fnctional identification for the entity:
    # functional source (source), functional id (_id) and a volgnummer if the entity has states
    using = ",".join(fields)

    action_on_missing = "DELETE" if mode == "full" else "SKIP"

    # The techical source id is returned
    # _source_id for the new source id, _entity_source_id for the current source_id
    query = f"""
SELECT * FROM (
SELECT
    {temporary}._source_id,
    {current}._source_id AS _entity_source_id,
    {temporary}._original_value,
    {current}._last_event,
    {temporary}._hash,
    CASE
        WHEN {current}._date_deleted IS NULL THEN 'CONFIRM'
        ELSE 'ADD'
    END AS type
FROM {temporary}
FULL OUTER JOIN (
    SELECT * FROM {current}
    ) AS {current} USING ({using})
WHERE (
    {temporary}._hash
) IS NOT DISTINCT FROM (
    {current}._hash
)
UNION ALL
SELECT
    {temporary}._source_id,
    {current}._source_id AS _entity_source_id,
    {temporary}._original_value,
    {current}._last_event,
    COALESCE({temporary}._hash, {current}._hash),
    CASE
        WHEN {temporary}._source_id IS NULL AND {current}._date_deleted IS NULL THEN '{action_on_missing}'
        WHEN {temporary}._source_id IS NULL AND {current}._date_deleted IS NOT NULL THEN 'SKIP'
        WHEN (
            {current}._source_id IS NULL OR
            {current}._date_deleted IS NOT NULL
        ) THEN 'ADD'
        ELSE 'MODIFY'
    END AS type
FROM {temporary}
FULL OUTER JOIN (
    SELECT * FROM {current}
    ) AS {current} USING ({using})
WHERE (
    {temporary}._hash
) IS DISTINCT FROM (
    {current}._hash
)
) AS Q
WHERE type != 'SKIP'
ORDER BY type
"""
    print("QUERY")
    print(query)
    input("Press Enter to continue...")
    return query
