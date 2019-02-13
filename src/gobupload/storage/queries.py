def get_comparison_query(current, temporary, collection):
    # If the collection has_states, take volgnummer into account
    entity_id = collection['entity_id']
    using = f"{entity_id}, volgnummer" if collection.get('has_states') else f"{entity_id}"

    return f"""
SELECT
    {temporary}._source_id,
    {current}._source_id AS _entity_source_id,
    {current}._last_event,
    {temporary}._hash,
    'CONFIRM' AS type
FROM {temporary}
FULL OUTER JOIN (
    SELECT * FROM {current}
    WHERE _date_deleted IS NULL
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
    {current}._last_event,
    COALESCE({temporary}._hash, {current}._hash),
    CASE
        WHEN {temporary}._source_id IS NULL THEN 'SKIP'
        WHEN {current}._source_id IS NULL THEN 'ADD'
        ELSE 'MODIFY'
    END AS type
FROM {temporary}
FULL OUTER JOIN (
    SELECT * FROM {current}
    WHERE _date_deleted IS NULL
    ) AS {current} USING ({using})
WHERE (
    {temporary}._hash
) IS DISTINCT FROM (
    {current}._hash
)"""
