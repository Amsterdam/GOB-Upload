def get_comparison_query(current, temporary, fields):
    # The using part of the statements contains the fnctional identification for the entity:
    # functional source (source), functional id (_id) and a volgnummer if the entity has states
    using = ",".join(fields)

    # The techical source id is returned
    # _source_id for the new source id, _entity_source_id for the current source_id
    return f"""
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
        WHEN {temporary}._source_id IS NULL AND {current}._date_deleted IS NULL THEN 'DELETE'
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
