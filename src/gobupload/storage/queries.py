from gobcore.enum import ImportMode


def get_comparison_query(source, current, temporary, fields, mode=ImportMode.FULL):
    # The using part of the statements contains the fnctional identification for the entity:
    # functional source (source), functional id (_id) and a volgnummer if the entity has states
    using = ",".join(fields)

    # On a full upload any missing items are deletions, for any other upload missing items are skipped
    action_on_missing = "DELETE" if mode in {ImportMode.DELETE, ImportMode.FULL} else "SKIP"

    # The techical source id is returned
    # _source_id for the new source id, _entity_source_id for the current source_id
    return f"""
SELECT * FROM (
SELECT
    {temporary}._tid,
    {temporary}._source,
    {current}._source AS _entity_source,
    {current}._tid AS _entity_tid,
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
    {temporary}._tid,
    {temporary}._source,
    {current}._source AS _entity_source,
    {current}._tid AS _entity_tid,
    {temporary}._original_value,
    {current}._last_event,
    COALESCE({temporary}._hash, {current}._hash),
    CASE
        WHEN {temporary}._tid IS NULL AND {current}._date_deleted IS NULL THEN '{action_on_missing}'
        WHEN {temporary}._tid IS NULL AND {current}._date_deleted IS NOT NULL THEN 'SKIP'
        WHEN (
            {current}._tid IS NULL OR
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
WHERE type != 'SKIP' AND (_source = '{source}' OR _entity_source = '{source}')
ORDER BY type
"""
