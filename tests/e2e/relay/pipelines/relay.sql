-- The id travels as the partition key, not in the body: a `partition_key:` column is not part of
-- the whole-row JSON, so the seed's events carry only {"name": ...} and the key beside them.
INSERT INTO {{ sink('hub', 'orders_out') }}
select
    "partition_key"::bigint as id,
    json_extract_string(body, '$.name') as name,
    enqueued_time as seen_at
from {{ source('hub', 'orders_in') }}
order by "partition", sequence_number
