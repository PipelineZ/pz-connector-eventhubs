-- append with `duplicates: accept`, not replace: a feed's read is a slice, not a snapshot, so pz
-- refuses to let it rewrite a whole output (PZ0335). The id arrives as the partition key again.
INSERT INTO {{ sink('files', 'result', strategy: 'append', duplicates: 'accept', format: 'csv') }}
select "partition_key"::bigint as id, json_extract_string(body, '$.name') as name
from {{ source('hub', 'drain_in') }}
order by 1
