INSERT INTO {{ sink('hub', 'seed_out') }}
select id, name from {{ source('files', 'rows') }} order by id
