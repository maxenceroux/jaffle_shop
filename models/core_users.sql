{{ config(schema='to_delete') }}

with users as (
    select cast(id as int) as id,
    email 
    from {{source('airbyte_metabase', 'core_user')}}
)
select * from users