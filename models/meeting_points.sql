{{ config(schema='to_delete') }}

with meeting_points as (
    SELECT
  CAST(id AS int) AS id,
  NULL AS _fivetran_deleted,
  _airbyte_emitted_at AS _fivetran_synced,
  city,
  CAST(LEFT(created_at, 19) AS datetime) AS created_at,
  CAST(LEFT(deleted_at, 19) AS datetime) AS deleted_at,
  CONCAT('{"type":"point","coordinates":[',CAST(SPLIT(SPLIT(location, ',')[SAFE_OFFSET(0)],'(')[SAFE_OFFSET(1)] AS float64),',', CAST(SPLIT(SPLIT(location, ',')[SAFE_OFFSET(1)],')')[SAFE_OFFSET(0)] AS float64),']}') AS location,
  CAST(REPLACE(SPLIT(location, ',')[SAFE_OFFSET(0)],'(', '') AS float64) AS location_x,
  CAST(REPLACE(SPLIT(location, ',')[SAFE_OFFSET(1)],')','') AS float64) AS location_y,
  map_link,
  name,
  CAST(place_id AS int) AS place_id,
  timezone,
  CAST(LEFT(updated_at, 19) AS datetime) AS updated_at,
  zipcode,
  cast(gearbox_type as int) as gearbox_type
FROM
  {{source('airbyte_fivetran_test_migration', 'meeting_points')}}
) 
select * from meeting_points