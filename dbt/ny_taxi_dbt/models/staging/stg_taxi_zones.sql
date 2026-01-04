
select
    cast(LocationID as int64) as location_id,
    trim(Borough) as borough,
    trim(Zone) as zone,
    trim(service_zone) as service_zone
from {{ ref('taxi_zone_lookup') }}