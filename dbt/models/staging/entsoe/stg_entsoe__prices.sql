with source as (

    select * from {{ source('entsoe', 'bronze_prices') }}

),

renamed as (

    select
        cast(zone as string) as zone,
        cast(resolution as string) as resolution,
        cast(interval_start_utc as timestamp) as interval_start_utc,
        cast(interval_end_utc as timestamp) as interval_end_utc,
        cast(value as double) as price_eur_mwh,
        cast(unit as string) as unit,
        cast(document_mrid as string) as document_mrid,
        cast(time_series_mrid as string) as time_series_mrid,
        cast(revision_number as int) as revision_number,
        cast(created_datetime_utc as timestamp) as created_datetime_utc,
        cast(ingested_at_utc as timestamp) as ingested_at_utc,
        cast(source_file as string) as source_file
    from source
    where {{ start_date_filter('interval_start_utc') }}

)

select * from renamed
