with source_rental as (
    select * from {{source('dvdrental_source','rental')}}
),

final as (
    select r.customer_id,
    count(*) as active_rentals_count
    from source_rental r
    where r.return_date is null
    group by 
        r.customer_id
)

select * from final