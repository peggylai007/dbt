with source_rental as (
    select * from {{source('dvdrental_source','rental')}}
),

source_customer as (
    select * from {{source('dvdrental_source','customer')}}
),

active_rental as (
    select r.customer_id,
    count(*) as active_rentals_count
    from source_rental r
    where r.return_date is null
    group by 
        r.customer_id
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        coalesce(ar.active_rentals_count, 0) as active_rentals_count
    from
    source_customer c
    left join
        active_rental ar on c.customer_id = ar.customer_id
    where
        c.last_update > current_date - interval '13 year'
    order by
    c.customer_id    
)

select * from final

