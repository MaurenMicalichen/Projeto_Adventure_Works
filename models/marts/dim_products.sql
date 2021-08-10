with staging as (

    select *
    from {{ ref('stg_products')}}

)

select * from staging