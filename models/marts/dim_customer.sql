with staging as (

    select *
    from {{ ref('stg_customer')}}

)

select * from staging