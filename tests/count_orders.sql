with
    soma_quantidade as (
        select COUNT(DISTINCT id_pedido) as count_val
        from {{ref('fact_sales')}}
        where data_pedido between '2012-08-01' and '2012-08-30'
    )

select * from soma_quantidade where count_val != 277

