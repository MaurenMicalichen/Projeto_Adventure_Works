with
    soma_quantidade as (
        select sum(quantidade) as sum_val
        from {{ref('fact_sales')}}
        where data_pedido between '2012-08-01' and '2012-08-30'
    )

select * from soma_quantidade where sum_val != 5559