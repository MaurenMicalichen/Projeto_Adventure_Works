with dados_vendas as (
    select 
        /*row_number() over (order by salesorderid) as sk_pedido, --chave auto-incremental */
        salesorderid as id_pedido,
        salespersonid as id_vendedor,
        customerid as id_cliente,
        shipmethodid as id_metodo_envio,
        billtoaddressid	as id_endereco_cobranca,
        shiptoaddressid	as id_endereco_entrega,
        creditcardid as id_cartao_credito,
        territoryid	as id_territorio,
        status,
        orderdate as data_pedido
    from {{source('adventure_works_elt', 'salesorderheader')}}
)

select * from dados_vendas
