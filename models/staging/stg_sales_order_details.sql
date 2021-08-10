with dados_detalhes_da_venda as (
    select
        salesorderid as id_pedido,	
        salesorderdetailid as id_detalhe_pedido,
        unitprice as preco_unitario,
        specialofferid as id_oferta_especial,
        productid as id_produto,		
        orderqty as quantidade,	
        unitpricediscount as desconto_preco_unitario		
    from {{ source('adventure_works_elt', 'salesorderdetail' )}}
)

select * from dados_detalhes_da_venda
