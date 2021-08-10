with dados_produtos as (
    select 
        row_number() over (order by productid) as sk_produto, --chave auto-incremental
        productid as id_produto,
        class as classe,		
        productnumber as codigo_produto,			
        name as nome_produto
	
    from {{source('adventure_works_elt', 'product')}}
)

select * from dados_produtos