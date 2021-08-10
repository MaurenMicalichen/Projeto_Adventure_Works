with dados_motivo as (
    select
        row_number() over (order by salesreasonid) as sk_motivo_de_venda, --chave auto-incremental
        salesreasonid as id_motivo_de_venda,
        reasontype as tipo_de_motivo,
        name as motivo_de_venda
    from {{ source('adventure_works_elt', 'salesreason' )}}
    ),

    dados_motivo_vendas as (
    select 
        salesorderid as id_pedido,
        salesreasonid as id_motivo_de_venda
	from {{source('adventure_works_elt', 'salesorderheadersalesreason')}}
    ),

    dados_juntos as (
    select
        dados_motivo.sk_motivo_de_venda,
        dados_motivo.id_motivo_de_venda,
        dados_motivo.tipo_de_motivo,
        dados_motivo_vendas.id_pedido
    from dados_motivo
    left join dados_motivo_vendas on dados_motivo.id_motivo_de_venda = dados_motivo_vendas.id_motivo_de_venda
    where id_pedido is not Null
    ),

    final as (
        SELECT
        id_pedido,
        STRING_AGG(tipo_de_motivo, ', ') as motivo_de_venda
        FROM dados_juntos
        GROUP BY id_pedido
    )

select * from final
