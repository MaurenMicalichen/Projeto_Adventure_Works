with
    clientes as (
        select *
        from {{ ref('dim_customer') }}

    ),
    vendedores as (
        select *
        from {{ ref('dim_sales_person') }}

    ),
    produtos as (
        select *
        from {{ ref('dim_products') }}

    ),
    info_motivos as (
        select *
        from {{ ref('stg_sales_reason') }}

    ),
    info_cartao as (
        select *
        from {{ ref('stg_credit_card') }}

    ),
    info_endereco as (
        select *
        from {{ ref('stg_country_region') }}
    ),

    vendas_com_sk as (
      select
            clientes.sk_cliente as fk_cliente,
            vendedores.sk_entidade_comercial as fk_entidade_comercial,
            info_motivos.motivo_de_venda,
            vendas.id_pedido,
            vendas.id_metodo_envio,
            vendas.id_endereco_cobranca,
            vendas.id_endereco_entrega,
            vendas.id_cartao_credito,
            vendas.status,
            vendas.data_pedido
        from {{ ref('stg_sales_order_header') }} as vendas
        left join clientes on vendas.id_cliente = clientes.id_cliente
        left join vendedores on vendas.id_vendedor = vendedores.id_entidade_comercial
        left join info_motivos on vendas.id_pedido = info_motivos.id_pedido
    ),

    detalhes_pedidos_com_sk as (
      select
            order_detail.id_pedido,	
            produtos.sk_produto as fk_produto,
            order_detail.id_detalhe_pedido,
            order_detail.preco_unitario,
            order_detail.id_oferta_especial,		
            order_detail.quantidade,	
            order_detail.desconto_preco_unitario	
        from {{ ref('stg_sales_order_details') }} as order_detail
        left join produtos on order_detail.id_produto = produtos.id_produto
    ),

    final as (
      select
            vendas_com_sk.fk_cliente,
            vendas_com_sk.fk_entidade_comercial,
            vendas_com_sk.id_pedido,
            vendas_com_sk.id_metodo_envio,
            vendas_com_sk.id_endereco_cobranca,
            vendas_com_sk.id_endereco_entrega,
            vendas_com_sk.status,
            vendas_com_sk.data_pedido,
            vendas_com_sk.motivo_de_venda,
            detalhes_pedidos_com_sk.fk_produto,
            detalhes_pedidos_com_sk.id_detalhe_pedido,
            detalhes_pedidos_com_sk.preco_unitario,
            detalhes_pedidos_com_sk.id_oferta_especial,		
            detalhes_pedidos_com_sk.quantidade,	
            detalhes_pedidos_com_sk.desconto_preco_unitario,
            info_endereco.cidade,
            info_endereco.nome_estado,	
            info_endereco.nome_pais,
            info_cartao.tipo_cartao
        from vendas_com_sk
        left join detalhes_pedidos_com_sk on vendas_com_sk.id_pedido = detalhes_pedidos_com_sk.id_pedido
        left join info_endereco on vendas_com_sk.id_endereco_entrega = info_endereco.sk_endereco
        left join info_cartao on vendas_com_sk.id_cartao_credito = info_cartao.id_cartao_credito
    )

select * from final
