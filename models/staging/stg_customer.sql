with dados_cliente as (
    select
    row_number() over (order by customerid) as sk_cliente, --chave auto-incremental
    customerid as id_cliente,
    personid as id_pessoa,
    territoryid as id_territorio,	
    storeid	as id_loja
    from {{ source('adventure_works_elt', 'customer' )}}
    ),

    dados_pessoas as (
    select 
        businessentityid as id_entidade_comercial,	
        lastname as sobrenome,	
        persontype as tipo_pessoa,	
        emailpromotion as promocacao_email,			
        firstname as nome,
        CONCAT(firstname, ' ', lastname) as nome_completo
	from {{source('adventure_works_elt', 'person')}}
    ),

    dados_lojas as (
    select 
        businessentityid as id_entidade_comercial,
        salespersonid as id_vendedor,
        name as nome_loja
    from {{source('adventure_works_elt', 'store')}}
    ),

    final as (
    select
        dados_cliente.sk_cliente,
        dados_cliente.id_cliente,
        dados_cliente.id_pessoa,		
        dados_cliente.id_territorio,	
        dados_cliente.id_loja,
        dados_pessoas.id_entidade_comercial,
        dados_pessoas.sobrenome,	
        dados_pessoas.tipo_pessoa,
        dados_pessoas.promocacao_email,			
        dados_pessoas.nome,
        dados_pessoas.nome_completo,
        dados_lojas.id_vendedor,
        dados_lojas.nome_loja,
        CASE
        WHEN dados_pessoas.nome_completo is not null
        THEN dados_pessoas.nome_completo
        ELSE dados_lojas.nome_loja
        END as nome_cliente
    from dados_cliente
    left join dados_pessoas on dados_cliente.id_pessoa = dados_pessoas.id_entidade_comercial
    left join dados_lojas on dados_cliente.id_loja = dados_lojas.id_entidade_comercial
    )

select * from final
/*select * from dados_cliente*/