with dados_vendedores as (
    select
        row_number() over (order by businessentityid) as sk_entidade_comercial, --chave auto-incremental
        businessentityid as id_entidade_comercial,					
        territoryid as id_territorio
    from {{ source('adventure_works_elt', 'salesperson' )}}
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

    final as (
    select
        dados_vendedores.sk_entidade_comercial,			
        dados_vendedores.id_territorio,
        dados_pessoas.id_entidade_comercial,	
        dados_pessoas.sobrenome,	
        dados_pessoas.tipo_pessoa,
        dados_pessoas.promocacao_email,			
        dados_pessoas.nome,
        dados_pessoas.nome_completo
    from dados_vendedores
    left join dados_pessoas on dados_vendedores.id_entidade_comercial = dados_pessoas.id_entidade_comercial
    )

select * from final