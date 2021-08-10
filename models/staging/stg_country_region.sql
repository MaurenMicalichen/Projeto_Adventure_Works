with dados_regioes as (
    select 
        countryregioncode as codigo_pais,	
        name as nome_pais
	from {{source('adventure_works_elt', 'countryregion')}}
    ),

    dados_estados as (
    select 
        stateprovinceid	as id_estado,	
        countryregioncode as codigo_pais,	
        name as nome_estado,
        territoryid as id_territorio,		
        stateprovincecode as codigo_estados
	from {{source('adventure_works_elt', 'stateprovince')}}
    ),

    dados_cidades as (
    select 
        row_number() over (order by addressid) as sk_endereco, --chave auto-incremental
        addressid as id_endereco,
        stateprovinceid	as id_estado,	
        city as cidade
	from {{source('adventure_works_elt', 'address')}}
    ),

    final as (
    select
    dados_cidades.sk_endereco,
    dados_cidades.id_endereco,
    dados_cidades.id_estado,	
    dados_cidades.cidade,
    dados_estados.codigo_pais,	
    dados_estados.nome_estado,
    dados_estados.id_territorio,		
    dados_estados.codigo_estados,
    dados_regioes.nome_pais
    from dados_estados
    left join dados_regioes on dados_estados.codigo_pais = dados_regioes.codigo_pais
    left join dados_cidades on dados_estados.id_estado = dados_cidades.id_estado
    )

select * from final