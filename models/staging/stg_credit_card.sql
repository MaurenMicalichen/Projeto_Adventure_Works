with dados_cartao as (
    select
        row_number() over (order by creditcardid) as sk_cartao, --chave auto-incremental
        creditcardid as id_cartao_credito,
        cardtype as tipo_cartao,
        cardnumber as numero_cartao
    from {{ source('adventure_works_elt', 'creditcard' )}}
)

select * from dados_cartao