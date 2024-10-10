select 

weekofyear(date_trunc('week',"first_successful_tx_date")) as cohort, -- numerando a semana do ano
 CASE 
        WHEN acquisition_channel = 'online_marketing' THEN 'OM' -- Ajustar nome dos canais
        WHEN acquisition_channel = 'second_hand' THEN 'Second Hand'

else acquisition_channel end as acquisition_channel,

CASE 
        WHEN acquisition_subchannel = 'card_reader' THEN 'Card Readers' -- ajustar nome dos sub canais 
        WHEN acquisition_subchannel = 'intents' THEN 'Produtos Digitais'
        WHEN acquisition_subchannel = 'organic' THEN 'App + Orgânico'
        WHEN acquisition_subchannel = 'app' THEN 'App + Orgânico'
        WHEN acquisition_subchannel = 'offline_sale' THEN '-'
        WHEN acquisition_subchannel = 'online_sale' THEN '-'
        WHEN acquisition_subchannel = 'payments_link_sale' THEN '-'
else acquisition_subchannel end as acquisition_subchannel,

--signup_intent,
count(merchant_code) -- agrupando o número de MIDs por canal

from SALES_INTELLIGENCE_BR_PROD.MART.SALES_GROWTH_MERCHANTS_BR
where "first_successful_tx_date" between '2024-09-29' and '2024-10-05' -- Puxando do primeiro dia do quarter
and acquisition_channel in ('RaF','Executives','Consultants','Retail','Integradores','second_hand','online_marketing') -- apenas canais que eu quero
group by all
order by cohort asc; -- ajustando em ordem crescente de cohort