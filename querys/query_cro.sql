select 

weekofyear(date_trunc('week',"first_successful_tx_date")) as cohort,
 CASE 
        WHEN acquisition_channel = 'online_marketing' THEN 'OM'
        WHEN acquisition_channel = 'second_hand' THEN 'Second Hand'
        WHEN acquisition_channel = 'ZAZ' THEN 'Others'
        WHEN acquisition_channel = 'Prisma' THEN 'Others'
        WHEN acquisition_channel = 'Retail' THEN 'Others'
        WHEN acquisition_channel = 'others' THEN 'Others'


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

count(merchant_code) as MERCHANT_CODE

from SALES_INTELLIGENCE_BR_PROD.MART.SALES_GROWTH_MERCHANTS_BR
where "first_successful_tx_date" between '2024-09-29' and ':date:'
group by 1,2,3
order by cohort asc