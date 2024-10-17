select
weekofyear(date_trunc('week', cro."first_successful_tx_date")) as cohort,
weekofyear(date_trunc('week', server_time_created_at)) as tx_week,


CASE 
        WHEN acquisition_channel = 'online_marketing' THEN 'OM'
        WHEN acquisition_channel = 'second_hand' THEN 'Second Hand'
else acquisition_channel end as acquisition_channel,


CASE 
        WHEN acquisition_subchannel = 'card_reader' THEN 'Card Readers'
        WHEN acquisition_subchannel = 'intents' THEN 'Produtos Digitais'
        WHEN acquisition_subchannel = 'organic' THEN 'App + Orgânico'
        WHEN acquisition_subchannel = 'app' THEN 'App + Orgânico'
        WHEN acquisition_subchannel = 'offline_sale' THEN '-'
        WHEN acquisition_subchannel = 'online_sale' THEN '-'
        WHEN acquisition_subchannel = 'payments_link_sale' THEN '-'
else acquisition_subchannel end as acquisition_subchannel,

round(sum(amount)) as tpv


from SUMUP_DWH_PROD.SRC_PAYMENT.TRANSACTIONS t
left join SUMUP_DWH_PROD.SRC_PAYMENT.TRANSACTION_CARDS tc on t.id = tc.transaction_id
left JOIN (
        SELECT DISTINCT transaction_id FROM SALES_INTELLIGENCE_BR_PROD.MART.BR_CHB_AND_REFUNDS
    ) chb_and_ref ON t.id = chb_and_ref.transaction_id
left join "SUMUP_DWH_PROD"."SRC_PAYMENT"."CARD_READERS" cr on cr.id = t.card_reader_id join SUMUP_DWH_PROD.src_payment.merchants m on t.merchant_id = m.id
LEFT JOIN "SUMUP_DWH_PROD"."SRC_PAYMENT"."DEVICES" d ON t.device_id = d.id
left join SUMUP_DWH_PROD.SRC_PAYMENT.ENTRY_MODES em on t.entry_mode_id = em.id
JOIN SALES_INTELLIGENCE_BR_PROD.mart.sales_growth_merchants_br cro ON m.merchant_code = cro.MERCHANT_code
left join SUMUP_DWH_PROD.SRC_RISK_AND_FRAUD_BR.TPV_REFUND_CHBS chb on chb.transaction_id = t.id
where 1=1
    and tx_result = 11
    AND t.acquirer_code NOT in (10200, 10300, 31337, 30100)
    and is_test_account = 'FALSE'
    and t.currency = 'BRL'
    and (case
           when process_as= '1' then 'credit'
           when process_as = '2' then 'debt'
           when em.mode in ('QR_CODE_PIX') then 'pix'
                   else 'other'
    end <> 'other')
    --and server_time_created_at >= '2024-10-06' --inicio da semana numbers (muda toda atualização) ou início do quarte por conta da mudança de passado
    --and server_time_created_at < '2024-10-13' --final da semana numbers (muda toda atualização)
    --and cro."first_successful_tx_date" >= '2024-09-29' -- Nao mexe (inicio de quarter)
    --and cro."first_successful_tx_date" < '2024-10-13' -- Data final do mês (muda toda atualização)
    and server_time_created_at between '2024-09-29' and ':date:'
    and cro."first_successful_tx_date" between '2024-09-29' and ':date:'
    and has_chb <> 'TRUE'
    and chb_and_ref.transaction_id is null
    and acquisition_channel in ('RaF','Executives','Consultants','Retail','Integradores','second_hand','online_marketing')
    group by all
    order by tx_week asc