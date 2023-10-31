select cod_acct_no,prod_type from
(select cod_prod,cod_acct_no from db_stage.stg_fcr_hdm_vw_ch_acct_mast
where batch_id = '1697491418491')acct_mast
inner join
(select cod_prod,cod_prod_desc,
(case when cod_prod_type = '1' then 'SA'
     when cod_prod_type = '2' then 'CA' end) as prod_type
from db_stage.stg_fcr_fcrlive_1_ba_prod_prodtype_xref
where batch_id = '1697498495713' and cod_module = 'CH' and cod_prod_type in ('1','2')
)prod
on acct_mast.cod_prod = prod.cod_prod
