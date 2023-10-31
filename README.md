
Anupam Rasayan India Ltd SBICC3234
https://github.com/Nagesh9890/Demonew/pulse

regex_list = [r'[^A-Za-z\&]+', r'\bNULL\b', r'\s+'


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

-------------------------------------------------------------

Entity Frameword 

import csv
import pyspark.sql.types as T
import pyspark.sql.functions as F
import re

def pycleaner(text, regex_list):
    for reg in regex_list:
        text = re.sub(reg, ' ', text)
    regex1 = re.compile(r"[^\W\d_]+|\d+")
    text2 = ' '.join(regex1.findall(text))
    return text2.lower().strip()


def get_one_to_many_category_map2(file_name, filters, clean_regex_list):
    one_many_map = {}
    with open(file_name, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for i, row in enumerate(spamreader):
            if i == 0:
                row = [x.upper() for x in row]
                entity_index = row.index('ENTITY_ID')
                keyword_index = row.index('KEY')
                cat_code_index = row.index('CATEGORY_CODE')
                channel_key = row.index('CHANNEL')
            
            elif row[channel_key] in filters:
                k1 = pycleaner(row[keyword_index], clean_regex_list)
                k2 = pycleaner(row[entity_index], clean_regex_list)
                channel = row[channel_key]
                v = row[cat_code_index]
                if k2 not in one_many_map:
                    one_many_map[k2] = []
                one_many_map[k2].append(v)
    
    conflict_dict = {}
    for ele in one_many_map:
        if len(list(set(one_many_map[ele]))) > 1:
            conflict_dict[ele] = list(set(one_many_map[ele]))

    return conflict_dict            
    

def get_specific_mapper(file_name, filters, clean_regex_list):
    mapper_dict = {}
    regex_list = []
    conflict_dict = get_one_to_many_category_map2(file_name, filters, clean_regex_list)
    with open(file_name, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for i, row in enumerate(spamreader):
            if i == 0:
                row = [x.upper() for x in row]
                entity_index = row.index('ENTITY_ID')
                keyword_index = row.index('KEY')
                cat_code_index = row.index('CATEGORY_CODE')
                channel_key = row.index('CHANNEL')
            
            elif row[channel_key] in filters:
                v11 = None
                v1 = row[keyword_index]
                if '@' in v1:
                    v11 = v1.split('@')[0]
                
                v2 = pycleaner(row[entity_index].lower(), clean_regex_list)
                if v1.startswith('REGEX::'):
                    v1 = v1.split('::')[1]
                    regex_list.append([v1, row[cat_code_index] +'|'+ row[entity_index].lower()])
                else:
                    v1 = pycleaner(v1.lower(), clean_regex_list)
                    
                    k = row[cat_code_index] + '|' + row[entity_index].lower()
                    if k not in mapper_dict:
                        mapper_dict[k] = []
                    if v1 not in mapper_dict[k] and len(v1) > 1:
                        mapper_dict[k].append(v1)
                        v1_s = v1.replace(' ','')
                        if v1_s != v1:
                            mapper_dict[k].append(v1_s)
                        
                    if v11:
                        v11 = pycleaner(v11.lower(), clean_regex_list)
                        if v11 not in mapper_dict[k] and len(v11) > 1 and v11 not in conflict_dict:
                            mapper_dict[k].append(v11)
                            v11_s = v11.replace(' ', '')
                            if v11_s != v11:
                                mapper_dict[k].append(v11_s)
                    
                    if v2 not in mapper_dict[k] and len(v2) > 1 and v2 not in conflict_dict:
                        mapper_dict[k].append(v2)
                        v2_s = v2.replace(' ','')
                        if v2_s != v2:
                            mapper_dict[k].append(v2_s)
                        
    return mapper_dict, regex_list

def initilize_keywords(root_path, sc, data_list, regex_list= [r'[^A-Za-z0-9\&]+', r'\bNULL\b', r'\s+'], data_csv= '/Transaction-Classification/MasterData/FINAL_MAPPER_DATA.csv'):
    """
    root_path -> directory where Transaction-Classification folder is present
    sc -> spark context
    data_list -> list of beneficiary name datasets from the mapper table, contains these types at max: ['EPI', 'POS', 'crowdsource', 'DD', 'Cheques', 'UPI', 'NACH']
    regex_list -> list of regular expressions for cleanup, default vaule: [r'[^A-Za-z\&]+', r'\bNULL\b', r'\s+']
    data_csv -> path to mapper file in csv format, default_value: './Transaction-Classification/MasterData/FINAL_MAPPER_DATA.csv'
    """
    sc.addPyFile(root_path + '/Transaction-Classification/textutils/viktext.py')
    from viktext import KeywordProcessor
    data_csv = root_path + data_csv
    
    res, regex_list = get_specific_mapper(data_csv, data_list, regex_list)
    kpp = KeywordProcessor()
    kpp.add_keywords_from_dict(res)
    return sc.broadcast(kpp), regex_list

def extractRegex2(default, benif_col, regex_list,  i = 0):
    if i == len(regex_list):
        return default
    else:
        return F.when(benif_col.rlike(regex_list[i][0]),regex_list[i][1]) \
                .otherwise(extractRegex2(default, benif_col, regex_list, i = i+1))
        
def textcleaner2(df, col_name, regex_list = [r'[^A-Za-z\&]+', r'\bNULL\b', r'\s+'], i = 0):
    if i == len(regex_list):
        return df.withColumn('_Benif_clean_', F.lower(F.trim(col_name)))    
    else:
        funct = F.regexp_replace(col_name, regex_list[i], ' ')
        return textcleaner2(df, col_name = funct, i = i+1)
    
def process_beneficiary(df, name_col, cat_col, entity_col, regex_list, kp_b, default_cat = '510000',
                        cleaner_regex_list=[r'[^A-Za-z0-9\&]+', r'\bNULL\b', r'\s+'], broadcasting = False):
    """
    df -> Input dataframe
    name_col -> column name containing beneficiary name
    cat_col -> output column name which would contain category code corresponing to the beneficiary name passed
    entity_col -> output column name which would contain entity id, which is currently the normalized entity name
    regex_list -> list of list of regex element1 is regex and element2 is category|
    kp_b -> Broadcast to keywordprocesser object containg mapping keywords
    default_cat -> Category to be passed in case entity not found in the list
    cleaner_regex_list -> list of regular expressions which would be eventually replaced by ' ' character, default_value: [r'[^A-Za-z\&]+', r'\bNULL\b', r'\s+']
    """
    temp_cols = ['_Benif_clean_', '_cat_benif1_', '_cat_benif2_', '_cat_code_', '_entity_id_']
    df2 = textcleaner2(df, F.col(name_col), cleaner_regex_list)
    df2 = df2.fillna('NA', subset=['_Benif_clean_'])
    
    df3 = df2.select('_Benif_clean_').dropDuplicates(['_Benif_clean_'])
    
    r = benifUDF(kp_b, default_cat)
    main_benif_categoryUDF = r.registerudf()
    
    #main_benif_categoryUDF = F.udf(main_benif_category, T.StringType())
    res = df3.withColumn('_cat_benif1_', main_benif_categoryUDF(F.col('_Benif_clean_')))
    
    if len(regex_list) > 0:
        res2 = res.withColumn('_cat_benif2_' ,extractRegex2(F.col('_cat_benif1_'), F.col('_Benif_clean_'), regex_list))
        split_col=F.split(res2['_cat_benif2_'],'\|')
        df4 = res2.withColumn("_cat_code_",split_col.getItem(0)).withColumn('_entity_id_', split_col.getItem(1)) \
            .select(['_Benif_clean_',"_cat_code_", '_entity_id_'])
    else:
        res2 = res
        split_col=F.split(res2['_cat_benif1_'],'\|')
        df4 = res2.withColumn("_cat_code_",split_col.getItem(0)).withColumn('_entity_id_', split_col.getItem(1)) \
            .select(['_Benif_clean_',"_cat_code_", '_entity_id_'])
    
    df4 = df4.filter( ( ( (F.col('_cat_code_') != default_cat) | (~F.col('_cat_code_').isNull() ) 
                       &  ((~F.col('_entity_id_').isNull()) | (F.col('_entity_id_') != ''))
                    ) ) )
    df4 = df4.withColumnRenamed('_entity_id_', entity_col).withColumnRenamed('_cat_code_', cat_col)
    
    if broadcasting == True:
        #experiment
        df4 = df4.cache()
        print (df4.count())
        df_res = df2.join(F.broadcast(df4), '_Benif_clean_', 'left')
    else:
        df_res = df2.join(df4, '_Benif_clean_', 'left')
    df_res = df_res.withColumn(cat_col, 
                               F.when(F.col(cat_col).isNull(), 
                                      default_cat).otherwise(F.col(cat_col)
                                                            )
                               )
    df_res = df_res.withColumn(entity_col, F.upper(F.col(entity_col)))
    return df_res.drop(F.col('_Benif_clean_'))


class benifUDF:
    def __init__(self, kp_b, default_cat):
        self.kp_b = kp_b
        self.default_cat = default_cat
        self.regex = re.compile(r"[^\W\d_]+|\d+")
        
    def registerudf(self):
        return F.udf(self.main_benif_category, T.StringType())
    
    def main_benif_category(self, benif_name):
        """
        UDF to find keywords presence
        depends upon global broadcase variable kp_b
        """
        benif_name2 = ' '.join(self.regex.findall(benif_name))
        words = self.kp_b.value.extract_keywords(benif_name2)
        if len(words) > 0:
            return words[0]
        else:
            return self.default_cat +'|' 
        
if __name__ == '__main__':
    #sc.addPyFile('BenefFramework.py')
    #from BenefFramework import *
    kpp, regex_list = initilize_keywords(sc,['EPI', 'crowdsource', 'DD', 'Cheques', 'POS'])
    kp_b = sc.broadcast(kpp)
    
    table = 'db_smith.smth_pool_epi'
    df = sqlContext.table(table)
    name_col = 'benef_name'
    df_res = process_beneficiary(df, 'benef_name', 'benif_cat', 'benif_id',regex_list, kp_b)
