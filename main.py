import pandas as pd
import pandasql as ps

# Read main file db_table.csv
data_f = pd.read_csv('./db_table.csv', delimiter=',', names=['date', 'updateDate', 'code1',
                               'code2', 'code3', 'value', 'source'])

# Create extra columns and add to output
siCode_sql_data = '''
        SELECT
        CASE WHEN bitCode == 1 
        THEN
            CASE WHEN code3 == 'AP' OR code3 == 'AH' THEN 'A' 
            WHEN code3 == 'PRD' THEN 'B' 
            WHEN code3 == 'YLD' THEN 'BpA' 
            ELSE 0 END 
        ELSE
            CASE WHEN code3 == 'AP' or code3 == 'AH' THEN 'H' 
            WHEN code3 == 'PRD' THEN 'T' 
            WHEN code3 == 'YLD' THEN 'TpH' 
            ELSE 0 END 
        END AS siCode 
        FROM data_f
'''
bitCode_sql_data = ''' SELECT CASE WHEN source == 'S1' THEN 1 ELSE 0 END AS bitCode FROM data_f'''

data_f['bitCode'] = ps.sqldf(bitCode_sql_data)
data_f['siCode'] = ps.sqldf(siCode_sql_data)
data_f.to_csv('new_file.csv', index=False)


# Frontend input part
input_data = eval(
    input('Please enter data:'
          '\nexample: {"code1": "shC", "code2": "C"}'))
code1, code2 = input_data['code1'], input_data['code2']

dict_values = {}
data_dict = {}

if isinstance(input_data, dict):
    filter_data = data_f[(data_f.code1.str.match(code1)) & (data_f.code2.str.contains(code2))]
    dict_values[(code1, code2)] = filter_data.source.values
    print(dict_values) # return  {(#G_code1#, #G_code2#): [#source1#, #source2#, ..., #sourceN#]}

    # create output with sources
    all_sources = set(*dict_values.values())
    for source in all_sources:
        source_filter_data = filter_data[filter_data.source == source].values
        data_dict[(code1, code2, source)] = source_filter_data
    print(data_dict)  # return {(#G_code1#, #G_code2#, #sourceK#): [data rows]}







