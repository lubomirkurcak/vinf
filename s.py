import sys
import re
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from pyspark.sql.types import StringType, ArrayType

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

if len(sys.argv) > 1 and sys.argv[1] != '-':
    sc = SparkSession.builder \
        .master(sys.argv[1]) \
        .appName("lubomir_kurcak_sets_vinf") \
        .config("spark.executor.uri", "hdfs://147.213.75.180/user/hadmin/pyspark.tgz") \
        .getOrCreate()
else:
    sc = SparkContext.getOrCreate()

#sc.setLogLevel('WARN')

spark = SparkSession(sc)



#df = spark.read.format('xml').load('w.xml')

#df = spark.sparkContext.wholeTextFiles('w.xml')
#df = spark.sparkContext.wholeTextFiles('enwiki-latest-pages-articles1.xml-p1p41242')
#df = spark.sparkContext.wholeTextFiles('enwiki-latest-pages-articles16.xml-p20460153p20570392')

#file_to_process = 'enwiki-latest-pages-articles10.xml-p4045403p5399366'
file_to_process = 'test.xml'
if len(sys.argv) > 2:
    file_to_process = sys.argv[2]

print('FILE TO PROCESS', file_to_process)
print('FILE TO PROCESS', file_to_process)
print('FILE TO PROCESS', file_to_process)
print('FILE TO PROCESS', file_to_process)

#df = spark.read.format('xml').options(rowTag='page').load('enwiki-latest-pages-articles16.xml-p20460153p20570392')
df = spark.read.format('xml').options(rowTag='page').load(file_to_process)

df2 = df.select('revision.text._VALUE')

df3 = df2.filter(df2._VALUE.contains('wikitable'))

def yield_tables(data_tuple):
    data = data_tuple['_VALUE']
    table_start_pattern = re.compile('(\\{\\\|.*wikitable)')
    table_end_pattern = re.compile('(\\\|\\})')
    pos = 0
    while True:
        match = table_start_pattern.search(data, pos)
        if not match: break
        table_start = match.span()[0]
        match2 = table_end_pattern.search(data, table_start)
        if not match2: break
        table_end = match2.span()[1]
        pos = table_end
        yield Row(coltype='non ascii', value=data[table_start:table_end])


tables = df3.rdd.flatMap(yield_tables)

#tables_df = tables.toDF()
#tables_df.write.mode("overwrite").parquet('tables.parquet')
#tables_df = spark.read.parquet('tables.parquet')

def remove_decorations_from_string(left, right, value, keep=True):
    while left in value:
        start = value.find(left)
        end = value.find(right, start)
        if end == -1: break
        if keep:
            value = value[:start] + value[start+len(left) : end] + value[end+len(right):]
        else:
            value = value[:start] + value[end+len(right):]
    return value.strip()

def clean_wiki_cell_value(value):
    value = value.lower()
    value = value.strip()
    if value.startswith(('style', 'colspan', 'rowspan', 'align', 'width', 'bgcolor', 'scope')):
        if '|' in value:
            value = value.split('|',1)[1]
    if value.find('[[') != -1:
        value = value[value.find('[[') + 2: value.find(']]')]
        while value.find('|') != -1:
            value = value[value.find('|')+1:]
    value = value.strip()
    if value.find('&') != -1:
        value = value[0:value.find('&')]
    value = value.strip()
    if value.find("'''") != -1:
        value = value[value.find("'''")+3:]
        if value.find("'''") != -1:
            value = value[:value.find("'''")]
    value = value.strip()
    if value.startswith('{{val|'):
        value = ''
    if value.startswith('{{chem|'):
        value = ''
    value = value.strip()
    #
    value = remove_decorations_from_string('{{sub|', '}}', value)
    value = remove_decorations_from_string('{{flag|', '}}', value)
    value = remove_decorations_from_string('{{flagu|', '}}', value)
    value = remove_decorations_from_string('{{smaller|', '}}', value, False)
    value = remove_decorations_from_string('{{', '}}', value)
    #while '{{sub|' in value:
     #   start = value.find('{{sub|')
     #   end = value.find('}}', start)
     #   value = value[:start] + value[start+6 : end] + value[end+2:]
    #
    value = value.strip()
    value = re.sub(r'[^A-Za-z0-9 ]+', '', value)
    value = value.strip()
    #
    return value

def get_sets_from_table(table_contents_tuple, print_parsed_table=False):
    table_contents = table_contents_tuple[1]
    #
    name = None
    headers = []
    row_values = []
    rows = []
    #
    for line in table_contents.splitlines():
        line = line.strip()
        if(line.startswith('{|')):
            pass
        elif(line.startswith('|+')):
            name = line[2:].strip()
        elif(line.startswith('!')):
            headers.extend(line[1:].split('!!'))
        elif(line.startswith('|-') or line.startswith('|}')):
            if row_values: rows.append(row_values)
            row_values = []
        elif(line.startswith('|')):
            row_values.extend(line[1:].split('||'))
        else:
            pass#print(line)
    #
    if(print_parsed_table):
        print('\033[95m' + 'Name' + '\033[0m')
        print(name)
        print('\033[95m' + 'Headers' + '\033[0m')
        print(headers)
        print('\033[95m' + 'Rows' + '\033[0m')
        for row in rows:
            print(row)
    #
    all_results = []
    #
    result_set = []
    for header in headers[1:]:
        result_set.append(clean_wiki_cell_value(header))
    all_results.append(result_set)
    #
    for column_index in range(len(headers)):
        result_set = []
        for row in rows:
            try:
                # niektore riadky mozu mat rozlicny pocet hodnot/stlpcov
                result_set.append(clean_wiki_cell_value(row[column_index]))
            except:
                pass
        #
        all_results.append(result_set)
    #
    final_results = []
    for result in all_results:
        final_results.append(list(set(filter(None, result))))
    #
    #return final_results
    #
    for result in final_results:
        if result and len(result) > 1:
            yield result

#tables_df.rdd.map(get_sets_from_table)
sets_df_data = tables.flatMap(get_sets_from_table)

sets_df = spark.createDataFrame(data=sets_df_data, schema=ArrayType(StringType(),False))

#sets_df.write.mode("overwrite").parquet('sets.parquet')
#sets_df.write.format("com.databricks.spark.csv").save("sets.csv")

# pouzit TAB
def array_to_string(my_list):
        return '\t'.join([str(elem) for elem in my_list])

array_to_string_udf = udf(array_to_string, StringType())

csv_df = sets_df.withColumn('column_as_str', array_to_string_udf(sets_df[0]))

#csv_df.write.format("com.databricks.spark.csv").save("sets.csv")
#csv_df.select('column_as_str').write.format("com.databricks.spark.csv").save("sets.csv")
#csv_df.select('column_as_str').coalesce(1).write.format("com.databricks.spark.csv").save("sets.csv")
#csv_df.select('column_as_str').coalesce(1).write.format("text").option("header", "false").save("sets.csv")
#csv_df.select('column_as_str').coalesce(1).write.format("text").option("header", "false").mode("append").save("sets.csv")

csv_df.select('column_as_str').coalesce(1).write.format("text").option("header", "false").mode("overwrite").save("sets.csv")




