import pandas as pd 
import pyodbc
import cx_Oracle
from datetime import datetime
pd.options.mode.chained_assignment = None

server = '10.111.11.11,1433' 
database = 'db' 
username = 'u' 
password = 'p' 

connOracle = cx_Oracle.connect('user/pwd@bd:1521/servic')
connSqlServer = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER'+server+';DATABASE='+database+';UID='+username+';PWD='+password)

cursorSqlServer = connSqlServer.cursor()

sql = """select * from bsgtabcorpempr.dbo.consulta_vencimentario"""
df = pd.read_sql(sql, connSqlServer).fillna(0)

df.columns = map(str.upper, df.columns)

dfa = df.replace('\n','', regex=True)

df = dfa[['NUM_CPF','NUM_CONTRATO','NAM_CLIENTE','DAT_CONTRATO','NUM_MONTANTE_APROVADO','VLR_CONCEDIDO','VLR_IOF','NUM_DA_PARCELA','QTD_PARCELAS','QTD_PARCELAS_PAGAS','QTD_DIAS_ATRASO','DAT_PRIMEIRO_VENCTO','DAT_VENCIMENTARIO','DAT_VCTO_MAIS_ANTIGO','NUM_PARCELA_MAIS_ANTIGA','VLR_SALDO_DEVEDOR','VLR_PARCELAS','VLR_PAGO','DAT_LIQUIDACAO','NUM_PMT_PRINCIPAL','NUM_PMT_JUROS','NUM_PMT_MORA','NUM_PMT_MULTA','NUM_PMT_RAP','NUM_PMT_IOF','NUM_PMT_EXCESSO','VLR_PRINCIPAL_E_ENCARGOS','VLR_JUROS_CET','COD_BANCO','NUM_AGENCIA','NUM_CONTA','COD_REGIONAL','DES_REGIONAL','COD_LOJA','DES_LOJA','COD_EMPREGADOR','DES_EMPREGADOR','COD_ORGAO','DES_ORGAO','COD_SUPERVISOR','NAM_SUPERVISOR','COD_OPERADOR','NAM_OPERADOR','COD_PRODUTO','NUM_MATRICULA','VLR_SALDO_OPERACAO_ANTIGA','COD_TIPO_PGTO','DAT_CONTABIL','VLR_SALDO_CONTABIL','COD_RATING_CLIENTE','COD_RATING_OPERACAO','COD_DDD','NUM_CELULAR','NUM_CL_FONE_CEL','NUM_FONE_CLIENTE','NUM_FONE_REFERENCIA_1','NAM_REFERENCIA_1','NUM_FONE_REFERENCIA_2','NAM_REFERENCIA_2','DES_EMAIL_CLIENTE','DES_ENDERECO','DES_COMPLEMENTO','DES_BAIRRO','DES_CIDADE','DES_ESTADO','NUM_CEP','DES_ENDERECO_COBRANCA','DES_COMPLEMENTO_END_COBRANCA','DES_BAIRRO_ENDERECO_COBRANCA','DES_CIDADE_ENDERECO_COBRANCA','DES_ESTADO_COBRANCA','NUM_CEP_DE_COBRANCA']]#.fillna('').apply(str)

df['NUM_CPF'] = df['NUM_CPF'].astype(str)
df['NUM_CPF'] = df.NUM_CPF.str.pad(11,side='left',fillchar='0').str.replace('.', '').str.replace('-', '')

df['DAT_VCTO_MAIS_ANTIGO']      = df['DAT_VCTO_MAIS_ANTIGO'].fillna('').apply(str)
df['NUM_PARCELA_MAIS_ANTIGA']   = df['NUM_PARCELA_MAIS_ANTIGA'].fillna('').apply(str)
df['VLR_SALDO_DEVEDOR']         = df['VLR_SALDO_DEVEDOR'].fillna(0)

df['DAT_LIQUIDACAO']      = df['DAT_LIQUIDACAO'].fillna('').apply(str)

df['NUM_PMT_JUROS']      = df['NUM_PMT_JUROS'].fillna('').apply(str)
df['NUM_PMT_MORA']      = df['NUM_PMT_MORA'].fillna('').apply(str)
df['NUM_PMT_MULTA']      = df['NUM_PMT_MULTA'].fillna('').apply(str)
df['NUM_PMT_RAP']      = df['NUM_PMT_RAP'].fillna('').apply(str)
df['NUM_PMT_IOF']      = df['NUM_PMT_IOF'].fillna('').apply(str)
df['NUM_PMT_EXCESSO']      = df['NUM_PMT_EXCESSO'].fillna('').apply(str)

df['VLR_PRINCIPAL_E_ENCARGOS']      = df['VLR_PRINCIPAL_E_ENCARGOS'].fillna(0)
df['VLR_JUROS_CET']      = df['VLR_JUROS_CET'].fillna(0)
df['COD_BANCO']      = df['COD_BANCO'].fillna('').apply(str)
df['NUM_AGENCIA']      = df['NUM_AGENCIA'].fillna('').apply(str)

df['NUM_CONTA']      = df['NUM_CONTA'].fillna('').apply(str)
df['COD_REGIONAL']      = df['COD_REGIONAL'].fillna('').apply(str)
df['DES_REGIONAL']      = df['DES_REGIONAL'].fillna('').apply(str)
df['COD_LOJA']      = df['COD_LOJA'].fillna('').apply(str)
df['DES_LOJA']      = df['DES_LOJA'].fillna('').apply(str)
df['COD_EMPREGADOR']      = df['COD_EMPREGADOR'].fillna('').apply(str)
df['DES_EMPREGADOR']      = df['DES_EMPREGADOR'].fillna('').apply(str)
df['COD_ORGAO']      = df['COD_ORGAO'].fillna('').apply(str)
df['DES_ORGAO']      = df['DES_ORGAO'].fillna('').apply(str)
df['COD_SUPERVISOR']      = df['COD_SUPERVISOR'].fillna('').apply(str)

df['NAM_SUPERVISOR']=		df['NAM_SUPERVISOR'].fillna('').apply(str)
df['COD_OPERADOR']=		df['COD_OPERADOR'].fillna('').apply(str)
df['NAM_OPERADOR']=		df['NAM_OPERADOR'].fillna('').apply(str)
df['COD_PRODUTO']=		df['COD_PRODUTO'].fillna('').apply(str)
df['NUM_MATRICULA']=		df['NUM_MATRICULA'].fillna('').apply(str)
df['VLR_SALDO_OPERACAO_ANTIGA']=		df['VLR_SALDO_OPERACAO_ANTIGA'].fillna(0)
df['COD_TIPO_PGTO']=		df['COD_TIPO_PGTO'].fillna('').apply(str)
df['DAT_CONTABIL']=		df['DAT_CONTABIL'].fillna('').apply(str)
df['VLR_SALDO_CONTABIL']=		df['VLR_SALDO_CONTABIL'].fillna(0)
df['COD_RATING_CLIENTE']=		df['COD_RATING_CLIENTE'].fillna('').apply(str)
df['COD_RATING_OPERACAO']=		df['COD_RATING_OPERACAO'].fillna('').apply(str)
df['COD_DDD']=		df['COD_DDD'].fillna('').apply(str)
df['NUM_CELULAR']=		df['NUM_CELULAR'].fillna('').apply(str)
df['NUM_CL_FONE_CEL']=		df['NUM_CL_FONE_CEL'].fillna('').apply(str)
df['NUM_FONE_CLIENTE']=		df['NUM_FONE_CLIENTE'].fillna('').apply(str)
df['NUM_FONE_REFERENCIA_1']=		df['NUM_FONE_REFERENCIA_1'].fillna('').apply(str)
df['NAM_REFERENCIA_1']=		df['NAM_REFERENCIA_1'].fillna('').apply(str)
df['NUM_FONE_REFERENCIA_2']=		df['NUM_FONE_REFERENCIA_2'].fillna('').apply(str)
df['NAM_REFERENCIA_2']=		df['NAM_REFERENCIA_2'].fillna('').apply(str)
df['DES_EMAIL_CLIENTE']=		df['DES_EMAIL_CLIENTE'].fillna('').apply(str)
df['DES_ENDERECO']=		df['DES_ENDERECO'].fillna('').apply(str)
df['DES_COMPLEMENTO']=		df['DES_COMPLEMENTO'].fillna('').apply(str)
df['DES_BAIRRO']=		df['DES_BAIRRO'].fillna('').apply(str)
df['DES_CIDADE']=		df['DES_CIDADE'].fillna('').apply(str)
df['DES_ESTADO']=		df['DES_ESTADO'].fillna('').apply(str)
df['NUM_CEP']=		df['NUM_CEP'].fillna('').apply(str)
df['DES_ENDERECO_COBRANCA']=		df['DES_ENDERECO_COBRANCA'].fillna('').apply(str)
df['DES_COMPLEMENTO_END_COBRANCA']=		df['DES_COMPLEMENTO_END_COBRANCA'].fillna('').apply(str)
df['DES_BAIRRO_ENDERECO_COBRANCA']=		df['DES_BAIRRO_ENDERECO_COBRANCA'].fillna('').apply(str)
df['DES_CIDADE_ENDERECO_COBRANCA']=		df['DES_CIDADE_ENDERECO_COBRANCA'].fillna('').apply(str)
df['DES_ESTADO_COBRANCA']=		df['DES_ESTADO_COBRANCA'].fillna('').apply(str)
df['NUM_CEP_DE_COBRANCA']=		df['NUM_CEP_DE_COBRANCA'].fillna('').apply(str)

df = list(df.to_records(index=False, column_dtypes=dict))

arrayValues=[]
for i in df:
    arrayValues.append(i)


cursorOracle = connOracle.cursor()

cursor.prepare ("""
INSERT /*array */ INTO CREDITO.TESTE_FUNCAO
(NUM_CPF,NUM_CONTRATO,NAM_CLIENTE,DAT_CONTRATO,NUM_MONTANTE_APROVADO,VLR_CONCEDIDO,VLR_IOF,NUM_DA_PARCELA,QTD_PARCELAS,QTD_PARCELAS_PAGAS
,QTD_DIAS_ATRASO,DAT_PRIMEIRO_VENCTO,DAT_VENCIMENTARIO,DAT_VCTO_MAIS_ANTIGO,NUM_PARCELA_MAIS_ANTIGA,VLR_SALDO_DEVEDOR,VLR_PARCELAS,VLR_PAGO,DAT_LIQUIDACAO,NUM_PMT_PRINCIPAL
,NUM_PMT_JUROS,NUM_PMT_MORA,NUM_PMT_MULTA,NUM_PMT_RAP,NUM_PMT_IOF,NUM_PMT_EXCESSO,VLR_PRINCIPAL_E_ENCARGOS,VLR_JUROS_CET,COD_BANCO,NUM_AGENCIA
,NUM_CONTA, COD_REGIONAL, DES_REGIONAL, COD_LOJA, DES_LOJA, COD_EMPREGADOR, DES_EMPREGADOR, COD_ORGAO, DES_ORGAO, COD_SUPERVISOR
,NAM_SUPERVISOR, COD_OPERADOR, NAM_OPERADOR, COD_PRODUTO, NUM_MATRICULA, VLR_SALDO_OPERACAO_ANTIGA, COD_TIPO_PGTO, DAT_CONTABIL, VLR_SALDO_CONTABIL, COD_RATING_CLIENTE
, COD_RATING_OPERACAO, COD_DDD, NUM_CELULAR, NUM_CL_FONE_CEL, NUM_FONE_CLIENTE, NUM_FONE_REFERENCIA_1, NAM_REFERENCIA_1, NUM_FONE_REFERENCIA_2, NAM_REFERENCIA_2
, DES_EMAIL_CLIENTE, DES_ENDERECO, DES_COMPLEMENTO, DES_BAIRRO, DES_CIDADE, DES_ESTADO, NUM_CEP, DES_ENDERECO_COBRANCA, DES_COMPLEMENTO_END_COBRANCA, DES_BAIRRO_ENDERECO_COBRANCA
, DES_CIDADE_ENDERECO_COBRANCA, DES_ESTADO_COBRANCA, NUM_CEP_DE_COBRANCA
)
values
(:NUM_CPF,:NUM_CONTRATO,:NAM_CLIENTE,to_date(:DAT_CONTRATO,'dd/mm/yyyy'),:NUM_MONTANTE_APROVADO,:VLR_CONCEDIDO,:VLR_IOF,:NUM_DA_PARCELA,:QTD_PARCELAS,:QTD_PARCELAS_PAGAS
 ,:QTD_DIAS_ATRASO,to_Date(:DAT_PRIMEIRO_VENCTO,'dd/mm/yyyy'),to_Date(:DAT_VENCIMENTARIO,'dd/mm/yyyy'), :DAT_VCTO_MAIS_ANTIGO,:NUM_PARCELA_MAIS_ANTIGA,:VLR_SALDO_DEVEDOR, :VLR_PARCELAS,:VLR_PAGO,:DAT_LIQUIDACAO,:NUM_PMT_PRINCIPAL
 ,:NUM_PMT_JUROS,:NUM_PMT_MORA,:NUM_PMT_MULTA,:NUM_PMT_RAP,:NUM_PMT_IOF,:NUM_PMT_EXCESSO,:VLR_PRINCIPAL_E_ENCARGOS,:VLR_JUROS_CET,:COD_BANCO,:NUM_AGENCIA
 ,:NUM_CONTA, :COD_REGIONAL, :DES_REGIONAL, :COD_LOJA, :DES_LOJA, :COD_EMPREGADOR, :DES_EMPREGADOR, :COD_ORGAO, :DES_ORGAO, :COD_SUPERVISOR
 ,:NAM_SUPERVISOR, :COD_OPERADOR, :NAM_OPERADOR, :COD_PRODUTO, :NUM_MATRICULA, :VLR_SALDO_OPERACAO_ANTIGA, :COD_TIPO_PGTO, :DAT_CONTABIL, :VLR_SALDO_CONTABIL, :COD_RATING_CLIENTE
 ,:COD_RATING_OPERACAO, :COD_DDD, :NUM_CELULAR, :NUM_CL_FONE_CEL, :NUM_FONE_CLIENTE, :NUM_FONE_REFERENCIA_1, :NAM_REFERENCIA_1, :NUM_FONE_REFERENCIA_2, :NAM_REFERENCIA_2
 ,:DES_EMAIL_CLIENTE, :DES_ENDERECO, :DES_COMPLEMENTO, :DES_BAIRRO, :DES_CIDADE, :DES_ESTADO, :NUM_CEP, :DES_ENDERECO_COBRANCA, :DES_COMPLEMENTO_END_COBRANCA, :DES_BAIRRO_ENDERECO_COBRANCA
 ,:DES_CIDADE_ENDERECO_COBRANCA, :DES_ESTADO_COBRANCA, :NUM_CEP_DE_COBRANCA

)""")

cursorOracle.executemany(None, arrayValues)

connOracle.commit()
cursorOracle.close()

connOracle.close()

cursorSqlServer.close()
connSqlServer.close()
