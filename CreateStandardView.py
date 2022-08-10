
import pandas as pd
import SQLAlchClass

#####################################################################
#
#       Set initial variables. This must be done else the script can not run.
#
#####################################################################
fullTableName = 'MirrorFortnox.Account'
driver='SQL Server Native Client 11.0'
server='localhost'
#instance='mssqlserver01'
uid='sqluser'
pwd='sqluser'
database='Fortnox'

#####################################################################
#
#      DO NOT ALTER THE CODE BELOW THIS CODE BLOCK.
#
#####################################################################


def addSpacing(columnName,dataType,dummyTableName,maxStringLen):
    i = 0
    columnName = f'[{columnName}]'
    firstColumnName = columnName
    maxStringLen = maxStringLen - len(columnName)
    while i <= maxStringLen:
        firstColumnName = firstColumnName + ' '
        i = i + 1

    if dataType == 'nvarchar':
        finalstring = f"\n\t\t {firstColumnName} = CASE WHEN TRIM({columnName}) IS NULL THEN (SELECT Dummy{dataType} FROM {dummyTableName}) ELSE TRIM({columnName}) END"
    else:
        finalstring = f"\n\t\t {firstColumnName} = CASE WHEN {columnName} IS NULL THEN (SELECT Dummy{dataType} FROM {dummyTableName}) ELSE {columnName} END"
    return finalstring


dummyTableName = 'REPLACETHISWITHDUMMYVIEW'
tableName = fullTableName[fullTableName.index('.')+1:len(fullTableName)]

SQL_Server = SQLAlchClass.SQLServer(driver,server,uid,pwd,database)

columnInformation =f"""
    select
    	c.name,typ.name
    FROM sys.tables t
    INNER JOIN sys.schemas s
    	ON t.schema_id = s.schema_id
    INNER JOIN sys.columns c
    	ON c.object_id = t.object_id
    	INNER JOIN sys.types typ
    		ON c.system_type_id = typ.system_type_id
    WHERE s.name + '.' + t.name= '{fullTableName}'
    AND typ.name != 'sysname'
"""
response = SQL_Server.executeCustomSelect(columnInformation)
df = pd.DataFrame(response)



createViewQuery = f"""
CREATE VIEW [dbo].v{tableName} AS (
    SELECT
"""
createDummyTable =f"""
CREATE TABLE dbo.{dummyTableName}
(

"""

dfMaxValue = len(df.index) -1
listOfUsedDataTypes = []
maxLen = 0

for index ,row in df.iterrows():
    if len(row[0]) > maxLen:
        maxLen = len(row[0])
maxLen = maxLen + 10
#print(f'maxLen is {maxLen}')

for index ,row in df.iterrows():

    if index == dfMaxValue:
        if row[1] not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(row[1])
            createDummyTable = createDummyTable + f'Dummy{row[1]} {row[1]} null'
        createViewQuery = createViewQuery + f'{addSpacing(row[0],row[1],dummyTableName,maxLen)}\n'
    else:
        if row[1] not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(row[1])
            createDummyTable = createDummyTable + f'Dummy{row[1]} {row[1]} null,'
        createViewQuery = createViewQuery + f'{addSpacing(row[0],row[1],dummyTableName,maxLen)},'

createDummyTable = createDummyTable + ')'
createViewQuery = createViewQuery + f'FROM {fullTableName})'
#print(createViewQuery)
SQL_Server.executeCustomQuery(createDummyTable)
SQL_Server.executeCustomQuery(f"DROP VIEW IF EXISTS [dbo].v{tableName}")
SQL_Server.executeCustomQuery(createViewQuery)
SQL_Server.executeCustomQuery(f"DROP TABLE dbo.{dummyTableName}")
