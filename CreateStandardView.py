
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


def addSpacing(columnName,dataType,dummyTableName,maxStringLen,maxDataLen):

    #First instance of column name used, before =
    i = 0
    columnName = f'[{columnName}]'
    firstColumnName = columnName
    maxStringLen = maxStringLen - len(columnName)
    while i <= maxStringLen:
        firstColumnName = firstColumnName + ' '
        i = i + 1

    #Building the first section of the case statement, isnull part before subselect
    InitialPartCaseStatement = columnName
    if dataType == 'nvarchar':
        InitialPartCaseStatement = 'CASE WHEN TRIM(' + InitialPartCaseStatement + ') IS NULL THEN'
    else:
        InitialPartCaseStatement = 'CASE WHEN ' + InitialPartCaseStatement + ' IS NULL THEN      '
    i = 0
    while i <= maxStringLen:
        InitialPartCaseStatement = InitialPartCaseStatement + ' '
        i = i + 1

    #Building the subselect section of the query.
    subSelect = f'(SELECT Dummy{dataType} FROM {dummyTableName})'
    i = 0
    maxDataLen = maxDataLen - len(dataType)
    while i <= maxDataLen:
        subSelect = subSelect + ' '
        i = i + 1


    #Building the else section of the query.
    if dataType == 'nvarchar':
        elseSection = f'ELSE TRIM({columnName})'
    else:
        elseSection = f'ELSE {columnName}      '
    i = 0
    while i <= maxStringLen:
        elseSection = elseSection + ' '
        i = i + 1

    #Return final query string
    finalstring = f"\n\t\t {firstColumnName} = {InitialPartCaseStatement} {subSelect} {elseSection} END"
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

maxColLen = 0
for index ,row in df.iterrows():
    if len(row[0]) > maxColLen:
        maxColLen = len(row[0])
maxColLen = maxColLen + 10
#print(f'maxColLen is {maxColLen}')

maxDataLen = 0
for index ,row in df.iterrows():
    if len(row[0]) > maxDataLen:
        maxDataLen = len(row[0])
maxDataLen = maxDataLen

for index ,row in df.iterrows():

    if index == dfMaxValue:
        if row[1] not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(row[1])
            createDummyTable = createDummyTable + f'Dummy{row[1]} {row[1]} null'
        createViewQuery = createViewQuery + f'{addSpacing(row[0],row[1],dummyTableName,maxColLen,maxDataLen)}\n'
    else:
        if row[1] not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(row[1])
            createDummyTable = createDummyTable + f'Dummy{row[1]} {row[1]} null,'
        createViewQuery = createViewQuery + f'{addSpacing(row[0],row[1],dummyTableName,maxColLen,maxDataLen)},'

createDummyTable = createDummyTable + ')'
createViewQuery = createViewQuery + f'FROM {fullTableName})'
#print(createViewQuery)
SQL_Server.executeCustomQuery(createDummyTable)
SQL_Server.executeCustomQuery(f"DROP VIEW IF EXISTS [dbo].v{tableName}")
SQL_Server.executeCustomQuery(createViewQuery)
SQL_Server.executeCustomQuery(f"DROP TABLE dbo.{dummyTableName}")
