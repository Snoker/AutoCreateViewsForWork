
import pandas as pd
import SQLAlchClass
import re

#####################################################################
#
#       Set initial variables. This must be done else the script can not run.
#
#####################################################################

fullTableName = 'EDWWork.TimePunch'
driver='SQL Server Native Client 11.0'
server='localhost'
#instance='mssqlserver01'
uid='viewCreation'
pwd='viewCreation'
database='HampusTestZone'

#####################################################################
#
#      DO NOT ALTER THE CODE BELOW THIS CODE BLOCK.
#
#####################################################################


#####################################################################
#
#      DO NOT ALTER THE CODE BELOW THIS CODE BLOCK.
#
#####################################################################

def createColumnName(columnName):
    finalString = ''
    setOfCapitalLetters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    setOfIndexesOfCapitalLetters = []
    i = 0
    for item in columnName:
        if item in setOfCapitalLetters:
            setOfIndexesOfCapitalLetters.append(i)
        i = i + 1

    previousIndex = 0
    amountOfLoops = 0
    maxLoops = len(setOfIndexesOfCapitalLetters)

    for index in setOfIndexesOfCapitalLetters:
        amountOfLoops = amountOfLoops + 1
        finalString = f'{finalString} {columnName[previousIndex:index]}'
        if amountOfLoops == maxLoops:
            finalString = f'{finalString} {columnName[index:len(columnName)]}'
        previousIndex = index

    finalString = finalString.strip()
    finalString = f'[{finalString}]'
    return finalString


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


dfMaxValue = len(df.index) -1

for index ,row in df.iterrows():
    if 'Alternative' not in row[0]:
        if index == dfMaxValue:
            if row[0][-3:] != 'Key':
                createViewQuery = f'{createViewQuery}{createColumnName(row[0])} = [{row[0]}]\n'
            else:
                createViewQuery = f'{createViewQuery}{(row[0])} = [{row[0]}]\n'
        else:
            if row[0][-3:] != 'Key':
                createViewQuery = f'{createViewQuery}{createColumnName(row[0])} = [{row[0]}],\n'
            else:
                createViewQuery = f'{createViewQuery}[{(row[0])}] = [{row[0]}],\n'


createViewQuery = createViewQuery + f'FROM {fullTableName})'

print(createViewQuery)

SQL_Server.executeCustomQuery(f"DROP VIEW IF EXISTS [dbo].v{tableName}")
SQL_Server.executeCustomQuery(createViewQuery)
