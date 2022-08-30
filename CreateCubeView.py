
import pandas as pd
import SQLAlchClass
import re

#####################################################################
#
#       Set initial variables. This must be done else the script can not run.
#
#####################################################################

fullTableName = 'DMARN.FactForecast'
#fullTableName = input('Please provide the source table name in the follwing format: schema.tableName (mirror.account): ')
targetSchema = 'CubeARN'
#targetSchema = input('Please provide the target schema that the view is to be created in (it must exist in the DB): ')
driver='SQL Server Native Client 11.0'
server='localhost'
#instance='mssqlserver01'
uid='sqluser'
pwd='sqluser'
database='ATrain_DW'


#####################################################################
#
#      DO NOT ALTER THE CODE BELOW THIS CODE BLOCK.
#
#####################################################################

def createColumnName(columnName,maxStringLen):
    finalString = ''
    setOfCapitalLetters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    setOfIndexesOfCapitalLetters = []

    #We only add spacing between words for non surrogate keys. Surrogate key is identified by ending with 'Key'.
    if columnName[-3:] != 'Key':
        #Find location of all capital letters in the string.
        i = 0
        for item in columnName:
            if item in setOfCapitalLetters:
                setOfIndexesOfCapitalLetters.append(i)
            i = i + 1
        previousIndex = 0
        amountOfLoops = 0
        maxLoops = len(setOfIndexesOfCapitalLetters)

        #Add spacing based on capital letter, I.E StringValueOne = String Value One
        for index in setOfIndexesOfCapitalLetters:
            amountOfLoops = amountOfLoops + 1
            if index - previousIndex == 1:
                finalString = f'{finalString}{columnName[previousIndex:index]}'
            else:
                finalString = f'{finalString} {columnName[previousIndex:index]}'

            if amountOfLoops == maxLoops:
                if index - previousIndex == 1:
                    finalString = f'{finalString}{columnName[index:len(columnName)]}'
                else:
                    finalString = f'{finalString} {columnName[index:len(columnName)]}'
            previousIndex = index
        finalString = finalString.strip()
        finalString = f'[{finalString}]'
        i = 0

        #Add padding to make all strings the same length, makes the SQL code easier to read and adhears to our coding standards.
        maxStringLen = maxStringLen - len(finalString)
        while i <= maxStringLen:
            finalString = finalString + ' '
            i = i + 1
        finalString = f'     {finalString}'
        return finalString

    else:
        #Add padding to make all strings the same length, makes the SQL code easier to read and adhears to our coding standards.
        finalString = columnName
        finalString = f'[{finalString}]'
        i = 0
        maxStringLen = maxStringLen - len(finalString)
        while i <= maxStringLen:
            finalString = finalString + ' '
            i = i + 1
        finalString = f'     {finalString}'
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
CREATE VIEW {targetSchema}.v{tableName} AS (
    SELECT
"""

maxLen = 0
dfMaxValue = len(df.index) -1

for index ,row in df.iterrows():
    if len(row[0]) > maxLen:
        maxLen = len(row[0])
maxLen = maxLen + 10

for index ,row in df.iterrows():
    if 'Alternate' not in row[0]:
        if index == dfMaxValue:
            createViewQuery = f'{createViewQuery}{createColumnName(row[0],maxLen)} = [{row[0]}]\n'
        else:
            createViewQuery = f'{createViewQuery}{createColumnName(row[0],maxLen)} = [{row[0]}],\n'


createViewQuery = createViewQuery + f'FROM {fullTableName})'

print(f'''
The following view has now been created:
{createViewQuery}
''')

SQL_Server.executeCustomQuery(f"DROP VIEW IF EXISTS {targetSchema}.v{tableName}")
SQL_Server.executeCustomQuery(createViewQuery)
