
import pandas as pd
import SQLAlchClass
import re

#####################################################################
#
#       Set initial variables. This must be done else the script can not run.
#
#####################################################################

database='HampusLek'
sourceSchema = input('Please provide the source schema. ( Not including brackets ): ')
targetSchema = input('Please provide the target schema. ( Not including brackets ): ')
driver='SQL Server Native Client 11.0'
server='localhost'
#instance='mssqlserver01'
uid='sqluser'
pwd='sqluser'



#####################################################################
#
#      DO NOT ALTER THE CODE BELOW THIS CODE BLOCK.
#
#####################################################################

def createColumnName(columnName,maxStringLen):
    finalString = ''
    setOfCapitalLetters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    setOfIndexesOfCapitalLetters = []
    print(f'columnName:  {columnName}, maxStringLen: {maxStringLen} ')
    #We only add spacing between words for non surrogate keys. Surrogate key is identified by ending with 'Key'.
    if columnName[-3:] != 'Key':
        #Find location of all capital letters in the string.
        print('its not key')
        i = 0
        for item in columnName:
            if item in setOfCapitalLetters:
                setOfIndexesOfCapitalLetters.append(i)
            i = i + 1
        previousIndex = 0
        amountOfLoops = 0
        maxLoops = len(setOfIndexesOfCapitalLetters)
        if maxLoops == 0:
            finalString = f'{finalString}{columnName}'
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
        print('it is key')
        #Add padding to make all strings the same length, makes the SQL code easier to read and adhears to our coding standards.
        finalString = columnName
        print(f'finalstring is {finalString}')
        finalString = f'[{finalString}]'
        i = 0
        maxStringLen = maxStringLen - len(finalString)
        while i <= maxStringLen:
            finalString = finalString + ' '
            i = i + 1
        finalString = f'     {finalString}'
        print(f'finalstring is now {finalString}')
        return finalString




SQL_Server = SQLAlchClass.SQLServer(driver,server,uid,pwd,database)


schemaInformation = f"""
select
	[TableName]			= t.name
FROM sys.tables t
INNER JOIN sys.schemas s
	ON t.schema_id = s.schema_id
WHERE s.name = '{sourceSchema}'
"""
response = SQL_Server.executeCustomSelect(schemaInformation)
df_SchemaTables = pd.DataFrame(response)



for outerIndex,schemaRow in df_SchemaTables.iterrows():
    fullTableNameWithSchema = sourceSchema + '.' + schemaRow[0]
    tableName = schemaRow[0]
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
        WHERE s.name + '.' + t.name= '{fullTableNameWithSchema}'
        AND typ.name != 'sysname'
    """
    #print(columnInformation)
    response = SQL_Server.executeCustomSelect(columnInformation)
    df = pd.DataFrame(response)

    #print(df)
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


    createViewQuery = createViewQuery + f'FROM {fullTableNameWithSchema})'

    print(f'''
    The following view has now been created:
    {createViewQuery}
    ''')

    SQL_Server.executeCustomQuery(f"DROP VIEW IF EXISTS {targetSchema}.v{tableName}")
    SQL_Server.executeCustomQuery(createViewQuery)
