
import pandas as pd
import SQLAlchClass

#####################################################################
#
#       Set initial variables. This must be done else the script can not run.
#
#####################################################################
fullTableName = 'MirrorSpryFortnox.InvoiceRowJson'
#fullTableName = input('Please provide the source table name in the follwing format: schema.tableName (mirror.account): ')
targetSchema = 'MirrorSpryFortnox'
#targetSchema = input('Please provide the target schema that the view is to be created in (it must exist in the DB): ')
driver='SQL Server Native Client 11.0'
server='localhost'
#instance='mssqlserver01'
uid='sqluser'
pwd='sqluser'
database='democlient'

#####################################################################
#
#      DO NOT ALTER THE CODE BELOW THIS CODE BLOCK.
#
#####################################################################

def addSpacingEnd(fullString,stringShortener,maxLen):
    i = 0
    finalString = fullString
    maxLen = maxLen - len(stringShortener)
    while i <= maxLen:
        finalString = finalString + ' '
        i = i + 1
    return finalString

def addSpacingBeg(fullString,stringShortener,maxLen):
    i = 0
    finalString = fullString
    maxLen = maxLen - len(stringShortener)
    while i <= maxLen:
        finalString = finalString + ' '
        i = i + 1
    return finalString

def standardColumn(columnName,dataType,dummyTableName,maxStringLen,maxDataLen):
    #First instance of column name used, before =
    i = 0
    columnName = f'[{columnName}]'
    firstColumnName = columnName
    maxStringLen = maxStringLen - len(columnName)
    firstColumnName = addSpacingEnd(columnName,columnName,maxDataLen)

    #Building the first section of the case statement, isnull part before subselect
    InitialPartCaseStatement = columnName
    if dataType == 'nvarchar':
        InitialPartCaseStatement = 'CASE WHEN TRIM(' + InitialPartCaseStatement + ') IS NULL THEN'
    else:
        InitialPartCaseStatement = 'CASE WHEN ' + InitialPartCaseStatement + ' IS NULL THEN      '
    InitialPartCaseStatement = addSpacingEnd(InitialPartCaseStatement,'',maxStringLen)

    #Building the subselect section of the query.
    subSelect = f'(SELECT Dummy{dataType} FROM {dummyTableName})'
    subSelect = addSpacingEnd(subSelect,dataType,maxDataLen)

    #Building the else section of the query.
    if dataType == 'nvarchar':
        elseSection = f'ELSE TRIM({columnName})'
    else:
        elseSection = f'ELSE {columnName}      '
    elseSection = addSpacingEnd(elseSection,'',maxStringLen)

    #Return final query string
    finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement} {subSelect} {elseSection} END as NVARCHAR(500))"
    print(finalstring)
    return finalstring

def convertTrueFalseToBit(columnName,maxDataLen):
    columnName = f'{columnName}'
    firstColumnName = addSpacingEnd(columnName,columnName,maxDataLen)
    finalString = f"""\n\t\t {firstColumnName} = CAST( CASE
                                                            WHEN LOWER([{columnName}])	= ''
                                                                THEN NULL
                                                            WHEN LOWER([{columnName}]) = 'true'
                                                                THEN 1
                                                            WHEN LOWER([{columnName}]) = 'false'
                                                                THEN 0
                                                            ELSE 0
                                                                END as BIT)"""
    return(finalString)

def convertDateTime(columnName,dummyTableName,maxDataLen):
    columnName = f'{columnName}'
    firstColumnName = addSpacingEnd(columnName,columnName,maxDataLen)
    finalString = f"{firstColumnName} = CAST( CASE WHEN TRIM([{columnName}]) IS NULL THEN (SELECT DummyDateTime FROM {dummyTableName}) ELSE {columnName} END as DateTime)"
    return(finalString)

def convertDate(columnName,dummyTableName,maxDataLen):
    columnName = f'{columnName}'
    firstColumnName = addSpacingEnd(columnName,columnName,maxDataLen)
    finalString = f"{firstColumnName} = CAST( CASE WHEN TRIM([{columnName}]) IS NULL THEN (SELECT DummyDate FROM {dummyTableName}) ELSE {columnName} END as Date)"
    return(finalString)

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
CREATE VIEW {targetSchema}.v{tableName} AS (
    SELECT
"""
createDummyTable =f"""
CREATE TABLE dbo.{dummyTableName}
(

"""


setOfTrueFalseToBit =''
convertTrueFalse = ''
#convertTrueFalse = input('Are there any columns that are true/false columns that should be converted to a bit (Yes/No): ')
if convertTrueFalse.lower() in ['yes','y','ye','ja','j']:
    for index, row in df.iterrows():
        print(f'{index}: {row[0]}')
    setOfTrueFalseToBit = input('Please enter the corresponding numbers separated by a comma of the columns that should be converted (1,2,3,4): ')
setOfTrueFalseToBit = ',' + setOfTrueFalseToBit + ','

setOfDateTimes = ''
convertDateTimes = ''
#convertDateTimes = input('Are there any columns that are DateTime columns that should be converted to a DateTime (Yes/No): ')
if convertDateTimes.lower() in ['yes','y','ye','ja','j']:
    createDummyTable = createDummyTable + f'Dummydatetime datetime null,\n'
    for index, row in df.iterrows():
        if  str(f',{index},') not in setOfTrueFalseToBit:
            print(f'{index}: {row[0]}')
    setOfDateTimes = input('Please enter the corresponding numbers separated by a comma of the columns that should be converted (1,2,3,4): ')
setOfDateTimes = ',' + setOfDateTimes + ','


setOfDates = ''
convertDates = ''
#convertDates = input('Are there any columns that are Date columns that should be converted to a Date (Yes/No): ')
if convertDates.lower() in ['yes','y','ye','ja','j']:
    createDummyTable = createDummyTable + f'Dummydate date null,\n'
    for index, row in df.iterrows() and str(f',{index},') not in setOfTrueFalseToBit and str(f',{index},') not in setOfDateTimes:
        if  str(f',{index},') not in setOfTrueFalseToBit and  str(f',{index},') not in setOfDateTimes:
            print(f'{index}: {row[0]}')
    setOfDates = input('Please enter the corresponding numbers separated by a comma of the columns that should be converted (1,2,3,4): ')
setOfDates = ',' + setOfDates + ','


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

        if str(f',{index},') in setOfTrueFalseToBit:
            createViewQuery = createViewQuery + f'\n{convertTrueFalseToBit(row[0],maxDataLen)}\n'
        elif str(f',{index},') in setOfDateTimes:
            createViewQuery = createViewQuery + f'\n{convertDateTime(row[0],dummyTableName,maxDataLen)}\n'
        elif str(f',{index},') in setOfDates:
            createViewQuery = createViewQuery + f'\n{convertDate(row[0],dummyTableName,maxDataLen)}\n'
        else:
            createViewQuery = createViewQuery + f'{standardColumn(row[0],row[1],dummyTableName,maxColLen,maxDataLen)}\n'
    else:
        if row[1] not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(row[1])
            createDummyTable = createDummyTable + f'Dummy{row[1]} {row[1]} null,'

        if str(f',{index},') in setOfTrueFalseToBit:
            createViewQuery = createViewQuery + f'{convertTrueFalseToBit(row[0],maxDataLen)},'
        elif str(f',{index},') in setOfDateTimes:
            createViewQuery = createViewQuery + f'\n{convertDateTime(row[0],dummyTableName,maxDataLen)},'
        elif str(f',{index},') in setOfDates:
            createViewQuery = createViewQuery + f'\n{convertDate(row[0],dummyTableName,maxDataLen)},'
        else:
            createViewQuery = createViewQuery + f'{standardColumn(row[0],row[1],dummyTableName,maxColLen,maxDataLen)},'

createDummyTable = createDummyTable + ')'
createViewQuery = createViewQuery + f'FROM {fullTableName})'
#print(f'''The following dummy table was temporarily has now been created: {createDummyTable}''')
print(f'''
The following view has now been created:
{createViewQuery}
''')
SQL_Server.executeCustomQuery(f"DROP TABLE IF EXISTS {dummyTableName}")
SQL_Server.executeCustomQuery(createDummyTable)
SQL_Server.executeCustomQuery(f"DROP VIEW IF EXISTS {targetSchema}.v{tableName}")
SQL_Server.executeCustomQuery(createViewQuery)
SQL_Server.executeCustomQuery(f"DROP TABLE {dummyTableName}")
