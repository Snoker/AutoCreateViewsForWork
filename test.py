
import pandas as pd
import SQLAlchClass

#####################################################################
#
#       Set initial variables. This must be done else the script can not run.
#
#####################################################################
fullTableName = 'Mirror.test2'
#fullTableName = input('Please provide the source table name in the follwing format: schema.tableName (mirror.account): ')
targetSchema = 'Mirror'
#targetSchema = input('Please provide the target schema that the view is to be created in (it must exist in the DB): ')
driver='SQL Server Native Client 11.0'
server='localhost'
#instance='mssqlserver01'
uid='sqluser'
pwd='sqluser'
database='HampusLek'


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
        finalString = ' ' + finalString
        i = i + 1
    return finalString

def standardColumn(columnName,dataType,dummyTableName,maxStringLen,maxDataLen,colStringLen,colPrecision,colScale):
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
    if dataType == 'nvarchar':
        finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement} {subSelect} {elseSection} END as {dataType}({colStringLen}))"
    elif dataType == 'decimal':
        finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement} {subSelect} {elseSection} END as {dataType}({colPrecision},{colScale}))"
    else:
        finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement} {subSelect} {elseSection} END as {dataType})"

    #print(finalstring)
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


inputSourceIsAPI = input('Is the source an API? Yes/No: ')
if inputSourceIsAPI.lower() in ['yes','y','ye','ja','j']:
    sourceIsAPI = True
else:
    sourceIsAPI = False

dummyTableName = 'REPLACETHISWITHDUMMYVIEW'
tableName = fullTableName[fullTableName.index('.')+1:len(fullTableName)]

SQL_Server = SQLAlchClass.SQLServer(driver,server,uid,pwd,database)

columnInformation =f"""
    select
    	c.name,typ.name,c.max_length/2 as StringLen, c.precision, c.scale
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

precision = 0
scale = 0
columnStringLen = 0


for index ,row in df.iterrows():

    if sourceIsAPI == True:
        query =f"""
            SELECT
            	CASE
            	WHEN
            		0 IN
            		(
            			select
            				ISNUMERIC([{row[0]}])
            			from
            				{fullTableName}
            		)
            	THEN
            		'False'
            	ELSE
            		'True'
            	END as 'NumericCheck'
        """
        response = SQL_Server.executeCustomSelect(query)
        dfNumeric = pd.DataFrame(response)
        if dfNumeric['NumericCheck'][0] == 'True':
            query = f"""
            	SELECT
            		COUNT(*) as 'AmountOfDecimalRows'
            	FROM
            	(
            		SELECT
            			CharIndexCol
            		FROM
            		(
            			SELECT
            				CHARINDEX('.',REPLACE([{row[0]}],',','.')) as 'CharIndexCol'
            			FROM
            				{fullTableName}
            		) CheckForDecimals
            			WHERE CheckForDecimals.CharIndexCol != 0
            	) FilteredQuery
            """
            response = SQL_Server.executeCustomSelect(query)
            dfDecimal = pd.DataFrame(response)
            if dfDecimal['AmountOfDecimalRows'][0] > 0:
                dataType = 'decimal'
                precision = 9
                scale = 2
            else:
                query = f"""
                	SELECT
                    	COUNT([{row[0]}]) as 'AmountOfNot0Or1Rows'
                    FROM
                    	{fullTableName}
                    WHERE [{row[0]}] NOT IN ('0','1')
                """
                response = SQL_Server.executeCustomSelect(query)
                dfBool = pd.DataFrame(response)
                if dfBool['AmountOfNot0Or1Rows'][0] == 0:
                    dataType = 'bit'
                else:
                    dataType = 'int'
        else:
            query = f"""
                SELECT
                	MAX(LEN([{row[0]}])) as 'StringLen'
                FROM
                	{fullTableName}
            """
            response = SQL_Server.executeCustomSelect(query)
            dfStringLen = pd.DataFrame(response)
            #print(dfStringLen)
            if dfStringLen['StringLen'][0] != None:
                columnStringLen = int(dfStringLen['StringLen'][0] * 1.5)
            dataType = 'nvarchar'
    else:
        dataType = row[1]
        columnStringLen = row[2]
        precision = row[3]
        scale = row[4]

    if index == dfMaxValue:
        if dataType not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(dataType)
            createDummyTable = createDummyTable + f'Dummy{dataType} {dataType} null'

        if str(f',{index},') in setOfTrueFalseToBit:
            createViewQuery = createViewQuery + f'\n{convertTrueFalseToBit(row[0],maxDataLen)}\n'
        elif str(f',{index},') in setOfDateTimes:
            createViewQuery = createViewQuery + f'\n{convertDateTime(row[0],dummyTableName,maxDataLen)}\n'
        elif str(f',{index},') in setOfDates:
            createViewQuery = createViewQuery + f'\n{convertDate(row[0],dummyTableName,maxDataLen)}\n'
        else:
            createViewQuery = createViewQuery + f'{standardColumn(row[0],dataType,dummyTableName,maxColLen,maxDataLen,columnStringLen,precision,scale)}\n'
    else:
        if dataType not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(dataType)
            createDummyTable = createDummyTable + f'Dummy{dataType} {dataType} null,'

        if str(f',{index},') in setOfTrueFalseToBit:
            createViewQuery = createViewQuery + f'{convertTrueFalseToBit(row[0],maxDataLen)},'
        elif str(f',{index},') in setOfDateTimes:
            createViewQuery = createViewQuery + f'\n{convertDateTime(row[0],dummyTableName,maxDataLen)},'
        elif str(f',{index},') in setOfDates:
            createViewQuery = createViewQuery + f'\n{convertDate(row[0],dummyTableName,maxDataLen)},'
        else:
            createViewQuery = createViewQuery + f'{standardColumn(row[0],dataType,dummyTableName,maxColLen,maxDataLen,columnStringLen,precision,scale)},'

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
