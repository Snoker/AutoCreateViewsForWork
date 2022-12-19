
import pandas as pd
import SQLAlchClass

#####################################################################
#
#       Set initial variables. This must be done else the script can not run.
#
#####################################################################
database = input('Please provide the database name: ')
sourceSchema = input('Please provide the source schema. ( Not including brackets ): ')
targetSchema = input('Please provide the target schema. ( Not including brackets ): ')
driver='SQL Server Native Client 11.0'
server='localhost'
#instance='mssqlserver01'
uid='sqluser'
pwd='sqluser'
#database='HampusLek'


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

def standardColumn(columnName,dataType,dummyView,maxStringLen,maxDataLen,colStringLen,colPrecision,colScale):
    #First instance of column name used, before =
    i = 0
    columnName = f'[{columnName}]'
    firstColumnName = columnName
    maxStringLen = maxStringLen - len(columnName)
    firstColumnName = addSpacingEnd(columnName,columnName,maxDataLen)

    #Building the first section of the case statement, isnull part before subselect
    InitialPartCaseStatement = columnName
    if dataType == 'nvarchar' or dataType == 'varchar':
        InitialPartCaseStatement = f" ISNULL( NULLIF( TRIM( {InitialPartCaseStatement} ), '') "
    else:
        InitialPartCaseStatement = f" ISNULL( {InitialPartCaseStatement}  "
    #InitialPartCaseStatement = addSpacingEnd(InitialPartCaseStatement,'',maxStringLen)

    #Building the subselect section of the query.
    subSelect = f'(SELECT Dummy{dataType} FROM {dummyView}))'
    #subSelect = addSpacingEnd(subSelect,dataType,maxDataLen)

    #Building the else section of the query.
    if dataType == 'nvarchar' or dataType == 'varchar':
        elseSection = f'ELSE {columnName}'
    else:
        elseSection = f'ELSE {columnName}      '
    #elseSection = addSpacingEnd(elseSection,'',maxStringLen)

    # #Return final query string
    # if dataType == 'nvarchar':
    #     finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement} {subSelect} {elseSection} END as {dataType}({colStringLen}))"
    # elif dataType == 'decimal':
    #     finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement} {subSelect} {elseSection} END as {dataType}({colPrecision},{colScale}))"
    # else:
    #     finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement} {subSelect} {elseSection} END as {dataType})"

    #Return final query string
    if dataType == 'nvarchar' or dataType == 'varchar':
        finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement}, {subSelect} as {dataType}({colStringLen}))"
    elif dataType == 'decimal':
        finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement}, {subSelect} as {dataType}({colPrecision},{colScale}))"
    else:
        finalstring = f"\n\t\t {firstColumnName} = CAST( {InitialPartCaseStatement}, {subSelect} as {dataType})"

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

def convertDateTime(columnName,dummyView,maxDataLen):
    columnName = f'{columnName}'
    firstColumnName = addSpacingEnd(columnName,columnName,maxDataLen)
    finalString = f"{firstColumnName} = CAST( CASE WHEN TRIM([{columnName}]) IS NULL THEN (SELECT DummyDateTime FROM {dummyView}) ELSE {columnName} END as DateTime)"
    return(finalString)

def convertDate(columnName,dummyView,maxDataLen):
    columnName = f'{columnName}'
    firstColumnName = addSpacingEnd(columnName,columnName,maxDataLen)
    finalString = f"{firstColumnName} = CAST( CASE WHEN TRIM([{columnName}]) IS NULL THEN (SELECT DummyDate FROM {dummyView}) ELSE {columnName} END as Date)"
    return(finalString)

#DB object.
SQL_Server = SQLAlchClass.SQLServer(driver,server,uid,pwd,database)


#"Global" variables
dummyView = f'{sourceSchema}.vDummyValues'
listOfUsedDataTypes = []

checkForView = f"""
select
       DISTINCT type_name(user_type_id) as data_type
from sys.columns c
join sys.views v
     on v.object_id = c.object_id
INNER JOIN sys.schemas s
	ON v.schema_id = s.schema_id
WHERE s.name + '.' + v.name = '{sourceSchema}.vDummyValues'
"""
response = SQL_Server.executeCustomSelect(checkForView)
df_viewCheck = pd.DataFrame(response)

if not df_viewCheck.empty:
    maxIndex = len(df_viewCheck)
    for index, row in df_viewCheck.iterrows():
        dataType = row[0]
        if index == maxIndex:
            if dataType not in listOfUsedDataTypes:
                listOfUsedDataTypes.append(dataType)
        else:
            if dataType not in listOfUsedDataTypes:
                listOfUsedDataTypes.append(dataType)

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

    #print('entering itterator')

    table = schemaRow[0]
    #print(table)
    #print(f'Creating view for {targetSchema}.{table}. ')
    tableViewName = table.replace('-','')
    tableViewName = tableViewName.replace(' ','')
    tableViewName = tableViewName.replace('_','')
    tableViewName = tableViewName.replace('$','')
    tableViewName = f'v{tableViewName}'
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
        WHERE s.name + '.' + t.name= '{sourceSchema}.{table}'
        AND typ.name != 'sysname'
    """
    #print(columnInformation)
    response = SQL_Server.executeCustomSelect(columnInformation)
    df = pd.DataFrame(response)

    if table[-1] == "s":
        createViewQuery = f"""
        CREATE VIEW {targetSchema}.{tableViewName[0:len(tableViewName)-1]} AS (
            SELECT
        """
    else:
         createViewQuery = f"""
        CREATE VIEW {targetSchema}.{tableViewName} AS (
            SELECT
        """       

    table = f'[{table}]'
    #print(table)
    dfMaxValue = len(df.index) -1

    maxColLen = 0
    for index ,row in df.iterrows():
        if len(row[0]) > maxColLen:
            maxColLen = len(row[0])
    maxColLen = maxColLen + 10

    maxDataLen = 0
    for index ,row in df.iterrows():
        if len(row[0]) > maxDataLen:
            maxDataLen = len(row[0])
    maxDataLen = maxDataLen

    precision = 0
    scale = 0
    columnStringLen = 0

    for index ,row in df.iterrows():
        if row[1] == "nvarchar" or row[1] == "varchar":
            query =f"""
                SELECT
                    CASE
                    WHEN
                        0 IN
                        (
                            select
                                ISNUMERIC([{row[0]}])
                            from
                                {sourceSchema}.{table}
                            WHERE
                                [{row[0]}] IS NOT NULL
                        )
                    THEN
                        'False'
                    ELSE
                        'True'
                    END as 'NumericCheck'
            """
            response = SQL_Server.executeCustomSelect(query)
            dfNumeric = pd.DataFrame(response)
            #print(f'Numeric check DF for {row[0]} in {sourceSchema}.{table}:\n {dfNumeric}\n Query for this column was: \n {query}')
            if dfNumeric["NumericCheck"][0] == 'True':
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
                                {sourceSchema}.{table}
                        ) CheckForDecimals
                            WHERE CheckForDecimals.CharIndexCol != 0
                    ) FilteredQuery
                """
                response = SQL_Server.executeCustomSelect(query)
                dfDecimal = pd.DataFrame(response)
                if dfDecimal["AmountOfDecimalRows"][0] > 0:
                    dataType = 'decimal'
                    precision = 9
                    scale = 2
                else:
                    query = f"""
                        SELECT
                            COUNT([{row[0]}]) as 'AmountOfNot0Or1Rows'
                        FROM
                            {sourceSchema}.{table}
                        WHERE [{row[0]}] NOT IN ('0','1')
                    """
                    response = SQL_Server.executeCustomSelect(query)
                    dfBool = pd.DataFrame(response)
                    if dfBool["AmountOfNot0Or1Rows"][0] == 0:
                        dataType = 'bit'
                    else:
                        dataType = 'int'
            else:
                
                query = f"""
                    SELECT
                        MAX(LEN([{row[0]}])) as 'StringLen'
                    FROM
                        {sourceSchema}.{table}
                """
                response = SQL_Server.executeCustomSelect(query)
                dfStringLen = pd.DataFrame(response)
                #print(f'Strin len DF for {row[0]} in {sourceSchema}.{table}:\n {dfStringLen}\n Query for this column was: \n {query}')
                if dfStringLen["StringLen"][0] != None:
                    #Check for DateTime
                    query = f"""
                            SELECT
                            [ColumnIsDateTime] = IIF(LookForDates.[AmountOfDates] = AmountOfRowsInTable.[AmountOfRows],'True','False')
                            FROM
                            (
                                SELECT
                                    COUNT(*) as 'AmountOfDates'
                                    FROM 
                                    (
                                    select
                                    [IfDateTime] = 
                                        CASE WHEN 
                                                (LEN(TRIM([{row[0]}])) = 23 OR LEN(TRIM([{row[0]}])) = 19) AND 
                                                CHARINDEX('-',TRIM([{row[0]}])) = 5 AND
                                                CHARINDEX( '-', TRIM([{row[0]}]), CHARINDEX('-',TRIM([{row[0]}])) + 1 ) = 8 AND
                                                CHARINDEX(':',TRIM([{row[0]}])) = 14 AND
                                                CHARINDEX( ':', TRIM([{row[0]}]), CHARINDEX(':',TRIM([{row[0]}])) + 1 ) =  17
                                            THEN 1
                                            ELSE 0
                                            END
                                        FROM {sourceSchema}.{table}
                                    ) CheckForDate
                                    WHERE CheckForDate.[IfDateTime] = 1
                                ) LookForDates        
                                LEFT JOIN
                                (
                            SELECT COUNT(*) as 'AmountofRows' FROM {sourceSchema}.{table}
                                ) AmountOfRowsInTable ON 1 = 1
                    """
                    response = SQL_Server.executeCustomSelect(query)
                    dfCheckDateTime = pd.DataFrame(response)
                    #print(f'Datetime check DF for {row[0]} in {sourceSchema}.{table}:\n {dfCheckDateTime}\n Query for this column was: \n {query}')
                    if dfCheckDateTime["ColumnIsDateTime"][0] == 'True':
                        dataType = 'datetime'
                    else:
                        #Check for Date.
                        query = f"""
                                SELECT
                                    [ColumnIsDate] = IIF(LookForDates.[AmountOfDates] = AmountOfRowsInTable.[AmountOfRows],'True','False')
                                    FROM
                                    (
                                        SELECT
                                            COUNT(*) as 'AmountOfDates'
                                            FROM 
                                            (
                                            select
                                            [IfDate] = 
                                                CASE WHEN 
                                                        LEN(TRIM([{row[0]}])) = 10 AND 
                                                        CHARINDEX('-',TRIM([{row[0]}])) = 5 AND
                                                        CHARINDEX( '-', TRIM([{row[0]}]), CHARINDEX('-',TRIM([{row[0]}])) + 1 ) = 8
                                                    THEN 1
                                                    ELSE 0
                                                    END
                                                FROM {sourceSchema}.{table}
                                            ) CheckForDate
                                            WHERE CheckForDate.[IfDate] = 1
                                        ) LookForDates
                                        
                                        LEFT JOIN
                                        (
                                    SELECT COUNT(*) as 'AmountofRows' FROM {sourceSchema}.{table}
                                        ) AmountOfRowsInTable ON 1 = 1
                        """
                        response = SQL_Server.executeCustomSelect(query)
                        dfCheckDate = pd.DataFrame(response)
                        #print(f'Date check DF for {row[0]} in {sourceSchema}.{table}:\n {dfCheckDate}\n Query for this column was: \n {query}')
                        if dfCheckDate["ColumnIsDate"][0] == 'True':
                            dataType = 'date'
                        else:
                            columnStringLen = int(dfStringLen["StringLen"][0] + 3 * 1.5)
                            if columnStringLen >= 4000:
                                columnStringLen = 4000
                            dataType = 'nvarchar'
        else:
            columnStringLen = row[2]
            dataType = row[1]
            precision = row[3]
            scale = row[4]

        # print(f"""
        #    Table: {table},
        #    Index: {index},
        #    dfMaxValue: {dfMaxValue},
        #    dataType: {dataType},
        #    listOfUsedDataTypes: {listOfUsedDataTypes},
        #    columnStringLen: {columnStringLen},
        #    dataType: {dataType},
        #    columnName: {row[0]}
        # """)
        
           #ViewLast10Char: '{createDummyView[-11:]}'

        if index == dfMaxValue:
            if dataType not in listOfUsedDataTypes:
                listOfUsedDataTypes.append(dataType)
            createViewQuery = createViewQuery + f'{standardColumn(row[0],dataType,dummyView,maxColLen,maxDataLen,columnStringLen,precision,scale)}\n'
        else:
            if dataType not in listOfUsedDataTypes:
                listOfUsedDataTypes.append(dataType)
            createViewQuery = createViewQuery + f'{standardColumn(row[0],dataType,dummyView,maxColLen,maxDataLen,columnStringLen,precision,scale)},'

    amountOfDataType = len(listOfUsedDataTypes)
    k = 1

    createDummyView =f"""
        CREATE OR ALTER VIEW {dummyView} AS
        (
            SELECT
        """
    for viewDataType in listOfUsedDataTypes:
        if k == amountOfDataType:
            createDummyView = createDummyView + f"Dummy{viewDataType} =  CAST('{viewDataType}' as {viewDataType}))"
        else:
            k = k + 1
            createDummyView = createDummyView + f"Dummy{viewDataType} =  CAST('{viewDataType}' as {viewDataType}),\n"

    createViewQuery = createViewQuery + f'FROM {sourceSchema}.{table})'

    #print(f"The following query has been written to the database: {createViewQuery}")

    SQL_Server.executeCustomQuery(f'DROP VIEW IF EXISTS {sourceSchema}.vDummyValues')
    SQL_Server.executeCustomQuery(createDummyView)
    SQL_Server.executeCustomQuery(f"DROP VIEW IF EXISTS {targetSchema}.{tableViewName}")
    SQL_Server.executeCustomQuery(createViewQuery)

    #print(f'Done creating view for {targetSchema}.{table}. ')
