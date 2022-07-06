
import pandas as pd
import SQLAlchClass

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

for index ,row in df.iterrows():

    if index == dfMaxValue:
        if row[1] not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(row[1])
            createDummyTable = createDummyTable + f'Dummy{row[1]} {row[1]} null'

        if row[0] == 'nvarchar':
            createViewQuery = createViewQuery + f"\n\t\t {row[0]} = CASE WHEN TRIM({row[0]}) IS NULL THEN (SELECT Dummy{row[1]} FROM {dummyTableName}) ELSE TRIM({row[0]}) END\n"
        else:
            createViewQuery = createViewQuery + f"\n\t\t {row[0]} = CASE WHEN {row[0]} IS NULL THEN (SELECT Dummy{row[1]} FROM {dummyTableName}) ELSE {row[0]} END\n"
    else:

        if row[1] not in listOfUsedDataTypes:
            listOfUsedDataTypes.append(row[1])
            createDummyTable = createDummyTable + f'Dummy{row[1]} {row[1]} null,'

        if row[0] == 'nvarchar':
            createViewQuery = createViewQuery + f"\n\t\t {row[0]} = CASE WHEN TRIM({row[0]}) IS NULL THEN (SELECT Dummy{row[1]} FROM {dummyTableName}) ELSE TRIM({row[0]}) END,"
        else:
            createViewQuery = createViewQuery + f"\n\t\t {row[0]} = CASE WHEN {row[0]} IS NULL THEN (SELECT Dummy{row[1]} FROM {dummyTableName}) ELSE {row[0]} END,"
    #print('just printed row')

createDummyTable = createDummyTable + ')'
createViewQuery = createViewQuery + f'FROM {fullTableName})'
SQL_Server.executeCustomQuery(createDummyTable)
SQL_Server.executeCustomQuery(f"DROP VIEW IF EXISTS [dbo].v{tableName}")
SQL_Server.executeCustomQuery(createViewQuery)
SQL_Server.executeCustomQuery(f"DROP TABLE dbo.{dummyTableName}")
