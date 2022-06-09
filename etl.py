
import os
from sqlalchemy import create_engine
import pandas
import pyodbc

# Extract data from SQL Server
def extract():
    try:
        conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        "Server=L451\EASYTEST;"
        "Database=AdventureWorks2019;"
        "Trusted_Connection=yes;"
        )

        cursor = conn.cursor()
        cursor.execute("""
        select t.name as table_name
        from sys.tables t 
        where t.name in ('EmployeeDepartmentHistory', 'Department', 'Employee','Shift','JobCandidate','EmployeePayHistory')
        """)
        tables = cursor.fetchall()
        for table in tables:
            if table[0] == 'Employee':
                df = pandas.read_sql_query(f"select BusinessEntityID , NationalIDNumber , JobTitle , BirthDate , MaritalStatus , Gender ,HireDate , SalariedFlag from [HumanResources].{table[0]}", conn)
            else:
                df = pandas.read_sql_query(f"select * from [HumanResources].{table[0]}", conn)
            load(df, table[0])
    except Exception as e:
        print(e)
    finally:
        conn.close()
# Load data into MySql
def load(df, table):
    try:
        rows_imported = 0
        engine = create_engine(f'mysql://root:Monye.ngene19@localhost:3306/hr_department')
        # print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {table}')
        # save data to mysql
        df.to_sql(table, con=engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print(f'Data imported for table {table} is successful')
    except Exception as e:
        print(e)
    finally:
        engine.dispose()


try:
    extract()
except Exception as e:
    print(e)
