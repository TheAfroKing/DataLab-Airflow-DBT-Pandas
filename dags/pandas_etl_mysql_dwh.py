import pandas as pd
import pendulum
import logging
from datetime import timedelta
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task  
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.empty import EmptyOperator  
MYSQL_CONN_ID = "MySQL"
MySQLDWH_CONN_ID = "MySQLDWH"


@dag(
    dag_id="pandas_etl_mysql_dwh",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="DAG para ETL de la base de datos de prueba (MySQL) a un DWH en estrella (MySQL).",
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
    },
    tags=["dwh", "etl", "mysql", "pandas"],
)
def classic_models_etl():
    """
    ### ETL DAG
    Este DAG extrae datos de una base de datos MySQL transaccional (OLTP),
    la transforma usando Pandas a un esquema en estrella y la carga en un
    Data Warehouse (DWH) en MySQL.
    
    **Flujo de Tareas:**
    1.  **Cargar Dimensiones:** Carga todas las tablas de dimensión en paralelo.
        * `DimDate` (Generada)
        * `DimOffice`
        * `DimProduct` (Combina `products` y `productlines`)
        * `DimEmployee` (Depende de `offices`)
        * `DimCustomer` (Depende de `employees`)
    2.  **Cargar Hechos:** Una vez que todas las dimensiones están listas, carga las tablas de hechos.
        * ` `
        * `FactSales`
    """

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def load_dim_date():
        """
        Genera y carga la tabla DimDate.
        Extrae el rango de fechas de la tabla 'orders' de MySQL.
        """
        logging.info("Iniciando carga de DimDate...")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        dwh_hook = MySqlHook(mysql_conn_id=MySQLDWH_CONN_ID)

        logging.info("Extrayendo rango de fechas de MySQL...")
        sql = "SELECT MIN(orderDate), MAX(orderDate) FROM orders"
        min_date, max_date = mysql_hook.get_first(sql)
        
        logging.info(f"Rango de fechas: {min_date} a {max_date}")

        df_dates = pd.DataFrame(
            {"FullDate": pd.date_range(min_date, max_date)}
        )
        df_dates["DateKey"] = df_dates["FullDate"].dt.strftime("%Y%m%d").astype(int)
        df_dates["DayName"] = df_dates["FullDate"].dt.day_name()
        df_dates["DayOfWeek"] = df_dates["FullDate"].dt.dayofweek
        df_dates["DayOfMonth"] = df_dates["FullDate"].dt.day
        df_dates["MonthName"] = df_dates["FullDate"].dt.month_name()
        df_dates["MonthOfYear"] = df_dates["FullDate"].dt.month
        df_dates["Quarter"] = df_dates["FullDate"].dt.quarter
        df_dates["Year"] = df_dates["FullDate"].dt.year
        df_dates["IsWeekend"] = df_dates["DayOfWeek"].isin([5, 6])
        
        df_dates = df_dates[[
            "DateKey", "FullDate", "DayName", "DayOfWeek", "DayOfMonth", 
            "MonthName", "MonthOfYear", "Quarter", "Year", "IsWeekend"
        ]]

        logging.info("Truncando y cargando DimDate en MySQL DWH...")
        engine = dwh_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE DimDate; SET FOREIGN_KEY_CHECKS=1;"))
        df_dates.to_sql("DimDate", engine, if_exists="append", index=False)
        
        logging.info(f"Carga de DimDate completada. {len(df_dates)} filas insertadas.")
        
    @task
    def load_dim_office():
        """Extrae 'offices' de MySQL y carga en 'DimOffice'."""
        logging.info("Iniciando carga de DimOffice...")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        dwh_hook = MySqlHook(mysql_conn_id=MySQLDWH_CONN_ID)

        df_offices = mysql_hook.get_pandas_df(sql="SELECT * FROM offices")

        logging.info("Truncando y cargando DimOffice en MySQL DWH...")
        engine = dwh_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE DimOffice; SET FOREIGN_KEY_CHECKS=1;"))
        df_offices.to_sql("DimOffice", engine, if_exists="append", index=False)
        logging.info(f"Carga de DimOffice completada. {len(df_offices)} filas insertadas.")


    @task
    def load_dim_product():
        """Extrae 'products' y 'productlines', las une y carga en 'DimProduct'."""
        logging.info("Iniciando carga de DimProduct...")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        dwh_hook = MySqlHook(mysql_conn_id=MySQLDWH_CONN_ID)

        df_products = mysql_hook.get_pandas_df(sql="""
            SELECT productCode, productName, productLine, productScale, 
                   productVendor, productDescription, quantityInStock, buyPrice, MSRP 
            FROM products
        """)
        df_lines = mysql_hook.get_pandas_df(sql="SELECT productLine, textDescription FROM productlines")

        df_dim_product = pd.merge(df_products, df_lines, on="productLine", how="left")
        
        df_dim_product = df_dim_product[[
            "productCode", "productName", "productScale", "productVendor",
            "buyPrice", "MSRP", "productLine", "textDescription"
        ]]

        logging.info("Truncando y cargando DimProduct en MySQL DWH...")
        engine = dwh_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE DimProduct; SET FOREIGN_KEY_CHECKS=1;"))
        df_dim_product.to_sql("DimProduct", engine, if_exists="append", index=False)
        logging.info(f"Carga de DimProduct completada. {len(df_dim_product)} filas insertadas.")

    @task
    def load_dim_employee():
        """
        Extrae 'employees' y busca las OfficeKey de 'DimOffice'.
        Carga en 'DimEmployee'.
        """
        
        logging.info("Iniciando carga de DimEmployee...")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        dwh_hook = MySqlHook(mysql_conn_id=MySQLDWH_CONN_ID)
        engine = dwh_hook.get_sqlalchemy_engine()

        df_employees = mysql_hook.get_pandas_df(sql="""
            SELECT employeeNumber, lastName, firstName, extension, 
                   email, officeCode, reportsTo, jobTitle 
            FROM employees
        """)
        
        df_dim_office = pd.read_sql("SELECT OfficeKey, officeCode FROM DimOffice", engine)

        df_dim_employee = pd.merge(df_employees, df_dim_office, on="officeCode", how="left")
        df_dim_employee = df_dim_employee.rename(columns={"reportsTo": "reportsToEmployeeNumber"})
        
        df_dim_employee = df_dim_employee[[
            "employeeNumber", "lastName", "firstName", "jobTitle", 
            "email", "OfficeKey", "reportsToEmployeeNumber"
        ]]

        logging.info("Truncando y cargando DimEmployee en MySQL DWH...")
        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE DimEmployee; SET FOREIGN_KEY_CHECKS=1;"))
        df_dim_employee.to_sql("DimEmployee", engine, if_exists="append", index=False)
        logging.info(f"Carga de DimEmployee completada. {len(df_dim_employee)} filas insertadas.")

    @task
    def load_dim_customer():
        """
        Extrae 'customers' y busca las SalesRepEmployeeKey de 'DimEmployee'.
        Carga en 'DimCustomer'.
        """

        logging.info("Iniciando carga de DimCustomer...")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        dwh_hook = MySqlHook(mysql_conn_id=MySQLDWH_CONN_ID)
        engine = dwh_hook.get_sqlalchemy_engine()

        df_customers = mysql_hook.get_pandas_df(sql="SELECT * FROM customers")
        
        df_dim_employee = pd.read_sql("SELECT EmployeeKey, employeeNumber FROM DimEmployee", engine)
        
        df_dim_employee = df_dim_employee.rename(
            columns={"employeeNumber": "salesRepEmployeeNumber", "EmployeeKey": "SalesRepEmployeeKey"}
        )

        df_dim_customer = pd.merge(
            df_customers, 
            df_dim_employee, 
            on="salesRepEmployeeNumber", 
            how="left"
        )
        
        df_dim_customer = df_dim_customer[[
            "customerNumber", "customerName", "contactLastName", "contactFirstName",
            "phone", "city", "state", "country", "creditLimit", "SalesRepEmployeeKey"
        ]]

        logging.info("Truncando y cargando DimCustomer en MySQL DWH...")
        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE DimCustomer; SET FOREIGN_KEY_CHECKS=1;"))
        df_dim_customer.to_sql("DimCustomer", engine, if_exists="append", index=False)
        logging.info(f"Carga de DimCustomer completada. {len(df_dim_customer)} filas insertadas.")


    @task
    def load_fact_payments():
        """
        Extrae 'payments' y busca claves de 'DimDate' y 'DimCustomer'.
        Carga en 'FactPayments'.
        """

        logging.info("Iniciando carga de FactPayments...")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        dwh_hook = MySqlHook(mysql_conn_id=MySQLDWH_CONN_ID)
        engine = dwh_hook.get_sqlalchemy_engine()

        df_payments = mysql_hook.get_pandas_df(
            sql="SELECT customerNumber, checkNumber, paymentDate, amount FROM payments"
        )
        
        df_dim_date = pd.read_sql("SELECT DateKey, FullDate FROM DimDate", engine)
        df_dim_customer = pd.read_sql("SELECT CustomerKey, customerNumber FROM DimCustomer", engine)

        df_payments["paymentDate"] = pd.to_datetime(df_payments["paymentDate"])
        df_dim_date["FullDate"] = pd.to_datetime(df_dim_date["FullDate"])

        df_facts = pd.merge(df_payments, df_dim_customer, on="customerNumber", how="left")
        
        df_facts = pd.merge(df_facts, df_dim_date, left_on="paymentDate", right_on="FullDate", how="left")
        df_facts = df_facts.rename(columns={"DateKey": "PaymentDateKey"})
        
        df_facts = df_facts[["PaymentDateKey", "CustomerKey", "checkNumber", "amount"]]
        
        df_facts = df_facts.dropna(subset=["PaymentDateKey", "CustomerKey"])

        logging.info("Truncando y cargando FactPayments en MySQL DWH...")
        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE FactPayments; SET FOREIGN_KEY_CHECKS=1;"))
        df_facts.to_sql("FactPayments", engine, if_exists="append", index=False)
        logging.info(f"Carga de FactPayments completada. {len(df_facts)} filas insertadas.")

    @task
    def load_fact_sales():
        """
        Extrae 'orders' y 'orderdetails', busca claves de todas las dimensiones
        relevantes, calcula métricas y carga en 'FactSales'.
        """

        logging.info("Iniciando carga de FactSales...")
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        dwh_hook = MySqlHook(mysql_conn_id=MySQLDWH_CONN_ID)
        engine = dwh_hook.get_sqlalchemy_engine()

        df_orders = mysql_hook.get_pandas_df(
            sql="SELECT orderNumber, orderDate, requiredDate, shippedDate, status, customerNumber FROM orders"
        )
        df_details = mysql_hook.get_pandas_df(
            sql="SELECT orderNumber, productCode, quantityOrdered, priceEach, orderLineNumber FROM orderdetails"
        )
        
        df_dim_date = pd.read_sql("SELECT DateKey, FullDate FROM DimDate", engine)
        df_dim_product = pd.read_sql("SELECT ProductKey, productCode, buyPrice FROM DimProduct", engine)
        df_dim_customer = pd.read_sql("SELECT CustomerKey, customerNumber, SalesRepEmployeeKey FROM DimCustomer", engine)
        df_dim_employee = pd.read_sql("SELECT EmployeeKey, OfficeKey FROM DimEmployee", engine)
        
        logging.info("Transformando y uniendo datos para FactSales...")
        df_facts = pd.merge(df_orders, df_details, on="orderNumber")

        df_facts = pd.merge(df_facts, df_dim_product, on="productCode", how="left")

        df_facts = pd.merge(df_facts, df_dim_customer, on="customerNumber", how="left")
        
        df_facts = pd.merge(
            df_facts, 
            df_dim_employee, 
            left_on="SalesRepEmployeeKey", 
            right_on="EmployeeKey", 
            how="left",
            suffixes=('_cust', '_emp')
        )
        df_facts = df_facts.rename(columns={"EmployeeKey_emp": "EmployeeKey"})

        df_dim_date["FullDate"] = pd.to_datetime(df_dim_date["FullDate"])
        df_facts["orderDate"] = pd.to_datetime(df_facts["orderDate"])
        df_facts["shippedDate"] = pd.to_datetime(df_facts["shippedDate"])

        df_facts = pd.merge(
            df_facts, 
            df_dim_date, 
            left_on="orderDate", 
            right_on="FullDate", 
            how="left"
        )
        df_facts = df_facts.rename(columns={"DateKey": "OrderDateKey"})
        
        df_facts = pd.merge(
            df_facts, 
            df_dim_date, 
            left_on="shippedDate", 
            right_on="FullDate", 
            how="left", 
            suffixes=("_order", "_shipped")
        )
        df_facts = df_facts.rename(columns={"DateKey": "ShippedDateKey"})
        
        df_facts["totalLineAmount"] = df_facts["quantityOrdered"] * df_facts["priceEach"]
        df_facts["profit"] = (df_facts["priceEach"] - df_facts["buyPrice"]) * df_facts["quantityOrdered"]
        
        df_facts = df_facts.rename(columns={
            "status": "orderStatus"
        })
        
        final_cols = [
            "OrderDateKey", "ShippedDateKey", "ProductKey", "CustomerKey", 
            "EmployeeKey", "OfficeKey", "orderNumber", "orderLineNumber", 
            "orderStatus", "quantityOrdered", "priceEach", "totalLineAmount", "profit"
        ]
        df_final_facts = df_facts[final_cols]
        
        df_final_facts = df_final_facts.dropna(
            subset=["OrderDateKey", "ProductKey", "CustomerKey"]
        )

        logging.info("Truncando y cargando FactSales en MySQL DWH...")
        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE FactSales; SET FOREIGN_KEY_CHECKS=1;"))
        df_final_facts.to_sql("FactSales", engine, if_exists="append", index=False)
        logging.info(f"Carga de FactSales completada. {len(df_final_facts)} filas insertadas.")
    


    task_load_dim_date = load_dim_date()
    task_load_dim_office = load_dim_office()
    task_load_dim_product = load_dim_product()
    task_load_dim_employee = load_dim_employee()
    task_load_dim_customer = load_dim_customer()
    task_load_fact_payments = load_fact_payments()
    task_load_fact_sales = load_fact_sales()

    
    start >> [task_load_dim_date, task_load_dim_office, task_load_dim_product]
    
    task_load_dim_office >> task_load_dim_employee
    task_load_dim_employee >> task_load_dim_customer
    
    [task_load_dim_date, task_load_dim_customer] >> task_load_fact_payments
    
    [task_load_dim_date, task_load_dim_customer, task_load_dim_product] >> task_load_fact_sales
    
    [task_load_fact_payments, task_load_fact_sales] >> end

classic_models_etl()