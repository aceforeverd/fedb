# Python SDK

The default execution mode is Online.

## Python SDK Installation

Execute the following command to install Python SDK:

```bash
pip install openmldb
```

## OpenMLDB DBAPI

This section demonstrates the basic use of the OpenMLDB DB API. For all DBAPI interfaces, if an execution fails, it will raise a `DatabaseError` exception. Users can catch this exception and handle it as needed. The return value is a `Cursor`. For DDL SQL, you do not need to handle the return value. For other SQL statements, you can refer to the specific examples below for how to handle the return value.

### Create Connection

Parameter `db_name` name must exist, and the database must be created before the connection is created. To continue, create a connection without a database and then use the database db through the `execute ("USE<db>")` command.

```python
import openmldb.dbapi
db = openmldb.dbapi.connect(zk="$zkcluster", zkPath="$zkpath")
cursor = db.cursor()
```

#### Configuration Details

Zk and zkPath configurations are required.

The Python SDK can be used through OpenMLDB DBAPI/SQLAlchemy. The optional configurations are basically the same as those of the Java client. Please refer to the [Java SDK configuration](./java_sdk.md#sdk-configuration-details) for details.

### Create Database

Create database `db1`:

```python
cursor.execute("CREATE DATABASE db1")
cursor.execute("USE db1")
```

### Create Table

Create table `t1`:

```python
cursor.execute("CREATE TABLE t1 (col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1))")
```

### Insert Data into Table

Insert one sentence of data into the table:

```python
cursor.execute("INSERT INTO t1 VALUES(1000, '2020-12-25', 'guangdon', 'shenzhen', 1)")
```

### Execute SQL Query

```python
result = cursor.execute("SELECT * FROM t1")
print(result.fetchone())
print(result.fetchmany(10))
print(result.fetchall())
```

### SQL Batch Query

```python
#In the Batch Request mode, the input parameters of the interface are "SQL", "Common_Columns", "Request_Columns"
result = cursor.batch_row_request("SELECT * FROM t1", ["col1","col2"], ({"col1": 2000, "col2": '2020-12-22', "col3": 'fujian', "col4":'xiamen', "col5": 2}))
print(result.fetchone())
```
### Execute Deployment

Please note that the execution of deployments is only supported by DBAPI, and there is no equivalent interface in OpenMLDB SQLAlchemy. Additionally, deployment execution supports single requests only and does not support batch requests.

```python
cursor.execute("DEPLOY d1 SELECT col1 FROM t1")
# dict style
result = cursor.callproc("d1", {"col1": 1000, "col2": None, "col3": None, "col4": None, "col5": None})
print(result.fetchall())
# tuple style
result = cursor.callproc("d1", (1001, "2023-07-20", "abc", "def", 1))
print(result.fetchall())
# drop deployment before drop table
cursor.execute("DROP DEPLOYMENT d1")
```

### Delete Table

Delete table `t1`:

```python
cursor.execute("DROP TABLE t1")
```

### Delete Database

Delete database `db1`:

```python
cursor.execute("DROP DATABASE db1")
```

### Close Connection

```python
cursor.close()
```

## OpenMLDB SQLAlchemy

This section demonstrates the use of the Python SDK through OpenMLDB SQLAlchemy. Similarly, if any of the DBAPI interfaces fail, they will raise a `DatabaseError` exception. Users can catch and handle this exception as needed. The handling of return values should follow the SQLAlchemy standard.

### Create Connection

```python
create_engine('openmldb:///db_name?zk=zkcluster&zkPath=zkpath')
```

Parameter `db_name` must exist, and the database must be created before the connection is created. First, create a connection without a database, and then use the database `db` through the `execute ("USE<db>")` command.

```python
import sqlalchemy as db
engine = db.create_engine('openmldb:///?zk=127.0.0.1:2181&zkPath=/openmldb')
connection = engine.connect()
```

### Create Database

Use the `connection.execute()` interface to create database `db1`:

```python
try:
    connection.execute("CREATE DATABASE db1")
except Exception as e:
    print(e)

connection.execute("USE db1")
```

### Create Table

Use the `connection.execute()` interface to create table `t1`:

```python
try:
    connection.execute("CREATE TABLE t1 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1))")
except Exception as e:
    print(e)
```

### Insert Data into Table

Use the `connection.execute (ddl)` interface to execute the SQL insert statement, and you can insert data into the table:

```python
try:
    connection.execute("INSERT INTO t1 VALUES(1000, '2020-12-25', 'guangdon', 'shenzhen', 1);")
except Exception as e:
    print(e)
```

Use the `connection.execute (ddl, data)` interface to execute the insert statement of SQL with placeholder. You can specify the insert data dynamically or insert multiple rows:

```python
try:
    insert = "INSERT INTO t1 VALUES(1002, '2020-12-27', ?, ?, 3);"
    connection.execute(insert, ({"col3":"fujian", "col4":"fuzhou"}))
    connection.execute(insert, [{"col3":"jiangsu", "col4":"nanjing"}, {"col3":"zhejiang", "col4":"hangzhou"}])
except Exception as e:
    print(e)
```

### Execute SQL Batch Query

Use the `connection.execute (sql)` interface to execute SQL batch query statements:

```python
try:
    rs = connection.execute("SELECT * FROM t1")
    for row in rs:
        print(row)
    rs = connection.execute("SELECT * FROM t1 WHERE col3 = ?;", ('hefei'))
    rs = connection.execute("SELECT * FROM t1 WHERE col3 = ?;",[('hefei'), ('shanghai')])
except Exception as e:
    print(e)
```

### Execute SQL Query

Use the `connection.execute (sql, request)` interface to execute the SQL request query. You can put the input data into the second parameter of the execute function:

```python
try:
    rs = connection.execute("SELECT * FROM t1", ({"col1":9999, "col2":'2020-12-27', "col3":'zhejiang', "col4":'hangzhou', "col5":100}))
except Exception as e:
    print(e)
```

### Delete Table

Use the `connection.execute (ddl)` interface to delete table `t1`:

```python
try:
    connection.execute("DROP TABLE t1")
except Exception as e:
    print(e)
```

### Delete Database

Use the connection.execute（ddl）interface to delete database `db1`:

```python
try:
    connection.execute("DROP DATABASE db1")
except Exception as e:
    print(e)
```

## Notebook Magic Function

The OpenMLDB Python SDK supports the expansion of Notebook magic function. Use the following statement to register the function.

```python
import openmldb
db = openmldb.dbapi.connect(database='demo_db',zk='0.0.0.0:2181',zkPath='/openmldb')
openmldb.sql_magic.register(db)
```

Then you can use line magic function `%sql` and block magic function `%%sql` in Notebook.

![img](../images/openmldb_magic_function.png)

## A Complete Example

Refer to the [Python quickstart demo](https://github.com/4paradigm/OpenMLDB/tree/main/demo/python_quickstart/demo.py), which includes the above DBAPI and SQLAlchemy usage.

## Q&A

- **What do I do when error `ImportError：dlopen (.. _sql_router_sdk. so, 2): initializer function 0xnnnn not in mapped image for` appears when using SQLAlchemy?**

In addition to importing OpenMLDB, you may also have imported other third-party libraries, which may cause confusion in the loading order. Due to the complexity of the system, you can try to use the virtual env environment (such as conda) to avoid interference. In addition, import OpenMLDB before importing SQLAlchemy, and ensure that the two imports are in the first place.

If the error still occurs, it is recommended to connect to OpenMLDB by request http to connect to apiserver.

- **What do I do if Python SDK encounters the following problems?**

```plain
[libprotobuf FATAL /Users/runner/work/crossbow/crossbow/vcpkg/buildtrees/protobuf/src/23fa7edd52-3ba2225d30.clean/src/google/protobuf/stubs/common.cc:87] This program was compiled against version 3.6.1 of the Protocol Buffer runtime library, which is not compatible with the installed version (3.15.8).  Contact the program author for an update. ...
```

This problem may be due to the import of other versions of protobuf from other libraries. You can try to use the virtual env environment (such as conda).
