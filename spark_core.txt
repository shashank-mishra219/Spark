# Execute below commands after opening PySpark Shell 
# Execute each command one by one

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

person_list = [("Berry","","Allen",1,"M"),
        ("Oliver","Queen","",2,"M"),
        ("Robert","","Williams",3,"M"),
        ("Tony","","Stark",4,"F"),
        ("Rajiv","Mary","Kumar",5,"F")
]

schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", IntegerType(), True), \
        StructField("gender", StringType(), True), \    
 ])
 
 df = spark.createDataFrame(data=person_list,schema=schema)
 
 df.show(truncate=False)
 
 df.printSchema()
 
 
 # Read data from HDFS path
 
df1 = spark.read.option("header",True).csv("/input_data/departments.csv")
df1.printSchema()

#Output
root
 |-- DEPARTMENT_ID: string (nullable = true)
 |-- DEPARTMENT_NAME: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- LOCATION_ID: string (nullable = true)
 
 
df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/departments.csv")
df2.printSchema()

#Output
root
 |-- DEPARTMENT_ID: integer (nullable = true)
 |-- DEPARTMENT_NAME: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- LOCATION_ID: integer (nullable = true)
 
 
 df2.show()
 
 #Output
+-------------+--------------------+----------+-----------+
|DEPARTMENT_ID|     DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-------------+--------------------+----------+-----------+
|           10|      Administration|       200|       1700|
|           20|           Marketing|       201|       1800|
|           30|          Purchasing|       114|       1700|
|           40|     Human Resources|       203|       2400|
|           50|            Shipping|       121|       1500|
|           60|                  IT|       103|       1400|
|           70|    Public Relations|       204|       2700|
|           80|               Sales|       145|       2500|
|           90|           Executive|       100|       1700|
|          100|             Finance|       108|       1700|
|          110|          Accounting|       205|       1700|
|          120|            Treasury|        - |       1700|
|          130|       Corporate Tax|        - |       1700|
|          140|  Control And Credit|        - |       1700|
|          150|Shareholder Services|        - |       1700|
|          160|            Benefits|        - |       1700|
|          170|       Manufacturing|        - |       1700|
|          180|        Construction|        - |       1700|
|          190|         Contracting|        - |       1700|
|          200|          Operations|        - |       1700|
+-------------+--------------------+----------+-----------+

>>> empDf = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/employees.csv")
>>> empDf.printSchema()
root
 |-- EMPLOYEE_ID: integer (nullable = true)
 |-- FIRST_NAME: string (nullable = true)
 |-- LAST_NAME: string (nullable = true)
 |-- EMAIL: string (nullable = true)
 |-- PHONE_NUMBER: string (nullable = true)
 |-- HIRE_DATE: string (nullable = true)
 |-- JOB_ID: string (nullable = true)
 |-- SALARY: integer (nullable = true)
 |-- COMMISSION_PCT: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- DEPARTMENT_ID: integer (nullable = true)

>>> empDf.show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+


 >>> empDf.select("*").show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
only showing top 20 rows

>>> empDf.select("EMPLOYEE_ID","FIRST_NAME").show()
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows

>>> empDf.select(empDf.EMPLOYEE_ID,empDf.FIRST_NAME).show()
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows

>>> empDf.select(empDf["EMPLOYEE_ID"],empDf["FIRST_NAME"]).show()
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows

>>> from pyspark.sql.functions import col
>>> empDf.select(col("EMPLOYEE_ID"),col("FIRST_NAME")).show()
+-----------+----------+
|EMPLOYEE_ID|FIRST_NAME|
+-----------+----------+
|        198|    Donald|
|        199|   Douglas|
|        200|  Jennifer|
|        201|   Michael|
|        202|       Pat|
|        203|     Susan|
|        204|   Hermann|
|        205|   Shelley|
|        206|   William|
|        100|    Steven|
|        101|     Neena|
|        102|       Lex|
|        103| Alexander|
|        104|     Bruce|
|        105|     David|
|        106|     Valli|
|        107|     Diana|
|        108|     Nancy|
|        109|    Daniel|
|        110|      John|
+-----------+----------+
only showing top 20 rows

>>> empDf.select(col("EMPLOYEE_ID").alias("EMP_ID"),col("FIRST_NAME").alias("F_NAME")).show()
+------+---------+
|EMP_ID|   F_NAME|
+------+---------+
|   198|   Donald|
|   199|  Douglas|
|   200| Jennifer|
|   201|  Michael|
|   202|      Pat|
|   203|    Susan|
|   204|  Hermann|
|   205|  Shelley|
|   206|  William|
|   100|   Steven|
|   101|    Neena|
|   102|      Lex|
|   103|Alexander|
|   104|    Bruce|
|   105|    David|
|   106|    Valli|
|   107|    Diana|
|   108|    Nancy|
|   109|   Daniel|
|   110|     John|
+------+---------+
only showing top 20 rows

>>> empDf.select("EMPLOYEE_ID","FIRST_NAME","SALARY").withColumn("NEW_SALARY",col("SALARY") + 1000).show()
+-----------+----------+------+----------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|NEW_SALARY|
+-----------+----------+------+----------+
|        198|    Donald|  2600|      3600|
|        199|   Douglas|  2600|      3600|
|        200|  Jennifer|  4400|      5400|
|        201|   Michael| 13000|     14000|
|        202|       Pat|  6000|      7000|
|        203|     Susan|  6500|      7500|
|        204|   Hermann| 10000|     11000|
|        205|   Shelley| 12008|     13008|
|        206|   William|  8300|      9300|
|        100|    Steven| 24000|     25000|
|        101|     Neena| 17000|     18000|
|        102|       Lex| 17000|     18000|
|        103| Alexander|  9000|     10000|
|        104|     Bruce|  6000|      7000|
|        105|     David|  4800|      5800|
|        106|     Valli|  4800|      5800|
|        107|     Diana|  4200|      5200|
|        108|     Nancy| 12008|     13008|
|        109|    Daniel|  9000|     10000|
|        110|      John|  8200|      9200|
+-----------+----------+------+----------+
only showing top 20 rows

>>> empDf.withColumn("NEW_SALARY",col("SALARY") + 1000).select("EMPLOYEE_ID","FIRST_NAME","NEW_SALARY").show()
+-----------+----------+----------+
|EMPLOYEE_ID|FIRST_NAME|NEW_SALARY|
+-----------+----------+----------+
|        198|    Donald|      3600|
|        199|   Douglas|      3600|
|        200|  Jennifer|      5400|
|        201|   Michael|     14000|
|        202|       Pat|      7000|
|        203|     Susan|      7500|
|        204|   Hermann|     11000|
|        205|   Shelley|     13008|
|        206|   William|      9300|
|        100|    Steven|     25000|
|        101|     Neena|     18000|
|        102|       Lex|     18000|
|        103| Alexander|     10000|
|        104|     Bruce|      7000|
|        105|     David|      5800|
|        106|     Valli|      5800|
|        107|     Diana|      5200|
|        108|     Nancy|     13008|
|        109|    Daniel|     10000|
|        110|      John|      9200|
+-----------+----------+----------+
only showing top 20 rows

# To update existing column 
>>> empDf.withColumn("SALARY",col("SALARY") - 1000).select("EMPLOYEE_ID","FIRST_NAME","SALARY").show()
+-----------+----------+------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|
+-----------+----------+------+
|        198|    Donald|  1600|
|        199|   Douglas|  1600|
|        200|  Jennifer|  3400|
|        201|   Michael| 12000|
|        202|       Pat|  5000|
|        203|     Susan|  5500|
|        204|   Hermann|  9000|
|        205|   Shelley| 11008|
|        206|   William|  7300|
|        100|    Steven| 23000|
|        101|     Neena| 16000|
|        102|       Lex| 16000|
|        103| Alexander|  8000|
|        104|     Bruce|  5000|
|        105|     David|  3800|
|        106|     Valli|  3800|
|        107|     Diana|  3200|
|        108|     Nancy| 11008|
|        109|    Daniel|  8000|
|        110|      John|  7200|
+-----------+----------+------+
only showing top 20 rows


>>> empDf.withColumnRenamed("SALARY","EMP_SALARY").show()
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|EMP_SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|      2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|      2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|      4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN|     13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|      6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|      6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP|     10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR|     12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|      8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES|     24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP|     17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP|     17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|      9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|      6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|      4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|      4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|      4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR|     12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|      9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|      8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+----------+--------------+----------+-------------+
only showing top 20 rows

>>> empDf.drop("COMMISSION_PCT").show()
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+----------+-------------+
only showing top 20 rows

>>> empDf.filter(col("SALARY") < 5000).show()
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|  JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|        198|    Donald|   OConnell|DOCONNEL|650.507.9833|21-JUN-07|SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|      Grant|  DGRANT|650.507.9844|13-JAN-08|SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|     Whalen| JWHALEN|515.123.4444|17-SEP-03| AD_ASST|  4400|            - |       101|           10|
|        105|     David|     Austin| DAUSTIN|590.423.4569|25-JUN-05| IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|  Pataballa|VPATABAL|590.423.4560|05-FEB-06| IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|    Lorentz|DLORENTZ|590.423.5567|07-FEB-07| IT_PROG|  4200|            - |       103|           60|
|        115| Alexander|       Khoo|   AKHOO|515.127.4562|18-MAY-03|PU_CLERK|  3100|            - |       114|           30|
|        116|    Shelli|      Baida|  SBAIDA|515.127.4563|24-DEC-05|PU_CLERK|  2900|            - |       114|           30|
|        117|     Sigal|     Tobias| STOBIAS|515.127.4564|24-JUL-05|PU_CLERK|  2800|            - |       114|           30|
|        118|       Guy|     Himuro| GHIMURO|515.127.4565|15-NOV-06|PU_CLERK|  2600|            - |       114|           30|
|        119|     Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|PU_CLERK|  2500|            - |       114|           30|
|        125|     Julia|      Nayer|  JNAYER|650.124.1214|16-JUL-05|ST_CLERK|  3200|            - |       120|           50|
|        126|     Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|ST_CLERK|  2700|            - |       120|           50|
|        127|     James|     Landry| JLANDRY|650.124.1334|14-JAN-07|ST_CLERK|  2400|            - |       120|           50|
|        128|    Steven|     Markle| SMARKLE|650.124.1434|08-MAR-08|ST_CLERK|  2200|            - |       120|           50|
|        129|     Laura|     Bissot| LBISSOT|650.124.5234|20-AUG-05|ST_CLERK|  3300|            - |       121|           50|
|        130|     Mozhe|   Atkinson|MATKINSO|650.124.6234|30-OCT-05|ST_CLERK|  2800|            - |       121|           50|
|        131|     James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|ST_CLERK|  2500|            - |       121|           50|
|        132|        TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|ST_CLERK|  2100|            - |       121|           50|
|        133|     Jason|     Mallin| JMALLIN|650.127.1934|14-JUN-04|ST_CLERK|  3300|            - |       122|           50|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
only showing top 20 rows

>>> empDf.filter(col("SALARY") < 5000).show(100)
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|  JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+
|        198|    Donald|   OConnell|DOCONNEL|650.507.9833|21-JUN-07|SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|      Grant|  DGRANT|650.507.9844|13-JAN-08|SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|     Whalen| JWHALEN|515.123.4444|17-SEP-03| AD_ASST|  4400|            - |       101|           10|
|        105|     David|     Austin| DAUSTIN|590.423.4569|25-JUN-05| IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|  Pataballa|VPATABAL|590.423.4560|05-FEB-06| IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|    Lorentz|DLORENTZ|590.423.5567|07-FEB-07| IT_PROG|  4200|            - |       103|           60|
|        115| Alexander|       Khoo|   AKHOO|515.127.4562|18-MAY-03|PU_CLERK|  3100|            - |       114|           30|
|        116|    Shelli|      Baida|  SBAIDA|515.127.4563|24-DEC-05|PU_CLERK|  2900|            - |       114|           30|
|        117|     Sigal|     Tobias| STOBIAS|515.127.4564|24-JUL-05|PU_CLERK|  2800|            - |       114|           30|
|        118|       Guy|     Himuro| GHIMURO|515.127.4565|15-NOV-06|PU_CLERK|  2600|            - |       114|           30|
|        119|     Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|PU_CLERK|  2500|            - |       114|           30|
|        125|     Julia|      Nayer|  JNAYER|650.124.1214|16-JUL-05|ST_CLERK|  3200|            - |       120|           50|
|        126|     Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|ST_CLERK|  2700|            - |       120|           50|
|        127|     James|     Landry| JLANDRY|650.124.1334|14-JAN-07|ST_CLERK|  2400|            - |       120|           50|
|        128|    Steven|     Markle| SMARKLE|650.124.1434|08-MAR-08|ST_CLERK|  2200|            - |       120|           50|
|        129|     Laura|     Bissot| LBISSOT|650.124.5234|20-AUG-05|ST_CLERK|  3300|            - |       121|           50|
|        130|     Mozhe|   Atkinson|MATKINSO|650.124.6234|30-OCT-05|ST_CLERK|  2800|            - |       121|           50|
|        131|     James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|ST_CLERK|  2500|            - |       121|           50|
|        132|        TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|ST_CLERK|  2100|            - |       121|           50|
|        133|     Jason|     Mallin| JMALLIN|650.127.1934|14-JUN-04|ST_CLERK|  3300|            - |       122|           50|
|        134|   Michael|     Rogers| MROGERS|650.127.1834|26-AUG-06|ST_CLERK|  2900|            - |       122|           50|
|        135|        Ki|        Gee|    KGEE|650.127.1734|12-DEC-07|ST_CLERK|  2400|            - |       122|           50|
|        136|     Hazel| Philtanker|HPHILTAN|650.127.1634|06-FEB-08|ST_CLERK|  2200|            - |       122|           50|
|        137|    Renske|     Ladwig| RLADWIG|650.121.1234|14-JUL-03|ST_CLERK|  3600|            - |       123|           50|
|        138|   Stephen|     Stiles| SSTILES|650.121.2034|26-OCT-05|ST_CLERK|  3200|            - |       123|           50|
|        139|      John|        Seo|    JSEO|650.121.2019|12-FEB-06|ST_CLERK|  2700|            - |       123|           50|
|        140|    Joshua|      Patel|  JPATEL|650.121.1834|06-APR-06|ST_CLERK|  2500|            - |       123|           50|
+-----------+----------+-----------+--------+------------+---------+--------+------+--------------+----------+-------------+

>>> empDf.filter(col("SALARY") < 5000).select("EMPLOYEE_ID","FIRST_NAME","SALARY").show(100)
+-----------+----------+------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|
+-----------+----------+------+
|        198|    Donald|  2600|
|        199|   Douglas|  2600|
|        200|  Jennifer|  4400|
|        105|     David|  4800|
|        106|     Valli|  4800|
|        107|     Diana|  4200|
|        115| Alexander|  3100|
|        116|    Shelli|  2900|
|        117|     Sigal|  2800|
|        118|       Guy|  2600|
|        119|     Karen|  2500|
|        125|     Julia|  3200|
|        126|     Irene|  2700|
|        127|     James|  2400|
|        128|    Steven|  2200|
|        129|     Laura|  3300|
|        130|     Mozhe|  2800|
|        131|     James|  2500|
|        132|        TJ|  2100|
|        133|     Jason|  3300|
|        134|   Michael|  2900|
|        135|        Ki|  2400|
|        136|     Hazel|  2200|
|        137|    Renske|  3600|
|        138|   Stephen|  3200|
|        139|      John|  2700|
|        140|    Joshua|  2500|
+-----------+----------+------+

>>> empDf.filter((col("DEPARTMENT_ID") == 50) & (col("SALARY") < 5000)).select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(100)
+-----------+----------+------+-------------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+----------+------+-------------+
|        198|    Donald|  2600|           50|
|        199|   Douglas|  2600|           50|
|        125|     Julia|  3200|           50|
|        126|     Irene|  2700|           50|
|        127|     James|  2400|           50|
|        128|    Steven|  2200|           50|
|        129|     Laura|  3300|           50|
|        130|     Mozhe|  2800|           50|
|        131|     James|  2500|           50|
|        132|        TJ|  2100|           50|
|        133|     Jason|  3300|           50|
|        134|   Michael|  2900|           50|
|        135|        Ki|  2400|           50|
|        136|     Hazel|  2200|           50|
|        137|    Renske|  3600|           50|
|        138|   Stephen|  3200|           50|
|        139|      John|  2700|           50|
|        140|    Joshua|  2500|           50|
+-----------+----------+------+-------------+

>>> empDf.filter("DEPARTMENT_ID <> 50").select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(100)
+-----------+-----------+------+-------------+
|EMPLOYEE_ID| FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+-----------+------+-------------+
|        200|   Jennifer|  4400|           10|
|        201|    Michael| 13000|           20|
|        202|        Pat|  6000|           20|
|        203|      Susan|  6500|           40|
|        204|    Hermann| 10000|           70|
|        205|    Shelley| 12008|          110|
|        206|    William|  8300|          110|
|        100|     Steven| 24000|           90|
|        101|      Neena| 17000|           90|
|        102|        Lex| 17000|           90|
|        103|  Alexander|  9000|           60|
|        104|      Bruce|  6000|           60|
|        105|      David|  4800|           60|
|        106|      Valli|  4800|           60|
|        107|      Diana|  4200|           60|
|        108|      Nancy| 12008|          100|
|        109|     Daniel|  9000|          100|
|        110|       John|  8200|          100|
|        111|     Ismael|  7700|          100|
|        112|Jose Manuel|  7800|          100|
|        113|       Luis|  6900|          100|
|        114|        Den| 11000|           30|
|        115|  Alexander|  3100|           30|
|        116|     Shelli|  2900|           30|
|        117|      Sigal|  2800|           30|
|        118|        Guy|  2600|           30|
|        119|      Karen|  2500|           30|
+-----------+-----------+------+-------------+

>>> empDf.filter("DEPARTMENT_ID != 50").select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(100)
+-----------+-----------+------+-------------+
|EMPLOYEE_ID| FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+-----------+------+-------------+
|        200|   Jennifer|  4400|           10|
|        201|    Michael| 13000|           20|
|        202|        Pat|  6000|           20|
|        203|      Susan|  6500|           40|
|        204|    Hermann| 10000|           70|
|        205|    Shelley| 12008|          110|
|        206|    William|  8300|          110|
|        100|     Steven| 24000|           90|
|        101|      Neena| 17000|           90|
|        102|        Lex| 17000|           90|
|        103|  Alexander|  9000|           60|
|        104|      Bruce|  6000|           60|
|        105|      David|  4800|           60|
|        106|      Valli|  4800|           60|
|        107|      Diana|  4200|           60|
|        108|      Nancy| 12008|          100|
|        109|     Daniel|  9000|          100|
|        110|       John|  8200|          100|
|        111|     Ismael|  7700|          100|
|        112|Jose Manuel|  7800|          100|
|        113|       Luis|  6900|          100|
|        114|        Den| 11000|           30|
|        115|  Alexander|  3100|           30|
|        116|     Shelli|  2900|           30|
|        117|      Sigal|  2800|           30|
|        118|        Guy|  2600|           30|
|        119|      Karen|  2500|           30|
+-----------+-----------+------+-------------+

>>> empDf.filter("DEPARTMENT_ID == 50 and SALARY < 5000").select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(100)
+-----------+----------+------+-------------+
|EMPLOYEE_ID|FIRST_NAME|SALARY|DEPARTMENT_ID|
+-----------+----------+------+-------------+
|        198|    Donald|  2600|           50|
|        199|   Douglas|  2600|           50|
|        125|     Julia|  3200|           50|
|        126|     Irene|  2700|           50|
|        127|     James|  2400|           50|
|        128|    Steven|  2200|           50|
|        129|     Laura|  3300|           50|
|        130|     Mozhe|  2800|           50|
|        131|     James|  2500|           50|
|        132|        TJ|  2100|           50|
|        133|     Jason|  3300|           50|
|        134|   Michael|  2900|           50|
|        135|        Ki|  2400|           50|
|        136|     Hazel|  2200|           50|
|        137|    Renske|  3600|           50|
|        138|   Stephen|  3200|           50|
|        139|      John|  2700|           50|
|        140|    Joshua|  2500|           50|
+-----------+----------+------+-------------+

>>> empDf.distinct().show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        120|   Matthew|    Weiss|  MWEISS|650.123.1234|18-JUL-04|    ST_MAN|  8000|            - |       100|           50|
|        118|       Guy|   Himuro| GHIMURO|515.127.4565|15-NOV-06|  PU_CLERK|  2600|            - |       114|           30|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
|        123|    Shanta|  Vollman|SVOLLMAN|650.123.4234|10-OCT-05|    ST_MAN|  6500|            - |       100|           50|
|        124|     Kevin|  Mourgos|KMOURGOS|650.123.5234|16-NOV-07|    ST_MAN|  5800|            - |       100|           50|
|        137|    Renske|   Ladwig| RLADWIG|650.121.1234|14-JUL-03|  ST_CLERK|  3600|            - |       123|           50|
|        132|        TJ|    Olson| TJOLSON|650.124.8234|10-APR-07|  ST_CLERK|  2100|            - |       121|           50|
|        113|      Luis|     Popp|   LPOPP|515.124.4567|07-DEC-07|FI_ACCOUNT|  6900|            - |       108|          100|
|        139|      John|      Seo|    JSEO|650.121.2019|12-FEB-06|  ST_CLERK|  2700|            - |       123|           50|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        135|        Ki|      Gee|    KGEE|650.127.1734|12-DEC-07|  ST_CLERK|  2400|            - |       122|           50|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        121|      Adam|    Fripp|  AFRIPP|650.123.2234|10-APR-05|    ST_MAN|  8200|            - |       100|           50|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        117|     Sigal|   Tobias| STOBIAS|515.127.4564|24-JUL-05|  PU_CLERK|  2800|            - |       114|           30|
|        130|     Mozhe| Atkinson|MATKINSO|650.124.6234|30-OCT-05|  ST_CLERK|  2800|            - |       121|           50|
|        134|   Michael|   Rogers| MROGERS|650.127.1834|26-AUG-06|  ST_CLERK|  2900|            - |       122|           50|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
only showing top 20 rows

>>> empDf.distinct().show(100)
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID| FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        120|    Matthew|      Weiss|  MWEISS|650.123.1234|18-JUL-04|    ST_MAN|  8000|            - |       100|           50|
|        118|        Guy|     Himuro| GHIMURO|515.127.4565|15-NOV-06|  PU_CLERK|  2600|            - |       114|           30|
|        110|       John|       Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
|        123|     Shanta|    Vollman|SVOLLMAN|650.123.4234|10-OCT-05|    ST_MAN|  6500|            - |       100|           50|
|        124|      Kevin|    Mourgos|KMOURGOS|650.123.5234|16-NOV-07|    ST_MAN|  5800|            - |       100|           50|
|        137|     Renske|     Ladwig| RLADWIG|650.121.1234|14-JUL-03|  ST_CLERK|  3600|            - |       123|           50|
|        132|         TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|  ST_CLERK|  2100|            - |       121|           50|
|        113|       Luis|       Popp|   LPOPP|515.124.4567|07-DEC-07|FI_ACCOUNT|  6900|            - |       108|          100|
|        139|       John|        Seo|    JSEO|650.121.2019|12-FEB-06|  ST_CLERK|  2700|            - |       123|           50|
|        201|    Michael|  Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        107|      Diana|    Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        135|         Ki|        Gee|    KGEE|650.127.1734|12-DEC-07|  ST_CLERK|  2400|            - |       122|           50|
|        104|      Bruce|      Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        103|  Alexander|     Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        108|      Nancy|  Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        121|       Adam|      Fripp|  AFRIPP|650.123.2234|10-APR-05|    ST_MAN|  8200|            - |       100|           50|
|        100|     Steven|       King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        117|      Sigal|     Tobias| STOBIAS|515.127.4564|24-JUL-05|  PU_CLERK|  2800|            - |       114|           30|
|        130|      Mozhe|   Atkinson|MATKINSO|650.124.6234|30-OCT-05|  ST_CLERK|  2800|            - |       121|           50|
|        134|    Michael|     Rogers| MROGERS|650.127.1834|26-AUG-06|  ST_CLERK|  2900|            - |       122|           50|
|        133|      Jason|     Mallin| JMALLIN|650.127.1934|14-JUN-04|  ST_CLERK|  3300|            - |       122|           50|
|        138|    Stephen|     Stiles| SSTILES|650.121.2034|26-OCT-05|  ST_CLERK|  3200|            - |       123|           50|
|        105|      David|     Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        111|     Ismael|    Sciarra|ISCIARRA|515.124.4369|30-SEP-05|FI_ACCOUNT|  7700|            - |       108|          100|
|        116|     Shelli|      Baida|  SBAIDA|515.127.4563|24-DEC-05|  PU_CLERK|  2900|            - |       114|           30|
|        204|    Hermann|       Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        115|  Alexander|       Khoo|   AKHOO|515.127.4562|18-MAY-03|  PU_CLERK|  3100|            - |       114|           30|
|        102|        Lex|    De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        114|        Den|   Raphaely|DRAPHEAL|515.127.4561|07-DEC-02|    PU_MAN| 11000|            - |       100|           30|
|        206|    William|      Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|
|        205|    Shelley|    Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
|        119|      Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|  PU_CLERK|  2500|            - |       114|           30|
|        106|      Valli|  Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        126|      Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|  ST_CLERK|  2700|            - |       120|           50|
|        140|     Joshua|      Patel|  JPATEL|650.121.1834|06-APR-06|  ST_CLERK|  2500|            - |       123|           50|
|        125|      Julia|      Nayer|  JNAYER|650.124.1214|16-JUL-05|  ST_CLERK|  3200|            - |       120|           50|
|        136|      Hazel| Philtanker|HPHILTAN|650.127.1634|06-FEB-08|  ST_CLERK|  2200|            - |       122|           50|
|        129|      Laura|     Bissot| LBISSOT|650.124.5234|20-AUG-05|  ST_CLERK|  3300|            - |       121|           50|
|        203|      Susan|     Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        198|     Donald|   OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        202|        Pat|        Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        200|   Jennifer|     Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        109|     Daniel|     Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        101|      Neena|    Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        122|      Payam|   Kaufling|PKAUFLIN|650.123.3234|01-MAY-03|    ST_MAN|  7900|            - |       100|           50|
|        199|    Douglas|      Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        127|      James|     Landry| JLANDRY|650.124.1334|14-JAN-07|  ST_CLERK|  2400|            - |       120|           50|
|        128|     Steven|     Markle| SMARKLE|650.124.1434|08-MAR-08|  ST_CLERK|  2200|            - |       120|           50|
|        112|Jose Manuel|      Urman| JMURMAN|515.124.4469|07-MAR-06|FI_ACCOUNT|  7800|            - |       108|          100|
|        131|      James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|  ST_CLERK|  2500|            - |       121|           50|
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+

>>> empDf.dropDuplicates().show(100)
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID| FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        120|    Matthew|      Weiss|  MWEISS|650.123.1234|18-JUL-04|    ST_MAN|  8000|            - |       100|           50|
|        118|        Guy|     Himuro| GHIMURO|515.127.4565|15-NOV-06|  PU_CLERK|  2600|            - |       114|           30|
|        110|       John|       Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
|        123|     Shanta|    Vollman|SVOLLMAN|650.123.4234|10-OCT-05|    ST_MAN|  6500|            - |       100|           50|
|        124|      Kevin|    Mourgos|KMOURGOS|650.123.5234|16-NOV-07|    ST_MAN|  5800|            - |       100|           50|
|        137|     Renske|     Ladwig| RLADWIG|650.121.1234|14-JUL-03|  ST_CLERK|  3600|            - |       123|           50|
|        132|         TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|  ST_CLERK|  2100|            - |       121|           50|
|        113|       Luis|       Popp|   LPOPP|515.124.4567|07-DEC-07|FI_ACCOUNT|  6900|            - |       108|          100|
|        139|       John|        Seo|    JSEO|650.121.2019|12-FEB-06|  ST_CLERK|  2700|            - |       123|           50|
|        201|    Michael|  Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        107|      Diana|    Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        135|         Ki|        Gee|    KGEE|650.127.1734|12-DEC-07|  ST_CLERK|  2400|            - |       122|           50|
|        104|      Bruce|      Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        103|  Alexander|     Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        108|      Nancy|  Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        121|       Adam|      Fripp|  AFRIPP|650.123.2234|10-APR-05|    ST_MAN|  8200|            - |       100|           50|
|        100|     Steven|       King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        117|      Sigal|     Tobias| STOBIAS|515.127.4564|24-JUL-05|  PU_CLERK|  2800|            - |       114|           30|
|        130|      Mozhe|   Atkinson|MATKINSO|650.124.6234|30-OCT-05|  ST_CLERK|  2800|            - |       121|           50|
|        134|    Michael|     Rogers| MROGERS|650.127.1834|26-AUG-06|  ST_CLERK|  2900|            - |       122|           50|
|        133|      Jason|     Mallin| JMALLIN|650.127.1934|14-JUN-04|  ST_CLERK|  3300|            - |       122|           50|
|        138|    Stephen|     Stiles| SSTILES|650.121.2034|26-OCT-05|  ST_CLERK|  3200|            - |       123|           50|
|        105|      David|     Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        111|     Ismael|    Sciarra|ISCIARRA|515.124.4369|30-SEP-05|FI_ACCOUNT|  7700|            - |       108|          100|
|        116|     Shelli|      Baida|  SBAIDA|515.127.4563|24-DEC-05|  PU_CLERK|  2900|            - |       114|           30|
|        204|    Hermann|       Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        115|  Alexander|       Khoo|   AKHOO|515.127.4562|18-MAY-03|  PU_CLERK|  3100|            - |       114|           30|
|        102|        Lex|    De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        114|        Den|   Raphaely|DRAPHEAL|515.127.4561|07-DEC-02|    PU_MAN| 11000|            - |       100|           30|
|        206|    William|      Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|
|        205|    Shelley|    Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
|        119|      Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|  PU_CLERK|  2500|            - |       114|           30|
|        106|      Valli|  Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        126|      Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|  ST_CLERK|  2700|            - |       120|           50|
|        140|     Joshua|      Patel|  JPATEL|650.121.1834|06-APR-06|  ST_CLERK|  2500|            - |       123|           50|
|        125|      Julia|      Nayer|  JNAYER|650.124.1214|16-JUL-05|  ST_CLERK|  3200|            - |       120|           50|
|        136|      Hazel| Philtanker|HPHILTAN|650.127.1634|06-FEB-08|  ST_CLERK|  2200|            - |       122|           50|
|        129|      Laura|     Bissot| LBISSOT|650.124.5234|20-AUG-05|  ST_CLERK|  3300|            - |       121|           50|
|        203|      Susan|     Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        198|     Donald|   OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        202|        Pat|        Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        200|   Jennifer|     Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        109|     Daniel|     Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        101|      Neena|    Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        122|      Payam|   Kaufling|PKAUFLIN|650.123.3234|01-MAY-03|    ST_MAN|  7900|            - |       100|           50|
|        199|    Douglas|      Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        127|      James|     Landry| JLANDRY|650.124.1334|14-JAN-07|  ST_CLERK|  2400|            - |       120|           50|
|        128|     Steven|     Markle| SMARKLE|650.124.1434|08-MAR-08|  ST_CLERK|  2200|            - |       120|           50|
|        112|Jose Manuel|      Urman| JMURMAN|515.124.4469|07-MAR-06|FI_ACCOUNT|  7800|            - |       108|          100|
|        131|      James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|  ST_CLERK|  2500|            - |       121|           50|
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+

>>> empDf.dropDuplicates(["DEPARTMENT_ID", "HIRE_DATE"]).show(100)
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID| FIRST_NAME|  LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        200|   Jennifer|     Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        202|        Pat|        Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        201|    Michael|  Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        114|        Den|   Raphaely|DRAPHEAL|515.127.4561|07-DEC-02|    PU_MAN| 11000|            - |       100|           30|
|        119|      Karen| Colmenares|KCOLMENA|515.127.4566|10-AUG-07|  PU_CLERK|  2500|            - |       114|           30|
|        118|        Guy|     Himuro| GHIMURO|515.127.4565|15-NOV-06|  PU_CLERK|  2600|            - |       114|           30|
|        115|  Alexander|       Khoo|   AKHOO|515.127.4562|18-MAY-03|  PU_CLERK|  3100|            - |       114|           30|
|        116|     Shelli|      Baida|  SBAIDA|515.127.4563|24-DEC-05|  PU_CLERK|  2900|            - |       114|           30|
|        117|      Sigal|     Tobias| STOBIAS|515.127.4564|24-JUL-05|  PU_CLERK|  2800|            - |       114|           30|
|        203|      Susan|     Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        122|      Payam|   Kaufling|PKAUFLIN|650.123.3234|01-MAY-03|    ST_MAN|  7900|            - |       100|           50|
|        140|     Joshua|      Patel|  JPATEL|650.121.1834|06-APR-06|  ST_CLERK|  2500|            - |       123|           50|
|        136|      Hazel| Philtanker|HPHILTAN|650.127.1634|06-FEB-08|  ST_CLERK|  2200|            - |       122|           50|
|        128|     Steven|     Markle| SMARKLE|650.124.1434|08-MAR-08|  ST_CLERK|  2200|            - |       120|           50|
|        121|       Adam|      Fripp|  AFRIPP|650.123.2234|10-APR-05|    ST_MAN|  8200|            - |       100|           50|
|        132|         TJ|      Olson| TJOLSON|650.124.8234|10-APR-07|  ST_CLERK|  2100|            - |       121|           50|
|        123|     Shanta|    Vollman|SVOLLMAN|650.123.4234|10-OCT-05|    ST_MAN|  6500|            - |       100|           50|
|        135|         Ki|        Gee|    KGEE|650.127.1734|12-DEC-07|  ST_CLERK|  2400|            - |       122|           50|
|        139|       John|        Seo|    JSEO|650.121.2019|12-FEB-06|  ST_CLERK|  2700|            - |       123|           50|
|        199|    Douglas|      Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        127|      James|     Landry| JLANDRY|650.124.1334|14-JAN-07|  ST_CLERK|  2400|            - |       120|           50|
|        137|     Renske|     Ladwig| RLADWIG|650.121.1234|14-JUL-03|  ST_CLERK|  3600|            - |       123|           50|
|        133|      Jason|     Mallin| JMALLIN|650.127.1934|14-JUN-04|  ST_CLERK|  3300|            - |       122|           50|
|        131|      James|     Marlow| JAMRLOW|650.124.7234|16-FEB-05|  ST_CLERK|  2500|            - |       121|           50|
|        125|      Julia|      Nayer|  JNAYER|650.124.1214|16-JUL-05|  ST_CLERK|  3200|            - |       120|           50|
|        124|      Kevin|    Mourgos|KMOURGOS|650.123.5234|16-NOV-07|    ST_MAN|  5800|            - |       100|           50|
|        120|    Matthew|      Weiss|  MWEISS|650.123.1234|18-JUL-04|    ST_MAN|  8000|            - |       100|           50|
|        129|      Laura|     Bissot| LBISSOT|650.124.5234|20-AUG-05|  ST_CLERK|  3300|            - |       121|           50|
|        198|     Donald|   OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        134|    Michael|     Rogers| MROGERS|650.127.1834|26-AUG-06|  ST_CLERK|  2900|            - |       122|           50|
|        138|    Stephen|     Stiles| SSTILES|650.121.2034|26-OCT-05|  ST_CLERK|  3200|            - |       123|           50|
|        126|      Irene|Mikkilineni|IMIKKILI|650.124.1224|28-SEP-06|  ST_CLERK|  2700|            - |       120|           50|
|        130|      Mozhe|   Atkinson|MATKINSO|650.124.6234|30-OCT-05|  ST_CLERK|  2800|            - |       121|           50|
|        103|  Alexander|     Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        106|      Valli|  Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        107|      Diana|    Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        104|      Bruce|      Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        105|      David|     Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        204|    Hermann|       Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        102|        Lex|    De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        100|     Steven|       King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        101|      Neena|    Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        113|       Luis|       Popp|   LPOPP|515.124.4567|07-DEC-07|FI_ACCOUNT|  6900|            - |       108|          100|
|        112|Jose Manuel|      Urman| JMURMAN|515.124.4469|07-MAR-06|FI_ACCOUNT|  7800|            - |       108|          100|
|        109|     Daniel|     Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        108|      Nancy|  Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        110|       John|       Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
|        111|     Ismael|    Sciarra|ISCIARRA|515.124.4369|30-SEP-05|FI_ACCOUNT|  7700|            - |       108|          100|
|        205|    Shelley|    Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
+-----------+-----------+-----------+--------+------------+---------+----------+------+--------------+----------+-------------+

>>> empDf.dropDuplicates(["DEPARTMENT_ID", "HIRE_DATE"]).select("EMPLOYEE_ID","HIRE_DATE","DEPARTMENT_ID").show(100)
+-----------+---------+-------------+
|EMPLOYEE_ID|HIRE_DATE|DEPARTMENT_ID|
+-----------+---------+-------------+
|        123|10-OCT-05|           50|
|        101|21-SEP-05|           90|
|        107|07-FEB-07|           60|
|        108|17-AUG-02|          100|
|        124|16-NOV-07|           50|
|        102|13-JAN-01|           90|
|        120|18-JUL-04|           50|
|        204|07-JUN-02|           70|
|        134|26-AUG-06|           50|
|        201|17-FEB-04|           20|
|        113|07-DEC-07|          100|
|        119|10-AUG-07|           30|
|        139|12-FEB-06|           50|
|        202|17-AUG-05|           20|
|        126|28-SEP-06|           50|
|        135|12-DEC-07|           50|
|        104|21-MAY-07|           60|
|        106|05-FEB-06|           60|
|        100|17-JUN-03|           90|
|        132|10-APR-07|           50|
|        137|14-JUL-03|           50|
|        121|10-APR-05|           50|
|        122|01-MAY-03|           50|
|        131|16-FEB-05|           50|
|        198|21-JUN-07|           50|
|        105|25-JUN-05|           60|
|        116|24-DEC-05|           30|
|        111|30-SEP-05|          100|
|        200|17-SEP-03|           10|
|        203|07-JUN-02|           40|
|        129|20-AUG-05|           50|
|        140|06-APR-06|           50|
|        130|30-OCT-05|           50|
|        115|18-MAY-03|           30|
|        103|03-JAN-06|           60|
|        136|06-FEB-08|           50|
|        138|26-OCT-05|           50|
|        205|07-JUN-02|          110|
|        114|07-DEC-02|           30|
|        112|07-MAR-06|          100|
|        125|16-JUL-05|           50|
|        199|13-JAN-08|           50|
|        118|15-NOV-06|           30|
|        128|08-MAR-08|           50|
|        109|16-AUG-02|          100|
|        133|14-JUN-04|           50|
|        117|24-JUL-05|           30|
|        110|28-SEP-05|          100|
|        127|14-JAN-07|           50|
+-----------+---------+-------------+

>>> from pyspark.sql.functions import *
>>> empDf.count()
50
>>> empDf.select(count("salary")).show()
+-------------+
|count(salary)|
+-------------+
|           50|
+-------------+

>>> empDf.select(count("salary").alias("total_count")).show()
+-----------+
|total_count|
+-----------+
|         50|
+-----------+

>>> empDf.select(max("salary").alias("max_salary")).show()
+----------+
|max_salary|
+----------+
|     24000|
+----------+

>>> empDf.select(min("salary").alias("min_salary")).show()
+----------+
|min_salary|
+----------+
|      2100|
+----------+

>>> empDf.select(avg("salary").alias("avg_salary")).show()
+----------+
|avg_salary|
+----------+
|   6182.32|
+----------+

>>> empDf.select(sum("salary").alias("sum_salary")).show()
+----------+
|sum_salary|
+----------+
|    309116|
+----------+

>>> empDf.select("EMPLOYEE_ID", sum("salary").alias("sum_salary")).show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/local/spark/python/pyspark/sql/dataframe.py", line 1685, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/usr/local/spark/python/lib/py4j-0.10.9.3-src.zip/py4j/java_gateway.py", line 1321, in __call__
  File "/usr/local/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: grouping expressions sequence is empty, and 'EMPLOYEE_ID' is not an aggregate function. Wrap '(sum(salary) AS sum_salary)' in windowing function(s) or wrap 'EMPLOYEE_ID' in first() (or first_value) if you don't care which value you get.;
Aggregate [EMPLOYEE_ID#16, sum(salary#23) AS sum_salary#193L]
+- Relation [EMPLOYEE_ID#16,FIRST_NAME#17,LAST_NAME#18,EMAIL#19,PHONE_NUMBER#20,HIRE_DATE#21,JOB_ID#22,SALARY#23,COMMISSION_PCT#24,MANAGER_ID#25,DEPARTMENT_ID#26] csv

>>> empDf.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy("salary").show()
+-----------+----------+-------------+------+
|EMPLOYEE_ID|FIRST_NAME|DEPARTMENT_ID|SALARY|
+-----------+----------+-------------+------+
|        132|        TJ|           50|  2100|
|        136|     Hazel|           50|  2200|
|        128|    Steven|           50|  2200|
|        127|     James|           50|  2400|
|        135|        Ki|           50|  2400|
|        131|     James|           50|  2500|
|        119|     Karen|           30|  2500|
|        140|    Joshua|           50|  2500|
|        198|    Donald|           50|  2600|
|        199|   Douglas|           50|  2600|
|        118|       Guy|           30|  2600|
|        126|     Irene|           50|  2700|
|        139|      John|           50|  2700|
|        130|     Mozhe|           50|  2800|
|        117|     Sigal|           30|  2800|
|        116|    Shelli|           30|  2900|
|        134|   Michael|           50|  2900|
|        115| Alexander|           30|  3100|
|        125|     Julia|           50|  3200|
|        138|   Stephen|           50|  3200|
+-----------+----------+-------------+------+
only showing top 20 rows

>>> empDf.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy(col("DEPARTMENT_ID").asc(),col("SALARY").desc()).show(100)
+-----------+-----------+-------------+------+
|EMPLOYEE_ID| FIRST_NAME|DEPARTMENT_ID|SALARY|
+-----------+-----------+-------------+------+
|        200|   Jennifer|           10|  4400|
|        201|    Michael|           20| 13000|
|        202|        Pat|           20|  6000|
|        114|        Den|           30| 11000|
|        115|  Alexander|           30|  3100|
|        116|     Shelli|           30|  2900|
|        117|      Sigal|           30|  2800|
|        118|        Guy|           30|  2600|
|        119|      Karen|           30|  2500|
|        203|      Susan|           40|  6500|
|        121|       Adam|           50|  8200|
|        120|    Matthew|           50|  8000|
|        122|      Payam|           50|  7900|
|        123|     Shanta|           50|  6500|
|        124|      Kevin|           50|  5800|
|        137|     Renske|           50|  3600|
|        133|      Jason|           50|  3300|
|        129|      Laura|           50|  3300|
|        125|      Julia|           50|  3200|
|        138|    Stephen|           50|  3200|
|        134|    Michael|           50|  2900|
|        130|      Mozhe|           50|  2800|
|        126|      Irene|           50|  2700|
|        139|       John|           50|  2700|
|        198|     Donald|           50|  2600|
|        199|    Douglas|           50|  2600|
|        131|      James|           50|  2500|
|        140|     Joshua|           50|  2500|
|        135|         Ki|           50|  2400|
|        127|      James|           50|  2400|
|        128|     Steven|           50|  2200|
|        136|      Hazel|           50|  2200|
|        132|         TJ|           50|  2100|
|        103|  Alexander|           60|  9000|
|        104|      Bruce|           60|  6000|
|        105|      David|           60|  4800|
|        106|      Valli|           60|  4800|
|        107|      Diana|           60|  4200|
|        204|    Hermann|           70| 10000|
|        100|     Steven|           90| 24000|
|        102|        Lex|           90| 17000|
|        101|      Neena|           90| 17000|
|        108|      Nancy|          100| 12008|
|        109|     Daniel|          100|  9000|
|        110|       John|          100|  8200|
|        112|Jose Manuel|          100|  7800|
|        111|     Ismael|          100|  7700|
|        113|       Luis|          100|  6900|
|        205|    Shelley|          110| 12008|
|        206|    William|          110|  8300|
+-----------+-----------+-------------+------+

>>> empDf.groupBy("DEPARTMENT_ID").sum("SALARY").show(100)
+-------------+-----------+
|DEPARTMENT_ID|sum(SALARY)|
+-------------+-----------+
|           20|      19000|
|           40|       6500|
|          100|      51608|
|           10|       4400|
|           50|      85600|
|           70|      10000|
|           90|      58000|
|           60|      28800|
|          110|      20308|
|           30|      24900|
+-------------+-----------+

>>> empDf.groupBy("DEPARTMENT_ID").max("SALARY").show(100)
+-------------+-----------+
|DEPARTMENT_ID|max(SALARY)|
+-------------+-----------+
|           20|      13000|
|           40|       6500|
|          100|      12008|
|           10|       4400|
|           50|       8200|
|           70|      10000|
|           90|      24000|
|           60|       9000|
|          110|      12008|
|           30|      11000|
+-------------+-----------+

>>> empDf.groupBy("DEPARTMENT_ID").min("SALARY").show(100)
+-------------+-----------+
|DEPARTMENT_ID|min(SALARY)|
+-------------+-----------+
|           20|       6000|
|           40|       6500|
|          100|       6900|
|           10|       4400|
|           50|       2100|
|           70|      10000|
|           90|      17000|
|           60|       4200|
|          110|       8300|
|           30|       2500|
+-------------+-----------+

>>> empDf.groupBy("DEPARTMENT_ID","JOB_ID").sum("SALARY").show(100)
+-------------+----------+-----------+
|DEPARTMENT_ID|    JOB_ID|sum(SALARY)|
+-------------+----------+-----------+
|           90|   AD_PRES|      24000|
|           30|    PU_MAN|      11000|
|           70|    PR_REP|      10000|
|           50|    ST_MAN|      36400|
|           40|    HR_REP|       6500|
|           60|   IT_PROG|      28800|
|           10|   AD_ASST|       4400|
|           30|  PU_CLERK|      13900|
|           50|  ST_CLERK|      44000|
|           20|    MK_REP|       6000|
|           50|  SH_CLERK|       5200|
|           90|     AD_VP|      34000|
|          100|FI_ACCOUNT|      39600|
|          110|    AC_MGR|      12008|
|          110|AC_ACCOUNT|       8300|
|           20|    MK_MAN|      13000|
|          100|    FI_MGR|      12008|
+-------------+----------+-----------+

>>> empDf.printSchema()
root
 |-- EMPLOYEE_ID: integer (nullable = true)
 |-- FIRST_NAME: string (nullable = true)
 |-- LAST_NAME: string (nullable = true)
 |-- EMAIL: string (nullable = true)
 |-- PHONE_NUMBER: string (nullable = true)
 |-- HIRE_DATE: string (nullable = true)
 |-- JOB_ID: string (nullable = true)
 |-- SALARY: integer (nullable = true)
 |-- COMMISSION_PCT: string (nullable = true)
 |-- MANAGER_ID: string (nullable = true)
 |-- DEPARTMENT_ID: integer (nullable = true)

>>> empDf.groupBy("DEPARTMENT_ID","JOB_ID").sum("SALARY", "EMPLOYEE_ID").show(100)
+-------------+----------+-----------+----------------+
|DEPARTMENT_ID|    JOB_ID|sum(SALARY)|sum(EMPLOYEE_ID)|
+-------------+----------+-----------+----------------+
|           90|   AD_PRES|      24000|             100|
|           30|    PU_MAN|      11000|             114|
|           70|    PR_REP|      10000|             204|
|           50|    ST_MAN|      36400|             610|
|           40|    HR_REP|       6500|             203|
|           60|   IT_PROG|      28800|             525|
|           10|   AD_ASST|       4400|             200|
|           30|  PU_CLERK|      13900|             585|
|           50|  ST_CLERK|      44000|            2120|
|           20|    MK_REP|       6000|             202|
|           50|  SH_CLERK|       5200|             397|
|           90|     AD_VP|      34000|             203|
|          100|FI_ACCOUNT|      39600|             555|
|          110|    AC_MGR|      12008|             205|
|          110|AC_ACCOUNT|       8300|             206|
|           20|    MK_MAN|      13000|             201|
|          100|    FI_MGR|      12008|             108|
+-------------+----------+-----------+----------------+

>>> empDf.groupBy("DEPARTMENT_ID").agg(sum("SALARY").alias("SUM_SALARY") , max("SALARY").alias("MAX_SALARY"), min("SALARY").alias("MIN_SALARY") , avg("SALARY").alias("AVG_SALARY")).show()
+-------------+----------+----------+----------+------------------+
|DEPARTMENT_ID|SUM_SALARY|MAX_SALARY|MIN_SALARY|        AVG_SALARY|
+-------------+----------+----------+----------+------------------+
|           20|     19000|     13000|      6000|            9500.0|
|           40|      6500|      6500|      6500|            6500.0|
|          100|     51608|     12008|      6900| 8601.333333333334|
|           10|      4400|      4400|      4400|            4400.0|
|           50|     85600|      8200|      2100|3721.7391304347825|
|           70|     10000|     10000|     10000|           10000.0|
|           90|     58000|     24000|     17000|19333.333333333332|
|           60|     28800|      9000|      4200|            5760.0|
|          110|     20308|     12008|      8300|           10154.0|
|           30|     24900|     11000|      2500|            4150.0|
+-------------+----------+----------+----------+------------------+


>>> empDf.groupBy("DEPARTMENT_ID").agg(sum("SALARY").alias("SUM_SALARY") , max("SALARY").alias("MAX_SALARY"), min("SALARY").alias("MIN_SALARY") , avg("SALARY").alias("AVG_SALARY")).where(col("MAX_SALARY") >= 10000).show()
+-------------+----------+----------+----------+------------------+
|DEPARTMENT_ID|SUM_SALARY|MAX_SALARY|MIN_SALARY|        AVG_SALARY|
+-------------+----------+----------+----------+------------------+
|           20|     19000|     13000|      6000|            9500.0|
|          100|     51608|     12008|      6900| 8601.333333333334|
|           70|     10000|     10000|     10000|           10000.0|
|           90|     58000|     24000|     17000|19333.333333333332|
|          110|     20308|     12008|      8300|           10154.0|
|           30|     24900|     11000|      2500|            4150.0|
+-------------+----------+----------+----------+------------------+

>>> empDf.groupBy("DEPARTMENT_ID").agg(sum("SALARY").alias("SUM_SALARY") , max("SALARY").alias("MAX_SALARY"), min("SALARY").alias("MIN_SALARY") , avg("SALARY").alias("AVG_SALARY")).where(col("MAX_SALARY") >= 10000).show()
+-------------+----------+----------+----------+------------------+
|DEPARTMENT_ID|SUM_SALARY|MAX_SALARY|MIN_SALARY|        AVG_SALARY|
+-------------+----------+----------+----------+------------------+
|           20|     19000|     13000|      6000|            9500.0|
|          100|     51608|     12008|      6900| 8601.333333333334|
|           70|     10000|     10000|     10000|           10000.0|
|           90|     58000|     24000|     17000|19333.333333333332|
|          110|     20308|     12008|      8300|           10154.0|
|           30|     24900|     11000|      2500|            4150.0|
+-------------+----------+----------+----------+------------------+

>>> df = empDf.withColumn("EMP_GRADE", when( col("SALARY") > 15000 , "A").when( (col("SALARY") >= 10000) & ( col("SALARY") < 15000), "B").otherwise("C"))
>>> df.select("EMPLOYEE_ID", "SALARY", "EMP_GRADE").show(100)
+-----------+------+---------+
|EMPLOYEE_ID|SALARY|EMP_GRADE|
+-----------+------+---------+
|        198|  2600|        C|
|        199|  2600|        C|
|        200|  4400|        C|
|        201| 13000|        B|
|        202|  6000|        C|
|        203|  6500|        C|
|        204| 10000|        B|
|        205| 12008|        B|
|        206|  8300|        C|
|        100| 24000|        A|
|        101| 17000|        A|
|        102| 17000|        A|
|        103|  9000|        C|
|        104|  6000|        C|
|        105|  4800|        C|
|        106|  4800|        C|
|        107|  4200|        C|
|        108| 12008|        B|
|        109|  9000|        C|
|        110|  8200|        C|
|        111|  7700|        C|
|        112|  7800|        C|
|        113|  6900|        C|
|        114| 11000|        B|
|        115|  3100|        C|
|        116|  2900|        C|
|        117|  2800|        C|
|        118|  2600|        C|
|        119|  2500|        C|
|        120|  8000|        C|
|        121|  8200|        C|
|        122|  7900|        C|
|        123|  6500|        C|
|        124|  5800|        C|
|        125|  3200|        C|
|        126|  2700|        C|
|        127|  2400|        C|
|        128|  2200|        C|
|        129|  3300|        C|
|        130|  2800|        C|
|        131|  2500|        C|
|        132|  2100|        C|
|        133|  3300|        C|
|        134|  2900|        C|
|        135|  2400|        C|
|        136|  2200|        C|
|        137|  3600|        C|
|        138|  3200|        C|
|        139|  2700|        C|
|        140|  2500|        C|
+-----------+------+---------+

>>> empDf.createOrReplaceTempView("employee")
>>> spark.sql(" select * from employee limit 5").show()
+-----------+----------+---------+--------+------------+---------+--------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|  JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+--------+------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03| AD_ASST|  4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|  MK_MAN| 13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|  MK_REP|  6000|            - |       201|           20|
+-----------+----------+---------+--------+------------+---------+--------+------+--------------+----------+-------------+

>>> df = spark.sql(" select employee_id,salary from employee")
>>> df.show(100)
+-----------+------+
|employee_id|salary|
+-----------+------+
|        198|  2600|
|        199|  2600|
|        200|  4400|
|        201| 13000|
|        202|  6000|
|        203|  6500|
|        204| 10000|
|        205| 12008|
|        206|  8300|
|        100| 24000|
|        101| 17000|
|        102| 17000|
|        103|  9000|
|        104|  6000|
|        105|  4800|
|        106|  4800|
|        107|  4200|
|        108| 12008|
|        109|  9000|
|        110|  8200|
|        111|  7700|
|        112|  7800|
|        113|  6900|
|        114| 11000|
|        115|  3100|
|        116|  2900|
|        117|  2800|
|        118|  2600|
|        119|  2500|
|        120|  8000|
|        121|  8200|
|        122|  7900|
|        123|  6500|
|        124|  5800|
|        125|  3200|
|        126|  2700|
|        127|  2400|
|        128|  2200|
|        129|  3300|
|        130|  2800|
|        131|  2500|
|        132|  2100|
|        133|  3300|
|        134|  2900|
|        135|  2400|
|        136|  2200|
|        137|  3600|
|        138|  3200|
|        139|  2700|
|        140|  2500|
+-----------+------+

>>> spark.sql("select department_id, sum(salary) as sum_salary from employee group by department_id").show()
+-------------+----------+
|department_id|sum_salary|
+-------------+----------+
|           20|     19000|
|           40|      6500|
|          100|     51608|
|           10|      4400|
|           50|     85600|
|           70|     10000|
|           90|     58000|
|           60|     28800|
|          110|     20308|
|           30|     24900|
+-------------+----------+

>>> spark.sql("select employee_id, department_id, rank() over(partition by department_id order by salary desc) as rank_salary from employee").show()
+-----------+-------------+-----------+
|employee_id|department_id|rank_salary|
+-----------+-------------+-----------+
|        200|           10|          1|
|        201|           20|          1|
|        202|           20|          2|
|        114|           30|          1|
|        115|           30|          2|
|        116|           30|          3|
|        117|           30|          4|
|        118|           30|          5|
|        119|           30|          6|
|        203|           40|          1|
|        121|           50|          1|
|        120|           50|          2|
|        122|           50|          3|
|        123|           50|          4|
|        124|           50|          5|
|        137|           50|          6|
|        129|           50|          7|
|        133|           50|          7|
|        125|           50|          9|
|        138|           50|          9|
+-----------+-------------+-----------+
only showing top 20 rows

>>> empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+-------------+----------------+----------+-----------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|DEPARTMENT_ID| DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+-------------+----------------+----------+-----------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|           50|        Shipping|       121|       1500|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|           50|        Shipping|       121|       1500|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|           10|  Administration|       200|       1700|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|           20|       Marketing|       201|       1800|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|           20|       Marketing|       201|       1800|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|           40| Human Resources|       203|       2400|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|           70|Public Relations|       204|       2700|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|          110|      Accounting|       205|       1700|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|          110|      Accounting|       205|       1700|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|           90|       Executive|       100|       1700|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|           90|       Executive|       100|       1700|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|           90|       Executive|       100|       1700|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|           60|              IT|       103|       1400|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|           60|              IT|       103|       1400|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|           60|              IT|       103|       1400|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|           60|              IT|       103|       1400|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|           60|              IT|       103|       1400|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|          100|         Finance|       108|       1700|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|          100|         Finance|       108|       1700|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|          100|         Finance|       108|       1700|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+-------------+----------------+----------+-----------+
only showing top 20 rows

>>> empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select("EMPLOYEE_ID", "DEPARTMENT_ID", "DEPARTMENT_NAME").show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/local/spark/python/pyspark/sql/dataframe.py", line 1685, in select
    jdf = self._jdf.select(self._jcols(*cols))
  File "/usr/local/spark/python/lib/py4j-0.10.9.3-src.zip/py4j/java_gateway.py", line 1321, in __call__
  File "/usr/local/spark/python/pyspark/sql/utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: Reference 'DEPARTMENT_ID' is ambiguous, could be: DEPARTMENT_ID, DEPARTMENT_ID.
>>> empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_NAME).show()
+-----------+-------------+---------------+
|EMPLOYEE_ID|DEPARTMENT_ID|DEPARTMENT_NAME|
+-----------+-------------+---------------+
|        200|           10| Administration|
|        202|           20|      Marketing|
|        201|           20|      Marketing|
|        119|           30|     Purchasing|
|        118|           30|     Purchasing|
|        117|           30|     Purchasing|
|        116|           30|     Purchasing|
|        115|           30|     Purchasing|
|        114|           30|     Purchasing|
|        203|           40|Human Resources|
|        140|           50|       Shipping|
|        139|           50|       Shipping|
|        138|           50|       Shipping|
|        137|           50|       Shipping|
|        136|           50|       Shipping|
|        135|           50|       Shipping|
|        134|           50|       Shipping|
|        133|           50|       Shipping|
|        132|           50|       Shipping|
|        131|           50|       Shipping|
+-----------+-------------+---------------+
only showing top 20 rows

>>> empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "left").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_NAME).show(100)
+-----------+-------------+----------------+
|EMPLOYEE_ID|DEPARTMENT_ID| DEPARTMENT_NAME|
+-----------+-------------+----------------+
|        198|           50|        Shipping|
|        199|           50|        Shipping|
|        200|           10|  Administration|
|        201|           20|       Marketing|
|        202|           20|       Marketing|
|        203|           40| Human Resources|
|        204|           70|Public Relations|
|        205|          110|      Accounting|
|        206|          110|      Accounting|
|        100|           90|       Executive|
|        101|           90|       Executive|
|        102|           90|       Executive|
|        103|           60|              IT|
|        104|           60|              IT|
|        105|           60|              IT|
|        106|           60|              IT|
|        107|           60|              IT|
|        108|          100|         Finance|
|        109|          100|         Finance|
|        110|          100|         Finance|
|        111|          100|         Finance|
|        112|          100|         Finance|
|        113|          100|         Finance|
|        114|           30|      Purchasing|
|        115|           30|      Purchasing|
|        116|           30|      Purchasing|
|        117|           30|      Purchasing|
|        118|           30|      Purchasing|
|        119|           30|      Purchasing|
|        120|           50|        Shipping|
|        121|           50|        Shipping|
|        122|           50|        Shipping|
|        123|           50|        Shipping|
|        124|           50|        Shipping|
|        125|           50|        Shipping|
|        126|           50|        Shipping|
|        127|           50|        Shipping|
|        128|           50|        Shipping|
|        129|           50|        Shipping|
|        130|           50|        Shipping|
|        131|           50|        Shipping|
|        132|           50|        Shipping|
|        133|           50|        Shipping|
|        134|           50|        Shipping|
|        135|           50|        Shipping|
|        136|           50|        Shipping|
|        137|           50|        Shipping|
|        138|           50|        Shipping|
|        139|           50|        Shipping|
|        140|           50|        Shipping|
+-----------+-------------+----------------+

>>> empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "right").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_NAME).show(100)
+-----------+-------------+--------------------+
|EMPLOYEE_ID|DEPARTMENT_ID|     DEPARTMENT_NAME|
+-----------+-------------+--------------------+
|        200|           10|      Administration|
|        202|           20|           Marketing|
|        201|           20|           Marketing|
|        119|           30|          Purchasing|
|        118|           30|          Purchasing|
|        117|           30|          Purchasing|
|        116|           30|          Purchasing|
|        115|           30|          Purchasing|
|        114|           30|          Purchasing|
|        203|           40|     Human Resources|
|        140|           50|            Shipping|
|        139|           50|            Shipping|
|        138|           50|            Shipping|
|        137|           50|            Shipping|
|        136|           50|            Shipping|
|        135|           50|            Shipping|
|        134|           50|            Shipping|
|        133|           50|            Shipping|
|        132|           50|            Shipping|
|        131|           50|            Shipping|
|        130|           50|            Shipping|
|        129|           50|            Shipping|
|        128|           50|            Shipping|
|        127|           50|            Shipping|
|        126|           50|            Shipping|
|        125|           50|            Shipping|
|        124|           50|            Shipping|
|        123|           50|            Shipping|
|        122|           50|            Shipping|
|        121|           50|            Shipping|
|        120|           50|            Shipping|
|        199|           50|            Shipping|
|        198|           50|            Shipping|
|        107|           60|                  IT|
|        106|           60|                  IT|
|        105|           60|                  IT|
|        104|           60|                  IT|
|        103|           60|                  IT|
|        204|           70|    Public Relations|
|       null|         null|               Sales|
|        102|           90|           Executive|
|        101|           90|           Executive|
|        100|           90|           Executive|
|        113|          100|             Finance|
|        112|          100|             Finance|
|        111|          100|             Finance|
|        110|          100|             Finance|
|        109|          100|             Finance|
|        108|          100|             Finance|
|        206|          110|          Accounting|
|        205|          110|          Accounting|
|       null|         null|            Treasury|
|       null|         null|       Corporate Tax|
|       null|         null|  Control And Credit|
|       null|         null|Shareholder Services|
|       null|         null|            Benefits|
|       null|         null|       Manufacturing|
|       null|         null|        Construction|
|       null|         null|         Contracting|
|       null|         null|          Operations|
|       null|         null|          IT Support|
|       null|         null|                 NOC|
|       null|         null|         IT Helpdesk|
|       null|         null|    Government Sales|
|       null|         null|        Retail Sales|
|       null|         null|          Recruiting|
|       null|         null|             Payroll|
+-----------+-------------+--------------------+

>>> empDf.join(deptDf, empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "fullouter").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_NAME).show(100)
+-----------+-------------+--------------------+
|EMPLOYEE_ID|DEPARTMENT_ID|     DEPARTMENT_NAME|
+-----------+-------------+--------------------+
|        200|           10|      Administration|
|        201|           20|           Marketing|
|        202|           20|           Marketing|
|        114|           30|          Purchasing|
|        115|           30|          Purchasing|
|        116|           30|          Purchasing|
|        117|           30|          Purchasing|
|        118|           30|          Purchasing|
|        119|           30|          Purchasing|
|        203|           40|     Human Resources|
|        198|           50|            Shipping|
|        199|           50|            Shipping|
|        120|           50|            Shipping|
|        121|           50|            Shipping|
|        122|           50|            Shipping|
|        123|           50|            Shipping|
|        124|           50|            Shipping|
|        125|           50|            Shipping|
|        126|           50|            Shipping|
|        127|           50|            Shipping|
|        128|           50|            Shipping|
|        129|           50|            Shipping|
|        130|           50|            Shipping|
|        131|           50|            Shipping|
|        132|           50|            Shipping|
|        133|           50|            Shipping|
|        134|           50|            Shipping|
|        135|           50|            Shipping|
|        136|           50|            Shipping|
|        137|           50|            Shipping|
|        138|           50|            Shipping|
|        139|           50|            Shipping|
|        140|           50|            Shipping|
|        103|           60|                  IT|
|        104|           60|                  IT|
|        105|           60|                  IT|
|        106|           60|                  IT|
|        107|           60|                  IT|
|        204|           70|    Public Relations|
|       null|         null|               Sales|
|        100|           90|           Executive|
|        101|           90|           Executive|
|        102|           90|           Executive|
|        108|          100|             Finance|
|        109|          100|             Finance|
|        110|          100|             Finance|
|        111|          100|             Finance|
|        112|          100|             Finance|
|        113|          100|             Finance|
|        205|          110|          Accounting|
|        206|          110|          Accounting|
|       null|         null|            Treasury|
|       null|         null|       Corporate Tax|
|       null|         null|  Control And Credit|
|       null|         null|Shareholder Services|
|       null|         null|            Benefits|
|       null|         null|       Manufacturing|
|       null|         null|        Construction|
|       null|         null|         Contracting|
|       null|         null|          Operations|
|       null|         null|          IT Support|
|       null|         null|                 NOC|
|       null|         null|         IT Helpdesk|
|       null|         null|    Government Sales|
|       null|         null|        Retail Sales|
|       null|         null|          Recruiting|
|       null|         null|             Payroll|
+-----------+-------------+--------------------+

>>> deptDf.show()
+-------------+--------------------+----------+-----------+
|DEPARTMENT_ID|     DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-------------+--------------------+----------+-----------+
|           10|      Administration|       200|       1700|
|           20|           Marketing|       201|       1800|
|           30|          Purchasing|       114|       1700|
|           40|     Human Resources|       203|       2400|
|           50|            Shipping|       121|       1500|
|           60|                  IT|       103|       1400|
|           70|    Public Relations|       204|       2700|
|           80|               Sales|       145|       2500|
|           90|           Executive|       100|       1700|
|          100|             Finance|       108|       1700|
|          110|          Accounting|       205|       1700|
|          120|            Treasury|        - |       1700|
|          130|       Corporate Tax|        - |       1700|
|          140|  Control And Credit|        - |       1700|
|          150|Shareholder Services|        - |       1700|
|          160|            Benefits|        - |       1700|
|          170|       Manufacturing|        - |       1700|
|          180|        Construction|        - |       1700|
|          190|         Contracting|        - |       1700|
|          200|          Operations|        - |       1700|
+-------------+--------------------+----------+-----------+
only showing top 20 rows

>>> empDf.show()
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|   EMAIL|PHONE_NUMBER|HIRE_DATE|    JOB_ID|SALARY|COMMISSION_PCT|MANAGER_ID|DEPARTMENT_ID|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
|        198|    Donald| OConnell|DOCONNEL|650.507.9833|21-JUN-07|  SH_CLERK|  2600|            - |       124|           50|
|        199|   Douglas|    Grant|  DGRANT|650.507.9844|13-JAN-08|  SH_CLERK|  2600|            - |       124|           50|
|        200|  Jennifer|   Whalen| JWHALEN|515.123.4444|17-SEP-03|   AD_ASST|  4400|            - |       101|           10|
|        201|   Michael|Hartstein|MHARTSTE|515.123.5555|17-FEB-04|    MK_MAN| 13000|            - |       100|           20|
|        202|       Pat|      Fay|    PFAY|603.123.6666|17-AUG-05|    MK_REP|  6000|            - |       201|           20|
|        203|     Susan|   Mavris| SMAVRIS|515.123.7777|07-JUN-02|    HR_REP|  6500|            - |       101|           40|
|        204|   Hermann|     Baer|   HBAER|515.123.8888|07-JUN-02|    PR_REP| 10000|            - |       101|           70|
|        205|   Shelley|  Higgins|SHIGGINS|515.123.8080|07-JUN-02|    AC_MGR| 12008|            - |       101|          110|
|        206|   William|    Gietz|  WGIETZ|515.123.8181|07-JUN-02|AC_ACCOUNT|  8300|            - |       205|          110|
|        100|    Steven|     King|   SKING|515.123.4567|17-JUN-03|   AD_PRES| 24000|            - |        - |           90|
|        101|     Neena|  Kochhar|NKOCHHAR|515.123.4568|21-SEP-05|     AD_VP| 17000|            - |       100|           90|
|        102|       Lex|  De Haan| LDEHAAN|515.123.4569|13-JAN-01|     AD_VP| 17000|            - |       100|           90|
|        103| Alexander|   Hunold| AHUNOLD|590.423.4567|03-JAN-06|   IT_PROG|  9000|            - |       102|           60|
|        104|     Bruce|    Ernst|  BERNST|590.423.4568|21-MAY-07|   IT_PROG|  6000|            - |       103|           60|
|        105|     David|   Austin| DAUSTIN|590.423.4569|25-JUN-05|   IT_PROG|  4800|            - |       103|           60|
|        106|     Valli|Pataballa|VPATABAL|590.423.4560|05-FEB-06|   IT_PROG|  4800|            - |       103|           60|
|        107|     Diana|  Lorentz|DLORENTZ|590.423.5567|07-FEB-07|   IT_PROG|  4200|            - |       103|           60|
|        108|     Nancy|Greenberg|NGREENBE|515.124.4569|17-AUG-02|    FI_MGR| 12008|            - |       101|          100|
|        109|    Daniel|   Faviet| DFAVIET|515.124.4169|16-AUG-02|FI_ACCOUNT|  9000|            - |       108|          100|
|        110|      John|     Chen|   JCHEN|515.124.4269|28-SEP-05|FI_ACCOUNT|  8200|            - |       108|          100|
+-----------+----------+---------+--------+------------+---------+----------+------+--------------+----------+-------------+
only showing top 20 rows

>>> deptDf.filter(deptDf.MANAGER_ID != "-").show()
+-------------+--------------------+----------+-----------+
|DEPARTMENT_ID|     DEPARTMENT_NAME|MANAGER_ID|LOCATION_ID|
+-------------+--------------------+----------+-----------+
|           10|      Administration|       200|       1700|
|           20|           Marketing|       201|       1800|
|           30|          Purchasing|       114|       1700|
|           40|     Human Resources|       203|       2400|
|           50|            Shipping|       121|       1500|
|           60|                  IT|       103|       1400|
|           70|    Public Relations|       204|       2700|
|           80|               Sales|       145|       2500|
|           90|           Executive|       100|       1700|
|          100|             Finance|       108|       1700|
|          110|          Accounting|       205|       1700|
|          120|            Treasury|        - |       1700|
|          130|       Corporate Tax|        - |       1700|
|          140|  Control And Credit|        - |       1700|
|          150|Shareholder Services|        - |       1700|
|          160|            Benefits|        - |       1700|
|          170|       Manufacturing|        - |       1700|
|          180|        Construction|        - |       1700|
|          190|         Contracting|        - |       1700|
|          200|          Operations|        - |       1700|
+-------------+--------------------+----------+-----------+
only showing top 20 rows

>>> empDf.alias("emp1").join(empDf.alias("emp2") , col("emp1.manager_id") == col("emp2.employee_id"), "inner").select(col("emp1.manager_id"), col("emp2.first_name"), col("emp2.last_name")).show(100)
+----------+----------+---------+
|manager_id|first_name|last_name|
+----------+----------+---------+
|       201|   Michael|Hartstein|
|       205|   Shelley|  Higgins|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       102|       Lex|  De Haan|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       124|     Kevin|  Mourgos|
|       124|     Kevin|  Mourgos|
+----------+----------+---------+

>>> empDf.alias("emp1").join(empDf.alias("emp2") , col("emp1.manager_id") == col("emp2.employee_id"), "inner").select(col("emp1.employee_id"), col("emp2.first_name"), col("emp2.last_name")).show(100)
+-----------+----------+---------+
|employee_id|first_name|last_name|
+-----------+----------+---------+
|        202|   Michael|Hartstein|
|        206|   Shelley|  Higgins|
|        124|    Steven|     King|
|        123|    Steven|     King|
|        122|    Steven|     King|
|        121|    Steven|     King|
|        120|    Steven|     King|
|        114|    Steven|     King|
|        102|    Steven|     King|
|        101|    Steven|     King|
|        201|    Steven|     King|
|        108|     Neena|  Kochhar|
|        205|     Neena|  Kochhar|
|        204|     Neena|  Kochhar|
|        203|     Neena|  Kochhar|
|        200|     Neena|  Kochhar|
|        103|       Lex|  De Haan|
|        107| Alexander|   Hunold|
|        106| Alexander|   Hunold|
|        105| Alexander|   Hunold|
|        104| Alexander|   Hunold|
|        113|     Nancy|Greenberg|
|        112|     Nancy|Greenberg|
|        111|     Nancy|Greenberg|
|        110|     Nancy|Greenberg|
|        109|     Nancy|Greenberg|
|        119|       Den| Raphaely|
|        118|       Den| Raphaely|
|        117|       Den| Raphaely|
|        116|       Den| Raphaely|
|        115|       Den| Raphaely|
|        128|   Matthew|    Weiss|
|        127|   Matthew|    Weiss|
|        126|   Matthew|    Weiss|
|        125|   Matthew|    Weiss|
|        132|      Adam|    Fripp|
|        131|      Adam|    Fripp|
|        130|      Adam|    Fripp|
|        129|      Adam|    Fripp|
|        136|     Payam| Kaufling|
|        135|     Payam| Kaufling|
|        134|     Payam| Kaufling|
|        133|     Payam| Kaufling|
|        140|    Shanta|  Vollman|
|        139|    Shanta|  Vollman|
|        138|    Shanta|  Vollman|
|        137|    Shanta|  Vollman|
|        199|     Kevin|  Mourgos|
|        198|     Kevin|  Mourgos|
+-----------+----------+---------+

>>> empDf.alias("emp1").select(col("emp1.manager_id").distinct()).join(empDf.alias("emp2") , col("emp1.manager_id") == col("emp2.employee_id"), "inner").select(col("emp1.employee_id"), col("emp2.first_name"), col("emp2.last_name")).show(100)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: 'Column' object is not callable
>>> empDf.alias("emp1").join(empDf.alias("emp2") , col("emp1.manager_id") == col("emp2.employee_id"), "inner").select(col("emp1.manager_id"), col("emp2.first_name"), col("emp2.last_name")).show(100)
+----------+----------+---------+
|manager_id|first_name|last_name|
+----------+----------+---------+
|       201|   Michael|Hartstein|
|       205|   Shelley|  Higgins|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       100|    Steven|     King|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       101|     Neena|  Kochhar|
|       102|       Lex|  De Haan|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       103| Alexander|   Hunold|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       108|     Nancy|Greenberg|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       114|       Den| Raphaely|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       120|   Matthew|    Weiss|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       121|      Adam|    Fripp|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       122|     Payam| Kaufling|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       123|    Shanta|  Vollman|
|       124|     Kevin|  Mourgos|
|       124|     Kevin|  Mourgos|
+----------+----------+---------+

>>> empDf.alias("emp1").join(empDf.alias("emp2") , col("emp1.manager_id") == col("emp2.employee_id"), "inner").select(col("emp1.manager_id"), col("emp2.first_name"), col("emp2.last_name")).dropDuplicates().show(100)
+----------+----------+---------+
|manager_id|first_name|last_name|
+----------+----------+---------+
|       122|     Payam| Kaufling|
|       101|     Neena|  Kochhar|
|       100|    Steven|     King|
|       205|   Shelley|  Higgins|
|       114|       Den| Raphaely|
|       103| Alexander|   Hunold|
|       124|     Kevin|  Mourgos|
|       120|   Matthew|    Weiss|
|       123|    Shanta|  Vollman|
|       121|      Adam|    Fripp|
|       108|     Nancy|Greenberg|
|       102|       Lex|  De Haan|
|       201|   Michael|Hartstein|
+----------+----------+---------+

>>> empDf.join(deptDf, (empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID) & (deptDf.LOCATION_ID == 1700), "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_NAME).show()
+-----------+-------------+---------------+
|EMPLOYEE_ID|DEPARTMENT_ID|DEPARTMENT_NAME|
+-----------+-------------+---------------+
|        200|           10| Administration|
|        119|           30|     Purchasing|
|        118|           30|     Purchasing|
|        117|           30|     Purchasing|
|        116|           30|     Purchasing|
|        115|           30|     Purchasing|
|        114|           30|     Purchasing|
|        102|           90|      Executive|
|        101|           90|      Executive|
|        100|           90|      Executive|
|        113|          100|        Finance|
|        112|          100|        Finance|
|        111|          100|        Finance|
|        110|          100|        Finance|
|        109|          100|        Finance|
|        108|          100|        Finance|
|        206|          110|     Accounting|
|        205|          110|     Accounting|
+-----------+-------------+---------------+

>>> from pyspark.sql.types import StructType,StructField, StringType, IntegerType
>>> location_data = [(1700, "INDIA"), (1800, "USA")]
>>> schema = StructType([ StructField("LOCATION_ID",IntegerType(),True), StructField("LOCATION_NAME",StringType(),True) ])
>>> locDf = spark.createDataFrame(data=location_data,schema=schema)
>>> locDf.printSchema()
root
 |-- LOCATION_ID: integer (nullable = true)
 |-- LOCATION_NAME: string (nullable = true)

>>> locDf.show()
+-----------+-------------+                                                     
|LOCATION_ID|LOCATION_NAME|
+-----------+-------------+
|       1700|        INDIA|
|       1800|          USA|
+-----------+-------------+

>>> empDf.join(deptDf, (empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID) & (deptDf.LOCATION_ID == 1700), "inner").join(locDf, deptDf.LOCATION_ID == locDf.LOCATION_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_NAME, locDf.LOCATION_NAME).show()
+-----------+-------------+---------------+-------------+
|EMPLOYEE_ID|DEPARTMENT_ID|DEPARTMENT_NAME|LOCATION_NAME|
+-----------+-------------+---------------+-------------+
|        205|          110|     Accounting|        INDIA|
|        206|          110|     Accounting|        INDIA|
|        108|          100|        Finance|        INDIA|
|        109|          100|        Finance|        INDIA|
|        110|          100|        Finance|        INDIA|
|        111|          100|        Finance|        INDIA|
|        112|          100|        Finance|        INDIA|
|        113|          100|        Finance|        INDIA|
|        100|           90|      Executive|        INDIA|
|        101|           90|      Executive|        INDIA|
|        102|           90|      Executive|        INDIA|
|        114|           30|     Purchasing|        INDIA|
|        115|           30|     Purchasing|        INDIA|
|        116|           30|     Purchasing|        INDIA|
|        117|           30|     Purchasing|        INDIA|
|        118|           30|     Purchasing|        INDIA|
|        119|           30|     Purchasing|        INDIA|
|        200|           10| Administration|        INDIA|
+-----------+-------------+---------------+-------------+

>>> def upperCase(in_str):
...     out_str = in_str.upper()
...     return out_str
... 
>>> print(upperCase("hello"))
HELLO
>>> upperCaseUDF = udf(lambda z : upperCase(z) , StringType())
>>> empDf.select(col("EMPLOYEE_ID") , col("FIRST_NAME"), col("LAST_NAME"), upperCaseUDF(col("FIRST_NAME")), upperCaseUDF(col("LAST_NAME"))).show()
+-----------+----------+---------+--------------------+-------------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|<lambda>(FIRST_NAME)|<lambda>(LAST_NAME)|
+-----------+----------+---------+--------------------+-------------------+
|        198|    Donald| OConnell|              DONALD|           OCONNELL|
|        199|   Douglas|    Grant|             DOUGLAS|              GRANT|
|        200|  Jennifer|   Whalen|            JENNIFER|             WHALEN|
|        201|   Michael|Hartstein|             MICHAEL|          HARTSTEIN|
|        202|       Pat|      Fay|                 PAT|                FAY|
|        203|     Susan|   Mavris|               SUSAN|             MAVRIS|
|        204|   Hermann|     Baer|             HERMANN|               BAER|
|        205|   Shelley|  Higgins|             SHELLEY|            HIGGINS|
|        206|   William|    Gietz|             WILLIAM|              GIETZ|
|        100|    Steven|     King|              STEVEN|               KING|
|        101|     Neena|  Kochhar|               NEENA|            KOCHHAR|
|        102|       Lex|  De Haan|                 LEX|            DE HAAN|
|        103| Alexander|   Hunold|           ALEXANDER|             HUNOLD|
|        104|     Bruce|    Ernst|               BRUCE|              ERNST|
|        105|     David|   Austin|               DAVID|             AUSTIN|
|        106|     Valli|Pataballa|               VALLI|          PATABALLA|
|        107|     Diana|  Lorentz|               DIANA|            LORENTZ|
|        108|     Nancy|Greenberg|               NANCY|          GREENBERG|
|        109|    Daniel|   Faviet|              DANIEL|             FAVIET|
|        110|      John|     Chen|                JOHN|               CHEN|
+-----------+----------+---------+--------------------+-------------------+
only showing top 20 rows

>>> @udf(returnType=StringType())
... def upperCaseNew(in_str):
...     out_str = in_str.upper()
...     return out_str
... 
>>> empDf.select(col("EMPLOYEE_ID") , col("FIRST_NAME"), col("LAST_NAME"), upperCaseNew(col("FIRST_NAME")), upperCaseNew(col("LAST_NAME"))).show()
+-----------+----------+---------+------------------------+-----------------------+
|EMPLOYEE_ID|FIRST_NAME|LAST_NAME|upperCaseNew(FIRST_NAME)|upperCaseNew(LAST_NAME)|
+-----------+----------+---------+------------------------+-----------------------+
|        198|    Donald| OConnell|                  DONALD|               OCONNELL|
|        199|   Douglas|    Grant|                 DOUGLAS|                  GRANT|
|        200|  Jennifer|   Whalen|                JENNIFER|                 WHALEN|
|        201|   Michael|Hartstein|                 MICHAEL|              HARTSTEIN|
|        202|       Pat|      Fay|                     PAT|                    FAY|
|        203|     Susan|   Mavris|                   SUSAN|                 MAVRIS|
|        204|   Hermann|     Baer|                 HERMANN|                   BAER|
|        205|   Shelley|  Higgins|                 SHELLEY|                HIGGINS|
|        206|   William|    Gietz|                 WILLIAM|                  GIETZ|
|        100|    Steven|     King|                  STEVEN|                   KING|
|        101|     Neena|  Kochhar|                   NEENA|                KOCHHAR|
|        102|       Lex|  De Haan|                     LEX|                DE HAAN|
|        103| Alexander|   Hunold|               ALEXANDER|                 HUNOLD|
|        104|     Bruce|    Ernst|                   BRUCE|                  ERNST|
|        105|     David|   Austin|                   DAVID|                 AUSTIN|
|        106|     Valli|Pataballa|                   VALLI|              PATABALLA|
|        107|     Diana|  Lorentz|                   DIANA|                LORENTZ|
|        108|     Nancy|Greenberg|                   NANCY|              GREENBERG|
|        109|    Daniel|   Faviet|                  DANIEL|                 FAVIET|
|        110|      John|     Chen|                    JOHN|                   CHEN|
+-----------+----------+---------+------------------------+-----------------------+

>>> from pyspark.sql.window import Window
>>> windowSpec = Window.partitionBy("DEPARTMENT_ID").orderBy("SALARY")
>>> empDf.withColumn("salary_rank", rank().over(windowSpec)).select("DEPARTMENT_ID","SALARY","salary_rank").show(100)
+-------------+------+-----------+
|DEPARTMENT_ID|SALARY|salary_rank|
+-------------+------+-----------+
|           10|  4400|          1|
|           20|  6000|          1|
|           20| 13000|          2|
|           30|  2500|          1|
|           30|  2600|          2|
|           30|  2800|          3|
|           30|  2900|          4|
|           30|  3100|          5|
|           30| 11000|          6|
|           40|  6500|          1|
|           50|  2100|          1|
|           50|  2200|          2|
|           50|  2200|          2|
|           50|  2400|          4|
|           50|  2400|          4|
|           50|  2500|          6|
|           50|  2500|          6|
|           50|  2600|          8|
|           50|  2600|          8|
|           50|  2700|         10|
|           50|  2700|         10|
|           50|  2800|         12|
|           50|  2900|         13|
|           50|  3200|         14|
|           50|  3200|         14|
|           50|  3300|         16|
|           50|  3300|         16|
|           50|  3600|         18|
|           50|  5800|         19|
|           50|  6500|         20|
|           50|  7900|         21|
|           50|  8000|         22|
|           50|  8200|         23|
|           60|  4200|          1|
|           60|  4800|          2|
|           60|  4800|          2|
|           60|  6000|          4|
|           60|  9000|          5|
|           70| 10000|          1|
|           90| 17000|          1|
|           90| 17000|          1|
|           90| 24000|          3|
|          100|  6900|          1|
|          100|  7700|          2|
|          100|  7800|          3|
|          100|  8200|          4|
|          100|  9000|          5|
|          100| 12008|          6|
|          110|  8300|          1|
|          110| 12008|          2|
+-------------+------+-----------+

>>> windowSpec = Window.partitionBy("DEPARTMENT_ID").orderBy(col("SALARY").desc())
>>> empDf.withColumn("salary_rank", rank().over(windowSpec)).select("DEPARTMENT_ID","SALARY","salary_rank").show(100)
+-------------+------+-----------+
|DEPARTMENT_ID|SALARY|salary_rank|
+-------------+------+-----------+
|           10|  4400|          1|
|           20| 13000|          1|
|           20|  6000|          2|
|           30| 11000|          1|
|           30|  3100|          2|
|           30|  2900|          3|
|           30|  2800|          4|
|           30|  2600|          5|
|           30|  2500|          6|
|           40|  6500|          1|
|           50|  8200|          1|
|           50|  8000|          2|
|           50|  7900|          3|
|           50|  6500|          4|
|           50|  5800|          5|
|           50|  3600|          6|
|           50|  3300|          7|
|           50|  3300|          7|
|           50|  3200|          9|
|           50|  3200|          9|
|           50|  2900|         11|
|           50|  2800|         12|
|           50|  2700|         13|
|           50|  2700|         13|
|           50|  2600|         15|
|           50|  2600|         15|
|           50|  2500|         17|
|           50|  2500|         17|
|           50|  2400|         19|
|           50|  2400|         19|
|           50|  2200|         21|
|           50|  2200|         21|
|           50|  2100|         23|
|           60|  9000|          1|
|           60|  6000|          2|
|           60|  4800|          3|
|           60|  4800|          3|
|           60|  4200|          5|
|           70| 10000|          1|
|           90| 24000|          1|
|           90| 17000|          2|
|           90| 17000|          2|
|          100| 12008|          1|
|          100|  9000|          2|
|          100|  8200|          3|
|          100|  7800|          4|
|          100|  7700|          5|
|          100|  6900|          6|
|          110| 12008|          1|
|          110|  8300|          2|
+-------------+------+-----------+

>>> windowSpec = Window.partitionBy("DEPARTMENT_ID").orderBy(col("SALARY").desc())
>>> empDf.withColumn("SUM", sum("SALARY").over(windowSpec)).select("DEPARTMENT_ID","SALARY","SUM").show(100)
+-------------+------+-----+
|DEPARTMENT_ID|SALARY|  SUM|
+-------------+------+-----+
|           10|  4400| 4400|
|           20| 13000|13000|
|           20|  6000|19000|
|           30| 11000|11000|
|           30|  3100|14100|
|           30|  2900|17000|
|           30|  2800|19800|
|           30|  2600|22400|
|           30|  2500|24900|
|           40|  6500| 6500|
|           50|  8200| 8200|
|           50|  8000|16200|
|           50|  7900|24100|
|           50|  6500|30600|
|           50|  5800|36400|
|           50|  3600|40000|
|           50|  3300|46600|
|           50|  3300|46600|
|           50|  3200|53000|
|           50|  3200|53000|
|           50|  2900|55900|
|           50|  2800|58700|
|           50|  2700|64100|
|           50|  2700|64100|
|           50|  2600|69300|
|           50|  2600|69300|
|           50|  2500|74300|
|           50|  2500|74300|
|           50|  2400|79100|
|           50|  2400|79100|
|           50|  2200|83500|
|           50|  2200|83500|
|           50|  2100|85600|
|           60|  9000| 9000|
|           60|  6000|15000|
|           60|  4800|24600|
|           60|  4800|24600|
|           60|  4200|28800|
|           70| 10000|10000|
|           90| 24000|24000|
|           90| 17000|58000|
|           90| 17000|58000|
|          100| 12008|12008|
|          100|  9000|21008|
|          100|  8200|29208|
|          100|  7800|37008|
|          100|  7700|44708|
|          100|  6900|51608|
|          110| 12008|12008|
|          110|  8300|20308|
+-------------+------+-----+

>>> windowSpec = Window.partitionBy("DEPARTMENT_ID")
>>> empDf.withColumn("SUM", sum("SALARY").over(windowSpec)).select("DEPARTMENT_ID","SALARY","SUM").show(100)
+-------------+------+-----+
|DEPARTMENT_ID|SALARY|  SUM|
+-------------+------+-----+
|           10|  4400| 4400|
|           20| 13000|19000|
|           20|  6000|19000|
|           30| 11000|24900|
|           30|  3100|24900|
|           30|  2900|24900|
|           30|  2800|24900|
|           30|  2600|24900|
|           30|  2500|24900|
|           40|  6500| 6500|
|           50|  2600|85600|
|           50|  2600|85600|
|           50|  8000|85600|
|           50|  8200|85600|
|           50|  7900|85600|
|           50|  6500|85600|
|           50|  5800|85600|
|           50|  3200|85600|
|           50|  2700|85600|
|           50|  2400|85600|
|           50|  2200|85600|
|           50|  3300|85600|
|           50|  2800|85600|
|           50|  2500|85600|
|           50|  2100|85600|
|           50|  3300|85600|
|           50|  2900|85600|
|           50|  2400|85600|
|           50|  2200|85600|
|           50|  3600|85600|
|           50|  3200|85600|
|           50|  2700|85600|
|           50|  2500|85600|
|           60|  9000|28800|
|           60|  6000|28800|
|           60|  4800|28800|
|           60|  4800|28800|
|           60|  4200|28800|
|           70| 10000|10000|
|           90| 24000|58000|
|           90| 17000|58000|
|           90| 17000|58000|
|          100| 12008|51608|
|          100|  9000|51608|
|          100|  8200|51608|
|          100|  7700|51608|
|          100|  7800|51608|
|          100|  6900|51608|
|          110| 12008|20308|
|          110|  8300|20308|
+-------------+------+-----+

>>> from pyspark.sql.functions import *
>>> spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)
>>> empDf.join(broadcast(deptDf), empDf.DEPARTMENT_ID == deptDf.DEPARTMENT_ID, "inner").select(empDf.EMPLOYEE_ID, empDf.DEPARTMENT_ID, deptDf.DEPARTMENT_NAME).show(100)
+-----------+-------------+----------------+
|EMPLOYEE_ID|DEPARTMENT_ID| DEPARTMENT_NAME|
+-----------+-------------+----------------+
|        198|           50|        Shipping|
|        199|           50|        Shipping|
|        200|           10|  Administration|
|        201|           20|       Marketing|
|        202|           20|       Marketing|
|        203|           40| Human Resources|
|        204|           70|Public Relations|
|        205|          110|      Accounting|
|        206|          110|      Accounting|
|        100|           90|       Executive|
|        101|           90|       Executive|
|        102|           90|       Executive|
|        103|           60|              IT|
|        104|           60|              IT|
|        105|           60|              IT|
|        106|           60|              IT|
|        107|           60|              IT|
|        108|          100|         Finance|
|        109|          100|         Finance|
|        110|          100|         Finance|
|        111|          100|         Finance|
|        112|          100|         Finance|
|        113|          100|         Finance|
|        114|           30|      Purchasing|
|        115|           30|      Purchasing|
|        116|           30|      Purchasing|
|        117|           30|      Purchasing|
|        118|           30|      Purchasing|
|        119|           30|      Purchasing|
|        120|           50|        Shipping|
|        121|           50|        Shipping|
|        122|           50|        Shipping|
|        123|           50|        Shipping|
|        124|           50|        Shipping|
|        125|           50|        Shipping|
|        126|           50|        Shipping|
|        127|           50|        Shipping|
|        128|           50|        Shipping|
|        129|           50|        Shipping|
|        130|           50|        Shipping|
|        131|           50|        Shipping|
|        132|           50|        Shipping|
|        133|           50|        Shipping|
|        134|           50|        Shipping|
|        135|           50|        Shipping|
|        136|           50|        Shipping|
|        137|           50|        Shipping|
|        138|           50|        Shipping|
|        139|           50|        Shipping|
|        140|           50|        Shipping|
+-----------+-------------+----------------+

>>> resultDf.write.mode("overwrite").option("header",True).save("/output/result")
>>> resultDf.write.mode("overwrite").option("header",True).format("csv").save("/output/result")
>>> resultDf.write.mode("append").option("header",True).format("csv").save("/output/result")


>>> resultDf.write.mode("overwrite").partitionBy("DEPARTMENT_NAME").option("header",True).format("csv").save("/output/result")
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Accounting
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Administration
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Executive
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Finance
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Human Resources
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=IT
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Marketing
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Public Relations
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Purchasing
drwxr-xr-x   - abc supergroup          0 2023-01-14 11:33 /output/result/DEPARTMENT_NAME=Shipping
-rw-r--r--   1 abc supergroup          0 2023-01-14 11:33 /output/result/_SUCCESS

>>> empDf.rdd.getNumPartitions()
1
>>> deptDf.rdd.getNumPartitions()
1
>>> resultDf.rdd.getNumPartitions()
1
>>> resultDf.repartition(10)
DataFrame[EMPLOYEE_ID: int, DEPARTMENT_ID: int, DEPARTMENT_NAME: string]
>>> resultDf.rdd.getNumPartitions()
1
>>> newDf = resultDf.repartition(10)
>>> newDf.rdd.getNumPartitions()
10
>>> df1 = newDf.repartition(2)
>>> df1.rdd.getNumPartitions()
2
>>> newDf.rdd.getNumPartitions()
10
>>> df2 = newDf.coalesce(20)
>>> df2.rdd.getNumPartitions()
10
>>> df3 = newDf.coalesce(5)
>>> df3.rdd.getNumPartitions()
5

>>> resultDf.coalesce(1).write.mode("overwrite").option("header",True).format("csv").save("/output/result")

>>> jsonDf = spark.read.json("/input_data/jsonexample.json")
>>> jsonDf.show()
+------------+----+-----+-------+
|      Array1|Num1|Text1|  Text2|
+------------+----+-----+-------+
|   [7, 8, 9]| 5.0|Hello|GoodBye|
|[70, 88, 91]| 6.5| This|   That|
|   [1, 2, 3]| 2.0|  Yes|     No|
+------------+----+-----+-------+

>>> jsonDf.printSchema()
root
 |-- Array1: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- Num1: double (nullable = true)
 |-- Text1: string (nullable = true)
 |-- Text2: string (nullable = true)

>>> jsonDf.select(jsonDf.Text1, jsonDf.Array1).show()
+-----+------------+
|Text1|      Array1|
+-----+------------+
|Hello|   [7, 8, 9]|
| This|[70, 88, 91]|
|  Yes|   [1, 2, 3]|
+-----+------------+

>>> jsonDf.select(jsonDf.Text1, jsonDf.Array1[2]).show()
+-----+---------+
|Text1|Array1[2]|
+-----+---------+
|Hello|        9|
| This|       91|
|  Yes|        3|
+-----+---------+

>>> jsonDf.select(jsonDf.Text1, explode(jsonDf.Array1)).show()
+-----+---+
|Text1|col|
+-----+---+
|Hello|  7|
|Hello|  8|
|Hello|  9|
| This| 70|
| This| 88|
| This| 91|
|  Yes|  1|
|  Yes|  2|
|  Yes|  3|
+-----+---+
