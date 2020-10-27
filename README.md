# miniProject2a

tools that will be used in this project are:
1.google cloud platform
2. spark
3. hive table


in the final result there will be 3 tables that are inserted into the hive table:
-datacovidefault.2019 (will enter the default database)
-RataRataRataAdditionCase result (will enter the userdb database)
-Add Case for Each Country (will enter the userdb database)

the workflow is:
-upload the owid-covid-data.csv file to hdfs and google cloud platform
-do a spark submit on the jar file that has been created in java

In the existing coding, it will use spark sql and there will be a save command in the hive table
