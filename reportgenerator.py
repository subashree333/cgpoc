import config
import findspark
from config import Config
import configparser

from pyspark.sql.functions import *

#from tab import config

findspark.init()
from configparser import ConfigParser
import os


from pyspark.sql import SparkSession
from pyspark.sql import *
def main():



    spark = SparkSession.builder.appName('table').config("spark.jars","C:\installers\drivers\postgresql-42.6.0.jar").getOrCreate()


    properties = {

    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "postgres"
    #"url":"jdbc:postgresql://localhost:5432/postgres"
}

    url = "jdbc:postgresql://localhost:5432/postgres"
    #customer1=config.get('parquet','customerpath')
    customerpath='C:/Users/subavija/Desktop/parquets/cust.parquet/part-00000-52f85d8f-182a-435b-8e9c-08cd1bb6a5fb-c000.snappy.parquet'
    cust1 = spark.read.parquet(customerpath)
    itempath='C:/Users/subavija/Desktop/parquets/items.parquet/part-00000-a7378c37-bf4e-4ebb-a421-8a58d19805f9-c000.snappy.parquet'
    item1 = spark.read.parquet(itempath)
    orderdetailspath='C:/Users/subavija/Desktop/parquets/order_details.parquet/part-00000-fad39e2f-600e-4db8-9eb2-dd627e70d9a7-c000.snappy.parquet'
    orderdetails1= spark.read.parquet(orderdetailspath)
    orderspath='C:/Users/subavija/Desktop/parquets/orders.parquet/part-00000-b7ad9f0f-5817-4839-b210-58254036f1dd-c000.snappy.parquet'
    orders1 = spark.read.parquet(orderspath)
    salespersonpath='C:/Users/subavija/Desktop/parquets/salesperson.parquet/part-00000-57fea39a-f3a6-4554-b498-45c3c961e9f9-c000.snappy.parquet'
    salesperson1 = spark.read.parquet(salespersonpath)
    shiptopath='C:/Users/subavija/Desktop/parquets/ship_to.parquet/part-00000-7f294d27-9237-4dda-9a9f-57958add6490-c000.snappy.parquet'
    shipto1 = spark.read.parquet(shiptopath)
    #print(cust1.show())
    #print(item1.show())
    #print(orderdetails1.show())
    #print(orders1.show())
    #print(salesperson1.show())
    #print(shipto1.show())
    cust1.createOrReplaceTempView("customertemp")
    #cust1.show()
    item1.createOrReplaceTempView("itemtemp")
    #item1.show()
    orderdetails1.createOrReplaceTempView("orderdetailstemp")
    #orderdetails1.show()
    orders1.createOrReplaceTempView("orderstemp")
    #orders1.show()
    salesperson1.createOrReplaceTempView("salespersontemp")
    #salesperson1.show()
    shipto1.createOrReplaceTempView("shiptotemp")
    #shipto1.show()
    #itempath.createOrReplaceTempView("item1")
    #orderdetailspath.createOrReplaceTempView("orderdetails1")
    #orderspath.createOrReplaceTempView("orders1")
    #salespersonpath.createOrReplaceTempView("salesperson1")
    #shiptopath.createOrReplaceTempView("shipto1")
    query1 = '''select c.cust_id,c.cust_name, count(od.ORDER_ID) as sums from customertemp c  
left join orderstemp rd on (c.CUST_ID= rd.cust_id )
left join orderdetailstemp od on ( rd.ORDER_ID = od. ORDER_ID)
left join itemtemp i on (od.ITEM_ID = i.ITEM_ID)
group by c.cust_id,c.cust_name''';
    query11 = spark.sql(query1).withColumn("current_date", current_date())
    #query11.show()
    #query11.write.jdbc(url=url, table ='table1', mode="overwrite", properties=properties)
    #query11.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/first_table/')

    query2 = '''select cust_id , cust_name,sum( ITEM_QUANTITY*DETAIL_UNIT_PRICE) total_order from (
select c.cust_id,c.cust_name, od.ITEM_ID, od.ITEM_QUANTITY, od.DETAIL_UNIT_PRICE  from customertemp c  
left join orderstemp rd on (c.CUST_ID= rd.cust_id )
left join orderdetailstemp od on ( rd.ORDER_ID = od. ORDER_ID)
left join itemtemp i on (od.ITEM_ID = i.ITEM_ID) ) p
group by cust_id , cust_name''';
    query22 = spark.sql(query2).withColumn("current_date", current_date())
    #query22.show()
    #query22.write.jdbc(url=url, table ='table2', mode="overwrite", properties=properties)
    #query22.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/second_table')

    query3 = '''select i.ITEM_ID,i.ITEM_DESCRIPTION , sum(ITEM_QUANTITY) as sums from itemtemp i
left join orderdetailstemp od on (od.ITEM_ID = i.ITEM_ID)
group by i.ITEM_ID,i.ITEM_DESCRIPTION  order by sums desc''';
    query33 = spark.sql(query3).withColumn("current_date", current_date())
    #query33.show()
    #query33.write.jdbc(url=url, table ='table3', mode="overwrite", properties=properties)
    #query33.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/third_table/')

    query4 = '''select i.CATEGORY , sum(od.ITEM_QUANTITY) as sums from itemtemp i
left join orderdetailstemp od on (od.ITEM_ID = i.ITEM_ID)
group by i.CATEGORY  order by sum(ITEM_QUANTITY) desc'''
    query44 = spark.sql(query4).withColumn("current_date", current_date())
    #query44.show()
    #query44.write.jdbc(url=url, table ='table4', mode="overwrite", properties=properties)
    #query44.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/forth_table/')

    query5 = '''select i.ITEM_ID,i.ITEM_DESCRIPTION , sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as sums from itemtemp i
left join orderdetailstemp od on (od.ITEM_ID = i.ITEM_ID)
group by i.ITEM_ID,i.ITEM_DESCRIPTION  order by sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) desc'''
    query55 = spark.sql(query5).withColumn("current_date", current_date())
    #query55.show()
    #query55.write.jdbc(url=url, table ='table5', mode="overwrite", properties=properties)
    #query55.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/fifth_table/')

    query6 = '''select i.CATEGORY , sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as sums from itemtemp i
left join orderdetailstemp od on (od.ITEM_ID = i.ITEM_ID)
group by i.CATEGORY  order by sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) desc'''
    query66 = spark.sql(query6).withColumn("current_date", current_date())
    #query66.show()
    #query66.write.jdbc(url=url, table ='table6', mode="overwrite", properties=properties)
    #query66.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/sixth_table/')

    query7 = '''select s.SALESMAN_ID,  sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) tot_ord_amt,
cast(sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) * 0.1 AS DECIMAL(10,2)) as sums
from salespersontemp s  
left join customertemp c on (s.SALESMAN_ID = c.SALESMAN_ID)
left join orderstemp rd on (c.CUST_ID= rd.cust_id )
left join orderdetailstemp od on ( rd.ORDER_ID = od.ORDER_ID)
group by s.SALESMAN_ID'''
    query77 = spark.sql(query7).withColumn("current_date", current_date())
    #query77.show()
    #query77.write.jdbc(url=url, table ='table7', mode="overwrite", properties=properties)
    query77.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/seventh_table/')

    query8 = '''select i.ITEM_ID,i.ITEM_DESCRIPTION from itemtemp i where 
i.ITEM_ID not in (select item_id from orderdetailstemp od )'''
    query88 = spark.sql(query8).withColumn("current_date", current_date())
    #query88.show()
    #query88.write.jdbc(url=url, table ='table8', mode="overwrite", properties=properties)
    query88.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/eigth_table/')

    query9 = '''select c.CUST_ID, c.cust_name from customertemp c where 
c.CUST_ID not in (select s.CUST_ID  from shiptotemp s)'''
    query99 = spark.sql(query9).withColumn("current_date", current_date())
    #query99.show()
    #query99.write.jdbc(url=url, table ='table9', mode="overwrite", properties=properties)
    query99.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/ninth_table/')

    query10 = '''select c.CUST_ID, c.cust_name from customertemp c
join shiptotemp s  on (s.cust_id =c.cust_id)  where length(s.POSTAL_CODE) !=6''';
    query100 = spark.sql(query10).withColumn('current_date', current_date())
    #query100.show()
    #query100.write.jdbc(url=url, table ='table10', mode="overwrite", properties=properties)
    query100.write.partitionBy('current_date').mode('overwrite').parquet('C:/Users/Newparquet/tenth_table/')

    




if __name__ == '__main__':
    main()

