#pip install kaggle


import kaggle
import psycopg2
import pandas as pd
import streamlit as st

# mkdir .kaggle ( creating directory)
# rm - remove existing directory ( rm -r ~/.kaggle)
# mv - (move the file from one location to another) - % mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
# cd - (will move into the directory) - cd .kaggle
# ls - (will list the contents in directory) - .kaggle % ls

#Here we are using kaggle api authenticate for using my kaggle key
kaggle.api.authenticate()

#kaggle api will download dataset and will unzip the file

#kaggle.api.dataset_download_files('ankitbansal06/retail-orders',path=".",unzip="True")

#pip install pandas

df = pd.read_csv('/Users/yokesh/Documents/Datascience/Project/Retail_Order/orders.csv')

df.head() #will display first 5 data in table

df.info() #will display column with datatypes


df.describe() 


df.isnull().sum() #checking whether we have any null values or not in table


df['Ship Mode'].unique() #checks the unique value in column


df['Ship Mode'].value_counts() # containing count of unique values


df.dropna(subset=['Ship Mode'], inplace=True) #drop the null value row from shipmode column


df.isnull().sum() #checking whether there is null or not null


#convert column name axis=0 ( index which is row), axis =1 ( which is column)



df.rename({'Order Id':'order_id','Order Date':'order_date','Ship Mode':'ship_mode','Segment':'segment','Country':'country','City':'city','State':'state',
          'Postal Code':'postal_code','Region':'region','Category':'category','Sub Category':'sub_category','Product Id':'product_id',
           'cost price':'cost_price','List Price':'list_price','Quantity':'quantity','Discount Percent':'discount_percent' },axis=1,inplace=True)


df.head() #will display first 5 data in table


df.insert(16,column="discount_price",value= df['list_price']-df['cost_price'])


#df.drop(columns='sale_price',inplace=True)


df.insert(17,column="sale_price",value= df['list_price'] * (1 - df['discount_percent'] / 100))

# selling price = list price * (1 - discount percent/100 )


df.head() #will display first 5 data in table


df.insert(18,column="profit",value= df['sale_price']- df['cost_price'])

df.head() #will display first 5 data in table

st.header("RETAIL ORDER ANALYSIS")
st.subheader("Retail Order")
df

# Splitting whole table in two First table orders
df_order = df[['order_id','segment','country','city','state','postal_code','region','product_id']]

# Second table order_details
df_order_details = df[['order_id','order_date','ship_mode','category','sub_category','product_id','cost_price','list_price','quantity','discount_percent',
                       'discount_price','sale_price','profit']]


# Connecting Postgresql

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="root@123",
    database="retail_order",
    port="5434")

cursor = mydb.cursor()

#Creating orders database 
create_query = '''create table if not exists orders(order_id int NOT NULL,
                   segment varchar(50),
                   country varchar(50),
                   city varchar(50),
                   state varchar(50),
                   postal_code varchar(50),
                   region varchar(50),
                   product_id varchar(100) NOT NULL,
                   PRIMARY KEY (order_id, product_id))'''
    
cursor.execute(create_query)

mydb.commit()

try:
    for index,row in df_order.iterrows():
    # SQL query to insert data (skipping the index column) # SQL query to insert data
        insert_query_order='''insert into orders(order_id,
                            segment,
                            country,
                            city,
                            state,
                            postal_code,
                            region,
                            product_id)values(%s,%s,%s,%s,%s,%s,%s,%s)'''
                
        values=(row['order_id'], # Values from the current row
                row['segment'],
                row['country'],
                row['city'],
                row['state'],
                row['postal_code'],
                row['region'],
                row['product_id'])
        
        # Execute the query with the values
        cursor.execute(insert_query_order,values)
        mydb.commit()

    print("Data successfully inserted into the orders table!")

except:
     print("Data not inserted into the orders table!")


mydb1= psycopg2.connect(
    host='localhost',
    user='postgres',
    password='root@123',
    database='retail_order',
    port='5434')

cursor1 =mydb1.cursor() #creates a cursor to interact with the database.

#creating Database order_details table

create_query1 = '''create table if not exists order_details(
                    order_id int NOT NULL,
                    order_date date,
                    ship_mode varchar(50),
                    category varchar(50),
                    sub_category varchar(50),
                    product_id varchar(100) not null,
                    cost_price float,
                    list_price float,
                    quantity int,
                    discount_percent int ,
                    discount_price int,
                    sale_price decimal(10,2),
                    profit decimal(10,2),
                    FOREIGN KEY (order_id, product_id)
                        REFERENCES orders(order_id, product_id))'''

cursor1.execute(create_query1)

mydb1.commit()

try:
    for index, row in df_order_details.iterrows():
        insert_query_order_details='''insert into order_details(
                                        order_id,
                                        order_date,
                                        ship_mode,
                                        category,
                                        sub_category,
                                        product_id,
                                        cost_price,
                                        list_price,
                                        quantity,
                                        discount_percent,
                                        discount_price,
                                        sale_price,
                                        profit)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(
            row['order_id'],
            row['order_date'],
            row['ship_mode'],
            row['category'],
            row['sub_category'],
            row['product_id'],
            row['cost_price'],
            row['list_price'],
            row['quantity'],
            row['discount_percent'],
            row['discount_price'],
            row['sale_price'],
            row['profit']
        )

        cursor1.execute(insert_query_order_details,values)
        mydb1.commit()
        
    print("Data successfully inserted into the orders table!")

except:
    print("Data not inserted into the orders table!")


# Connecting Streamlit to display our results

with st.sidebar:
    st.title("RETAIL ORDER ANALYSIS")
    st.header('Skills')
    st.caption("Python")
    st.caption("Data Cleaning")
    st.caption("Postgresql")

st.subheader("Data Analysis with SQL")

#Questions to be Answered
mydbStream= psycopg2.connect(
    host='localhost',
    user='postgres',
    password='root@123',
    database='retail_order',
    port='5434')

cursor = mydbStream.cursor()

Questions = st.selectbox("Select your Question",("1.Find top 10 highest revenue generating products",
                                                "2.Find the top 5 cities with the highest profit margins",
                                                "3.Calculate the total discount given for each category",
                                                "4.Find the average sale price per product category",
                                                "5.Find the region with the highest average sale price",
                                                "6.Find the total profit per category",
                                                "7.Identify the top 3 segments with the highest quantity of orders",
                                                "8.Determine the average discount percentage given per region",
                                                "9.Find the product category with the highest total profit",
                                                "10.Calculate the total revenue generated per year",
                                                "11.Calculate the total quantity sent through each ship mode",
                                                "12.Find the highest quantity sold sub category in category",
                                                "13.Join the order_date from order_details table to orders table",
                                                "14.Find which product_id and highest quantity sold in year 2023",
                                                "15.find total count of sub category in category",
                                                "16.Join region table to order_details and display data's of south region",
                                                "17.Find which date more orders are placed",
                                                "18.join both table into single table",
                                                "19.Find Cost price greater than 500",
                                                "20.Display important data from both table"))

if(Questions == "1.Find top 10 highest revenue generating products"):
    query = '''SELECT ship_mode,category,sub_category,product_id,SUM(sale_price * quantity) AS revenue FROM order_details
                GROUP BY ship_mode, category, sub_category, product_id
                ORDER BY revenue DESC LIMIT 10'''
    cursor.execute(query)
    mydb.commit()
    q1=cursor.fetchall()
    df=pd.DataFrame(q1,columns=["ship_mode","category","sub_category","product_id","revenue"])
    st.write(df)

elif(Questions == "2.Find the top 5 cities with the highest profit margins"):
    query1='''select distinct country,city,postal_code,profit from orders o
                join order_details s on o.product_id= s.product_id order by s.profit desc limit 5'''
    cursor.execute(query1)
    mydb.commit()
    q2=cursor.fetchall()
    df1=pd.DataFrame(q2,columns=["country","city","postal_code","profit"])
    st.write(df1)

elif(Questions == "3.Calculate the total discount given for each category"):
    query2='''select category,sum(discount_price) as total_discounts from order_details
                group by category'''
    cursor.execute(query2)
    mydb.commit()
    q3 = cursor.fetchall()
    df2 = pd.DataFrame(q3,columns=["category","total_discounts"])
    st.write(df2)

elif(Questions == "4.Find the average sale price per product category"):
    query3='''select category,avg(sale_price) as average_price from order_details
                group by category'''
    cursor.execute(query3)
    mydb.commit()
    q4=cursor.fetchall()
    df3 = pd.DataFrame(q4,columns=["category","average_price"])
    st.write(df3)

elif(Questions == "5.Find the region with the highest average sale price"):
    query4='''select o.region,avg(sale_price) as avg_sale_price from orders o 
              join order_details s on s.order_id = o.order_id
              group by region order by avg_sale_price desc limit 1'''
    cursor.execute(query4)
    mydb.commit()
    q5=cursor.fetchall()
    df4 = pd.DataFrame(q5,columns=["region","avg_sale_price"])
    st.write(df4)

elif(Questions == "6.Find the total profit per category"):
    query5='''select category,sum(profit) as total_profit from order_details
              group by category'''
    cursor.execute(query5)
    mydb.commit()
    q6=cursor.fetchall()
    df5 = pd.DataFrame(q6,columns=["region","avg_sale_price"])
    st.write(df5)

elif(Questions == "7.Identify the top 3 segments with the highest quantity of orders"):
    query6='''select o.segment, sum(s.quantity) as total_quantity from orders o
              join order_details s on s.order_id = o.order_id
              group by o.segment order by total_quantity desc limit 3'''
    cursor.execute(query6)
    mydb.commit()
    q7=cursor.fetchall()
    df6 = pd.DataFrame(q7,columns=["segment","total_quantity"])
    st.write(df6)

elif(Questions == "8.Determine the average discount percentage given per region"):
    query7='''select region, avg(discount_percent) as avg_dist_percent from orders o
              join order_details s on s.order_id = o.order_id
              group by region'''
    cursor.execute(query7)
    mydb.commit()
    q8=cursor.fetchall()
    df7 = pd.DataFrame(q8,columns=["region","avg_dist_percent"])
    st.write(df7)

elif(Questions == "9.Find the product category with the highest total profit"):
    query8='''select s.category,o.product_id,sum(profit) as total_profit from orders o
                join order_details s on s.product_id = o.product_id
                group by s.category,o.product_id order by total_profit desc limit 1'''
    cursor.execute(query8)
    mydb.commit()
    q9=cursor.fetchall()
    df8 = pd.DataFrame(q9,columns=["category","product_id","total_profit"])
    st.write(df8)

elif(Questions == "10.Calculate the total revenue generated per year"):
    query9='''select extract(year from order_date) as year,sum(profit) as total_profit from order_details
                group by year order by year'''
    cursor.execute(query9)
    mydb.commit()
    q10=cursor.fetchall()
    df9 = pd.DataFrame(q10,columns=["year","total_profit"])
    st.write(df9)

elif(Questions == "11.Calculate the total quantity sent through each ship mode"):
    query10='''select ship_mode,sum(quantity) as total_quantity from order_details
                group by ship_mode order by total_quantity'''
    cursor.execute(query10)
    mydb.commit()
    q11=cursor.fetchall()
    df10 = pd.DataFrame(q11,columns=["ship_mode","total_quantity"])
    st.write(df10)

elif(Questions == "12.Find the highest quantity sold sub category in category"):
    query11='''select category,sub_category,sum(quantity) as total_quantity from order_details
                group by category,sub_category
                order by total_quantity desc limit 1'''
    cursor.execute(query11)
    mydb.commit()
    q12=cursor.fetchall()
    df11 = pd.DataFrame(q12,columns=["category","sub_category","total_quantity"])
    st.write(df11)

elif(Questions == "13.Join the order_date from order_details table to orders table"):
    query12='''select  o.*,order_date from orders o
                join order_details s on s.order_id = o.order_id'''
    cursor.execute(query12)
    mydb.commit()
    q13=cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] 
    df12 = pd.DataFrame(q13,columns=columns)
    st.write(df12)

elif(Questions == "14.Find which product_id and highest quantity sold in year 2023"):
    query13='''select extract(year from order_date) as year,product_id,sum(quantity) as total_quantity from order_details
                group by year,product_id
                having extract(year from order_date)=2023
                order by year,total_quantity desc limit 1'''
    cursor.execute(query13)
    mydb.commit()
    q14=cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] 
    df13 = pd.DataFrame(q14,columns=columns)
    st.write(df13)

elif(Questions =="15.find total count of sub category in category"):
    query14='''select category,sub_category,count(sub_category) as total_count from order_details
                group by category,sub_category'''
    cursor.execute(query14)
    mydb.commit()
    q15=cursor.fetchall()
    df14=pd.DataFrame(q15,columns=["category","sub_category","total_count"])
    st.write(df14)


elif(Questions =="16.Join region table to order_details and display data's of south region"):
    query15='''select s.*,o.region from orders o
                join order_details s on s.order_id=o.order_id
                where o.region ='South';'''
    cursor.execute(query15)
    mydb.commit()
    q16=cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] 
    df15=pd.DataFrame(q16,columns=columns)
    st.write(df15)
    
elif(Questions =="17.Find which date more orders are placed"):
    query16='''select distinct order_date,count(order_date)as total_Orderdate from order_details
                group by order_date
                order by total_Orderdate desc limit 1'''
    cursor.execute(query16)
    mydb.commit()
    q17=cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] 
    df16=pd.DataFrame(q17,columns=columns)
    st.write(df16)

elif(Questions =="18.join both table into single table"):
    query17='''SELECT 
                o.order_id, 
                o.segment, 
                o.country, 
                o.city, 
                o.state, 
                o.postal_code, 
                o.region,
                o.product_id,
                s.order_date, 
                s.order_id AS order_id_details, 
                s.product_id AS product_id_details, 
                s.ship_mode, 
                s.category, 
                s.sub_category,
                s.quantity, 
                s.cost_price,
                s.list_price,
                s.discount_price, 
                s.sale_price, 
                s.profit  
                FROM orders o JOIN order_details s ON s.order_id = o.order_id'''
    cursor.execute(query17)
    mydb.commit()
    q18=cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] 
    df17=pd.DataFrame(q18,columns=columns)
    st.write(df17)

elif(Questions =="19.Find Cost price greater than 500"):
    query18='''select count(cost_price) as total_count from order_details
                where cost_price > 500'''
    cursor.execute(query18)
    mydb.commit()
    q19=cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] 
    df18=pd.DataFrame(q19,columns=columns)
    st.write(df18)

elif(Questions =="20.Display important data from both table"):
    query19='''select o.order_id,o.segment,o.country,o.city,o.state,o.postal_code,s.category,s.sub_category,s.product_id,s.cost_price,s.sale_price,s.profit from orders o
                join order_details s on s.order_id=o.order_id'''
    cursor.execute(query19)
    mydb.commit()
    q20=cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] 
    df19=pd.DataFrame(q20,columns=columns)
    st.write(df19)