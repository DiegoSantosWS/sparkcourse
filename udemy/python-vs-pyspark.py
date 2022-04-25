# In[1]:

# import findspark
# findspark.init()

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PaySpark").getOrCreate()
spark

# In[2]:

data = [['tom', 10], ['nick', 15], ['juli',14]]

# create the pandas DataFrame
df = spark.createDataFrame(data, ['Name', 'Age'])

# In[3]:

df.show()

# In[4]:

df.toPandas()

# In[5]:
# show columns
df.columns


# In[6]:

df.count()


# In[7]:

#List students

path = "students.csv"
df = spark.read.csv(path, header=True)
df.toPandas()

# ## Aggregate Data
# In[8]
# ## Aggregate Data
df.groupBy("gender").agg({'math score':'mean'}).show()


# In[42]:

from pyspark.sql import functions as F
df.groupBy('gender').agg(F.min("math score"), F.max("math score"), F.avg("math score")).show()

# In[9]:

# Let's fetch the id of our dataframe we created above
df.rdd.id()

# In[10]:

# Even if we duplicate the dataframe, the ID remains the same
df2 = df
df2.rdd.id()

# In[11]:

df = df.withColumn('new_col', df['math score'] * 2)
df.rdd.id()


# ## Spark's Lazy Comuptation
# 
# What does that mean exactly?
# 
# As the name itself indicates its definition, lazy evaluation in Spark means that the execution will not start until it absolutuley HAS to. 
# 
# Let's look at an example. 
#In[12]:

df = df.withColumn('new_col', df['math score'] * 2)

#In[13]:

collect = df.collect()

#In[14]:

# Or this
print(df)
# %%
