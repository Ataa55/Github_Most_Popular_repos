## utils
from consts import *
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
# from sqlalchemy import create_engine, exc, text



def connect_to_db(postgres_cred):
    
    engine = create_engine(f'postgresql://{postgres_cred["USER"]}:{postgres_cred["PASSWORD"]}@{postgres_cred["HOST"]}:{postgres_cred["PORT"]}/{postgres_cred["db"]}')
    try:
        with engine.connect() as conn:             
            print(f"succefuly connected to {postgres_cred['db']} at host: {postgres_cred['HOST']}")
        return engine
        
    except exc.SQLAlchemyError as e:
        print(f"connection failed !")
        print(f"Error {e}")


def write_pdf_to_db(df, table_name, engine):
    
    try:
        print(f"start writing data to {table_name}")
        start_time  = time.time() 
        df.to_sql(name = table_name, con = engine, if_exists='append', index=False, method = "multi", chunksize=1000)
        end_time  = time.time() 
        print(f"Data of {len(df)} record Successfully written to the {table_name} table")
        print(f"the proccess toke {end_time - start_time} seconds")
    
    except exc.SQLAlchemyError as e:
        print(f"failed to write data to {table_name} table")
        print(f"Error: {e}")

    except ValueError as ve:
        print(f"ValueError occured during the write process")
        print(f"Error: {ve}")
    
    except Exception as ex:
        print(f"An expected error occured")
        print(f"Error: {ex}")
    

def finalize_things(engine):
    engine.dispose()
    print("engine disposed and connection closed successfully")

def write_sdf_to_postgres_db(df, db_cred_dict, db_table, mode):
    driver = "org.postgresql.Driver"
    postgres_connectio_url = f"jdbc:postgresql://postgres:{db_cred_dict['PORT']}/{db_cred_dict['db']}"
    
    try:
        df.write.format("jdbc")\
            .option("url", postgres_connectio_url) \
            .option("driver", driver) \
            .option("dbtable", db_table) \
            .option("user", db_cred_dict["USER"]) \
            .option("password", db_cred_dict["PASSWORD"]) \
            .mode(mode) \
            .save()
    except Exception as e:
        print(f"error while writing to db {e}")


def start_spark_session(spark_master, app_name, memory):
    jar_path = "/opt/spark/jars/postgresql-42.2.18.jar"
    spark = (SparkSession.builder.appName(app_name)
            .config("spark.master",spark_master)
            .config("spark.driver.memory",memory)
            .config("spark.executer.memory",memory)
            .config("spark.jars", jar_path)
            .getOrCreate())
    return spark





def check_if_schema_exists(spark, schema_name):
    #check if the provided schema exists or not
    
    database_count = spark.sql(f"show databases like '{schema_name}'").count()
    if database_count:
        return True
    else:
        return False

def check_if_table_exists(spark, schema_name, table_name):
    #first check if the provided schema exists or not, raise exception if not exist
    #then check if the table exists or not, also raise exception if not exists
    if check_if_schema_exists(spark, schema_name):
        spark.sql(f"USE {schema_name}")
        table_count = spark.sql(f"show tables like '{table_name}'").count()
            
        if table_count:
            return True
            
        else:
            return False
    else:
        raise Exception(f"schema: {schema_name} doesn't exist")
        
def get_drift_cols(df_source, df_target):
    #check if any column at the source missed in the target
    
    drift_cols = {}   
    # get the source and target fields
    source_fields = df_source.schema.fields
    target_fields = df_target.schema.fields

    # get any missed cols
    missed_fields = set(df_source.schema.fields) - set(df_target.schema.fields)

    # detect and flag the columns that dropped or added to the source 
    added_cols_to_the_source = [field for field in source_fields if field not in target_fields ]
    dropped_cols_at_the_source = [field for field in target_fields if field not in source_fields ]
    
    if added_cols_to_the_source or dropped_cols_at_the_source:
        for field in added_cols_to_the_source:
            drift_cols[str(field.name)] = [str(field.dataType)[:-6],"add"]
        for field in dropped_cols_at_the_source:
            drift_cols[str(field.name)] = [str(field.dataType)[:-6],"drop"]
    
            
        # drift_cols = {col: type_mapping.get(dtype.lower(), dtype.upper()) for col, dtype in drift_cols.items()}
        print(f"new actions at the source {list(drift_cols.keys())}")
        

    # elif len(source_fields) == len(target_fields):
    #     # flag this column as renamed if the source and target culomns are the same in number but there are changes
    #     possible_renamed_cols_at_the_source = [field for field in source_fields if field not in target_fields ]

    #     for field in possible_renamed_cols_at_the_source:
    #         drift_cols[str(field.name)] = [str(field.dataType)[:-6],"rename"]
                    
    #     # drift_cols = {col: type_mapping.get(dtype.lower(), dtype.upper()) for col, dtype in drift_cols.items()}
    #     print(f"possible renamed columns at the source {list(drift_cols.keys())}")
               
    else:
        print("no changes at the source schema")
        return None
    return drift_cols

def apply_hash(df_source, df_target, join_on):
    """ 
        this func uses md5 function to apply hash, 
        here we will add hash column to the source and target data frames
        returns those two data frames with hash column 
    """
    data_cols = [col for col in df_target.columns]
    hash_func = F.md5(F.concat_ws("|", *[F.col(col) for col in data_cols]))
    df_source = df_source.withColumn("hash_value", hash_func).alias("source")
    df_target = df_target.withColumn("hash_value", hash_func).alias("target")
    return df_source, df_target
    

def get_source_changes(spark, schema_name,source_table_name, target_table_name, join_keys):
    """
        this function takes two data frames and return dataframe 
        that contains any record changes (update, insert, delete)
    """
    # get the sourse and target data    
    df_source = spark.sql(f"select * from {schema_name}.{source_table_name}")
    df_target = spark.sql(f"select * from {schema_name}.{target_table_name}")
    # add hash column that saves hased value for the data so we can check an update
    data_cols= [col for col in df_target.columns]
    hashed_source, hashed_target = apply_hash(df_source, df_target, join_keys)

    # apply full outer join on the source and the target by the key column to detect any changes 
    join_statment = [hashed_source[join_key] == hashed_target[join_key] for join_key in join_keys]
    join_result = hashed_source.join(hashed_target, join_statment, "full")
    
    # filter conditions to select update, insert and delete records from the join result
    update_condition = f"source.hash_value != target.hash_value"
    
    insert_key_condition = "and ".join([f"target.{key} is null " for key in join_keys])
    insert_condition = f"{insert_key_condition} and target.hash_value is null"
    
    delete_key_condition = "and ".join([f"source.{key} is null " for key in join_keys])
    delete_condition = f"{delete_key_condition} and source.hash_value is null"

    ## filter and flag the changes
    
    # >> get updtas
    filter_updated_data = join_result.filter(update_condition).select("source.*")
    updated_data = filter_updated_data.withColumn("cdc_flag", F.lit("U"))
    
    # >> get inserts                                                     
    filter_inserted_data = join_result.filter(insert_condition).select("source.*")
    inserted_data = filter_inserted_data.withColumn("cdc_flag", F.lit("I"))
    
    # >> get deletes
    filter_deleted_data = join_result.filter(delete_condition).select("target.*")
    deleted_data = filter_deleted_data.withColumn("cdc_flag", F.lit("D"))

    # >> union all the changes into one dataframe 
    upserted_data = updated_data.unionByName(inserted_data).unionByName(deleted_data).select(data_cols+["cdc_flag"])
    
    return upserted_data,join_result

def upsert_datalake_target(spark, schema_name, source_table_name, target_table_name, key_columns, changes, upsert_flag):
    
    # check if the schema and tables are exists, raise exception if any not exist
    for table in [source_table_name, target_table_name]:
        if not check_if_table_exists(spark, schema_name, table):
            raise Exception(f"Table: {table} doesn't exist in the schema: {schema_name}")
            
    # get the sourse and target data    
    df_source = spark.sql(f"select * from {schema_name}.{source_table_name}")
    df_target = spark.sql(f"select * from {schema_name}.{target_table_name}")
    
    
    # get the delta and changes from the source and save it temporarely
    
    
    if not changes.count():
        return changes
        
    # merge the changes (updates, inserts, deletes) into the target
    changes.createOrReplaceTempView("changes")
    merge_condition = " and".join([f"{schema_name}.{target_table_name}.{key_column} = changes.{key_column}" for key_column in key_columns])
    
    try:       
        spark.sql(f"use database {schema_name}")

        merge_upserts_statment = f""" MERGE INTO {schema_name}.{target_table_name} 
                                      using (select * from changes) 
                                      on {merge_condition}
                                      WHEN MATCHED AND changes.{upsert_flag} = 'U' THEN UPDATE SET *
                                      WHEN MATCHED AND changes.{upsert_flag} = 'D' THEN DELETE
                                      WHEN NOT MATCHED THEN INSERT *
                                  """
        print("start merging")
        spark.sql(merge_upserts_statment)
        
    except Exception as e:
        print(f"some error happened while merging{e}") 

    # return columns_to_add, columns_to_drop
  
       
    

    