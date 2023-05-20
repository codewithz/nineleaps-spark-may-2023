# nineleaps-spark-may-2023

# Define schema for JSON data
## --------------------------- TaxiBasesSchema for JSON ---------------------------------------------------
taxiBasesSchema = (
                    StructType
                    ([
                        StructField("License Number"         , StringType()    , True),
                        StructField("Entity Name"            , StringType()    , True),
                        StructField("Telephone Number"       , LongType()      , True),
                        StructField("SHL Endorsed"           , StringType()    , True),
                        StructField("Type of Base"           , StringType()    , True),

                        StructField("Address", 
                                        StructType
                                        ([
                                            StructField("Building"   , StringType(),   True),
                                            StructField("Street"     , StringType(),   True), 
                                            StructField("City"       , StringType(),   True), 
                                            StructField("State"      , StringType(),   True), 
                                            StructField("Postcode"   , StringType(),   True)
                                        ]),
                                    True
                                   ),
                        
                        StructField("GeoLocation", 
                                        StructType
                                        ([
                                            StructField("Latitude"   , StringType(),   True),
                                            StructField("Longitude"  , StringType(),   True), 
                                            StructField("Location"   , StringType(),   True)
                                        ]),
                                    True
                                   )  
                  ])
                )

##------------------- data from blog dataframe ------------

data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
       [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
       [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
       [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
       [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
       [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
      ]

schema= StructType([
    StructField("Id",IntegerType(),False),
    StructField("First",StringType(),False),
     StructField("Last",StringType(),False),
    StructField("URL",StringType(),False),
    StructField("Published",StringType(),False),
    StructField("Hits",IntegerType(),False),
    StructField("Campaigns",ArrayType(StringType()),False)
]
)

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

# ---------- San Fransisco Fire Calls Case Study ------------------------

fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
 StructField('UnitID', StringType(), True),
 StructField('IncidentNumber', IntegerType(), True),
 StructField('CallType', StringType(), True),
 StructField('CallDate', StringType(), True),
 StructField('WatchDate', StringType(), True),
 StructField('CallFinalDisposition', StringType(), True),
 StructField('AvailableDtTm', StringType(), True),
 StructField('Address', StringType(), True),
 StructField('City', StringType(), True),
 StructField('Zipcode', IntegerType(), True),
 StructField('Battalion', StringType(), True),
 StructField('StationArea', StringType(), True),
 StructField('Box', StringType(), True),
 StructField('OriginalPriority', StringType(), True),
 StructField('Priority', StringType(), True),
 StructField('FinalPriority', IntegerType(), True),
 StructField('ALSUnit', BooleanType(), True),
 StructField('CallTypeGroup', StringType(), True),
 StructField('NumAlarms', IntegerType(), True),
 StructField('UnitType', StringType(), True),
 StructField('UnitSequenceInCallDispatch', IntegerType(), True),
 StructField('FirePreventionDistrict', StringType(), True),
 StructField('SupervisorDistrict', StringType(), True),
 StructField('Neighborhood', StringType(), True),
 StructField('Location', StringType(), True),
 StructField('RowID', StringType(), True),
 StructField('Delay', FloatType(), True)])

# -------------- Bring in common column name [CSV] --------------------

df_wake=df_wake\
        .withColumnRenamed("HSISID","datasetId") \
        .withColumnRenamed("NAME","name") \
        .withColumnRenamed("ADDRESS1","address1") \
        .withColumnRenamed("ADDRESS2","address2") \
        .withColumnRenamed("CITY","city") \
        .withColumnRenamed("STATE","state") \
        .withColumnRenamed("POSTALCODE","zip") \
        .withColumnRenamed("PHONENUMBER","tel") \
        .withColumnRenamed("RESTAURANTOPENDATE","dateStart") \
        .withColumnRenamed("FACILITYTYPE","type") \
        .withColumnRenamed("X","geoX") \
        .withColumnRenamed("Y","geoY") \
        .drop("OBJECTID") \
        .drop("PERMITID") \
        .drop("GEOCODESTATUS")

# -------------- Bring in common column name [JSON] --------------------



df_durham=df_durham \
        .withColumn("datasetId",col("fields.id")) \
        .withColumn("name",col("fields.premise_name")) \
        .withColumn("address1",col("fields.premise_address1")) \
        .withColumn("address2",col("fields.premise_address2")) \
        .withColumn("city",col("fields.premise_city")) \
        .withColumn("state",col("fields.premise_state")) \
        .withColumn("zip",col("fields.premise_zip")) \
        .withColumn("tel",col("fields.premise_phone")) \
        .withColumn("dateStart",col("fields.opening_date")) \
        .withColumn("type",split(col("fields.type_description")," - ").getItem(1)) \
        .withColumn("geoX",col("fields.geolocation").getItem(0)) \
        .withColumn("geoY",col("fields.geolocation").getItem(1))

df_durham.printSchema()


