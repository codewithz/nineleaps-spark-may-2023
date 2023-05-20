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

# Create schema for Yellow Taxi Data

taxiSchema = (
                    StructType
                    ([ 
                        StructField("VendorId"               , IntegerType()   , True),
                        StructField("lpep_pickup_datetime"   , TimestampType() , True),
                        StructField("lpep_dropoff_datetime"  , TimestampType() , True),                            
                        StructField("passenger_count"        , DoubleType()    , True),
                        StructField("trip_distance"          , DoubleType()    , True),
                        StructField("RatecodeID"             , DoubleType()    , True),                            
                        StructField("store_and_fwd_flag"     , StringType()    , True),
                        StructField("PULocationID"           , IntegerType()   , True),
                        StructField("DOLocationID"           , IntegerType()   , True),                            
                        StructField("payment_type"           , IntegerType()   , True),                            
                        StructField("fare_amount"            , DoubleType()    , True),
                        StructField("extra"                  , DoubleType()    , True),
                        StructField("mta_tax"                , DoubleType()    , True),
                        StructField("tip_amount"             , DoubleType()    , True),
                        StructField("tolls_amount"           , DoubleType()    , True),
                        StructField("improvement_surcharge"  , DoubleType()    , True),
                        StructField("total_amount"           , DoubleType()    , True),
                        StructField("congestion_surcharge"   , DoubleType()    , True),
                        StructField("airport_fee"            , DoubleType()    , True)
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

#------------------ UDF -------------------------------
def convertCase(str):
    
    result = ""
    
    nameWordsArray = str.split(",")
    
    for nameWord in nameWordsArray:        
       result = (result
                    + nameWord[0:1].upper()             # Ex- for word 'MOHIT', returns=> 'M'
                    + nameWord[1:len(nameWord)].lower() # Ex- for word 'MOHIT', returns=> 'ohit, '
                    + ", "   
                )
    
    result = result[0:len(result) - 2]
                                        # Ex- for name 'Batra, Mohit, ' returns => 'Batra, Mohit'
    
    return result 
#   -------------- Window Functions ----------------------
  # Create schema for Yellow Taxi data
yellowTaxiSchema = (
                        StructType
                        ([ 
                            StructField("VendorId"               , IntegerType()   , True),
                            StructField("PickupTime"             , TimestampType() , True),
                            StructField("DropTime"               , TimestampType() , True),
                            StructField("PassengerCount"         , DoubleType()    , True),
                            StructField("TripDistance"           , DoubleType()    , True),
                            StructField("RateCodeId"             , DoubleType()    , True),
                            StructField("StoreAndFwdFlag"        , StringType()    , True),
                            StructField("PickupLocationId"       , IntegerType()   , True),
                            StructField("DropLocationId"         , IntegerType()   , True),
                            StructField("PaymentType"            , IntegerType()   , True),
                            StructField("FareAmount"             , DoubleType()    , True),
                            StructField("Extra"                  , DoubleType()    , True),
                            StructField("MtaTax"                 , DoubleType()    , True),
                            StructField("TipAmount"              , DoubleType()    , True),
                            StructField("TollsAmount"            , DoubleType()    , True),
                            StructField("ImprovementSurcharge"   , DoubleType()    , True),
                            StructField("TotalAmount"            , DoubleType()    , True),
                            StructField("CongestionSurcharge"    , DoubleType()    , True),
                            StructField("AirportFee"             , DoubleType()    , True)
                        ])
                   )


# Read Yellow Taxis file
yellowTaxiDF = (
                  spark
                    .read
                    .option("header", "true")    
                    .schema(yellowTaxiSchema)    
                    .csv("E:\SparkData\Raw\YellowTaxis_202210.csv")
               )


# Create temp view
yellowTaxiDF.createOrReplaceTempView("YellowTaxis")

# Create schema for Taxi Zones data
taxiZonesSchema = "PickupLocationId INT, Borough STRING, Zone STRING, ServiceZone STRING"


# Read Taxi Zones file
taxiZonesDF = (
                  spark
                    .read                    
                    .schema(taxiZonesSchema)
                    .csv("E:\SparkData\Raw\TaxiZones.csv")
              )


# Create temp view
taxiZonesDF.createOrReplaceTempView("TaxiZones")

