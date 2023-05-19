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
