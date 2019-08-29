
# `DataFrame`

* Are the most efficient due to catalyst optimizer
* Are available in all languages
* A table with data rows and columns
* Analogous to a spreadsheet or table
* Distributed and spans over multiple machines!
* Easiest to use, particularly for non-functional programmers

## The `SparkSession`

* A majority of the jobs that Spark will run will require the `SparkSession`
* The `SparkSession` is the entry point to programming Spark with the `Dataset` and `DataFrame` API
* In REPL and Notebook environments, it is previously assigned to the `spark` value


```scala
spark
```




    res1: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@3f7a3b36
    



## Creating a Range

* `range` is a method in the `SparkSession` that returns a `DataSet`
* A `DataFrame` is actually a `DataSet[Row]` where row is the representation of a row of data
* We will discuss `Row` and `DataSet` later


```scala
val dataset = spark.range(1, 100)
```




    dataset: org.apache.spark.sql.Dataset[Long] = [id: bigint]
    



## Changing a `Dataset[Long]` to a `DataFrame`


```scala
val dataFrame = dataset.toDF("myRange")
```




    dataFrame: org.apache.spark.sql.DataFrame = [myRange: bigint]
    



## Making a `DataFrame` from the `RDD`

## `show()`

* Shows the data
* Default of 20 elements
* Can be changed


```scala
dataFrame.show(40)
```

    +-------+
    |myRange|
    +-------+
    |      1|
    |      2|
    |      3|
    |      4|
    |      5|
    |      6|
    |      7|
    |      8|
    |      9|
    |     10|
    |     11|
    |     12|
    |     13|
    |     14|
    |     15|
    |     16|
    |     17|
    |     18|
    |     19|
    |     20|
    |     21|
    |     22|
    |     23|
    |     24|
    |     25|
    |     26|
    |     27|
    |     28|
    |     29|
    |     30|
    |     31|
    |     32|
    |     33|
    |     34|
    |     35|
    |     36|
    |     37|
    |     38|
    |     39|
    |     40|
    +-------+
    only showing top 40 rows
    
    

This is a download from Kaggle.com called the good reads books dataset located at https://www.kaggle.com/jealousleopard/goodreadsbooks

## `spark.read`
* Reads data from a filesystem
* Should specify a file type
* Preferably from a distributed file system like hdfs
* Uses `load` to load the information from the location, for example
  * Use `"hdfs://"` to load from hdfs
  * Use `"s3a://"` to load from s3 on AWS
* Here we will use a local file system
* **Question** what is wrong with the results? Hint: You may want to view the [data file](../data/books.csv)


```scala
val booksDF = spark.read
                   .format("csv")
                   .load("../data/books.csv")
booksDF.show()
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    |   _c0|                 _c1|                 _c2|           _c3|       _c4|          _c5|          _c6|        _c7|          _c8|               _c9|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|# num_pages|ratings_count|text_reviews_count|
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|          4.56|0439785960|9780439785969|          eng|        652|      1944099|             26249|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|          4.49|0439358078|9780439358071|          eng|        870|      1996446|             27613|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|          4.47|0439554934|9780439554930|          eng|        320|      5629932|             70390|
    |     4|Harry Potter and ...|        J.K. Rowling|          4.41|0439554896|9780439554893|          eng|        352|         6267|               272|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|          4.55|043965548X|9780439655484|          eng|        435|      2149872|             33964|
    |     8|Harry Potter Boxe...|J.K. Rowling-Mary...|          4.78|0439682584|9780439682589|          eng|       2690|        38872|               154|
    |     9|Unauthorized Harr...|W. Frederick Zimm...|          3.69|0976540606|9780976540601|        en-US|        152|           18|                 1|
    |    10|Harry Potter Coll...|        J.K. Rowling|          4.73|0439827604|9780439827607|          eng|       3342|        27410|               820|
    |    12|The Ultimate Hitc...|       Douglas Adams|          4.38|0517226952|9780517226957|          eng|        815|         3602|               258|
    |    13|The Ultimate Hitc...|       Douglas Adams|          4.38|0345453743|9780345453747|          eng|        815|       240189|              3954|
    |    14|The Hitchhiker's ...|       Douglas Adams|          4.22|1400052920|9781400052929|          eng|        215|         4416|               408|
    |    16|The Hitchhiker's ...|Douglas Adams-Ste...|          4.22|0739322206|9780739322208|          eng|          6|         1222|               253|
    |    18|The Ultimate Hitc...|       Douglas Adams|          4.38|0517149257|9780517149256|        en-US|        815|         2801|               192|
    |    21|A Short History o...|Bill Bryson-Willi...|          4.20|076790818X|9780767908184|          eng|        544|       228522|              8840|
    |    22|Bill Bryson's Afr...|         Bill Bryson|          3.43|0767915062|9780767915069|          eng|         55|         6993|               470|
    |    23|Bryson's Dictiona...|         Bill Bryson|          3.88|0767910435|9780767910439|          eng|        256|         2020|               124|
    |    24|In a Sunburned Co...|         Bill Bryson|          4.07|0767903862|9780767903868|          eng|        335|        68213|              4077|
    |    25|I'm a Stranger He...|         Bill Bryson|          3.90|076790382X|9780767903820|          eng|        304|        47490|              2153|
    |    26|The Lost Continen...|         Bill Bryson|          3.83|0060920084|9780060920081|        en-US|        299|        43779|              2146|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    only showing top 20 rows
    
    




    booksDF: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 8 more fields]
    



## Schema

* Schemas have by default are assumed by the structure of our tables
* We can view the schemas of each of these DataFrame by calling `printSchema`
* A schema is a `StructType` made up of a number of fields called `StructField`s
* A `StructField` has:
  * A `name`
  * A `type`
  * A `boolean` that specifies whether the column is nullable
  * A schema can also contain other `StructType` (Spark complex types).
  * Can also be overridden by your own custom schema which is preferred for production

## Fixing the schema

* Notice the schema from the above, by calling `printSchema()`
* This shows the schema of the `DataFrame`
* **Question** What do you think is wrong with the schema that is determined


```scala
booksDF.printSchema()
```

    root
     |-- _c0: string (nullable = true)
     |-- _c1: string (nullable = true)
     |-- _c2: string (nullable = true)
     |-- _c3: string (nullable = true)
     |-- _c4: string (nullable = true)
     |-- _c5: string (nullable = true)
     |-- _c6: string (nullable = true)
     |-- _c7: string (nullable = true)
     |-- _c8: string (nullable = true)
     |-- _c9: string (nullable = true)
    
    

## Infering the schema and bringing in the header

* Setting the option `inferSchama` we can set the schama based on the data 
* Setting the option `header` we can set the first row to be the header


```scala
val booksDF = spark.read.format("csv")
                     .option("inferSchema", "true")
                     .option("header", "true")
                     .load("../data/books.csv")
```




    booksDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    




```scala
booksDF.show(5)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|# num_pages|ratings_count|text_reviews_count|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|          4.56|0439785960|9780439785969|          eng|        652|      1944099|             26249|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|          4.49|0439358078|9780439358071|          eng|        870|      1996446|             27613|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|          4.47|0439554934|9780439554930|          eng|        320|      5629932|             70390|
    |     4|Harry Potter and ...|        J.K. Rowling|          4.41|0439554896|9780439554893|          eng|        352|         6267|               272|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|          4.55|043965548X|9780439655484|          eng|        435|      2149872|             33964|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    only showing top 5 rows
    
    

## `show` for the title looks cramped for space

* With `show` there are some other signatures that are worth while of investigating
* The signature from the (API)[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset] shows some of the following signatures
   * `show(numRows: Int, truncate: Int, vertical: Boolean):Unit`
   * `show(numRows: Int, truncate: Int): Unit`
   * `show(numRows: Int, truncate: Boolean): Unit`
   * `show(truncate: Boolean): Unit`
   * `show(): Unit`
   * `show(numRows: Int)`
* `numRows` are the number of rows you wish to show
* `truncate` as a `Boolean`. If set `True` then it will truncate, `False` will show full text
* `truncate` as an `Int`. If set to more than `0`, truncates strings to truncate characters and all cells will be aligned right.
* `vertical = true` will show the records as a list for better viewing, let's try each in turn using `smallerSelectionDF`


```scala
booksDF.show(numRows=30, truncate=false)
```

    +------+------------------------------------------------------------------------------------------------------------+---------------------------------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    |bookID|title                                                                                                       |authors                                      |average_rating|isbn      |isbn13       |language_code|# num_pages|ratings_count|text_reviews_count|
    +------+------------------------------------------------------------------------------------------------------------+---------------------------------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    |1     |Harry Potter and the Half-Blood Prince (Harry Potter  #6)                                                   |J.K. Rowling-Mary GrandPré                   |4.56          |0439785960|9780439785969|eng          |652        |1944099      |26249             |
    |2     |Harry Potter and the Order of the Phoenix (Harry Potter  #5)                                                |J.K. Rowling-Mary GrandPré                   |4.49          |0439358078|9780439358071|eng          |870        |1996446      |27613             |
    |3     |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)                                                    |J.K. Rowling-Mary GrandPré                   |4.47          |0439554934|9780439554930|eng          |320        |5629932      |70390             |
    |4     |Harry Potter and the Chamber of Secrets (Harry Potter  #2)                                                  |J.K. Rowling                                 |4.41          |0439554896|9780439554893|eng          |352        |6267         |272               |
    |5     |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3)                                                 |J.K. Rowling-Mary GrandPré                   |4.55          |043965548X|9780439655484|eng          |435        |2149872      |33964             |
    |8     |Harry Potter Boxed Set  Books 1-5 (Harry Potter  #1-5)                                                      |J.K. Rowling-Mary GrandPré                   |4.78          |0439682584|9780439682589|eng          |2690       |38872        |154               |
    |9     |Unauthorized Harry Potter Book Seven News: "Half-Blood Prince" Analysis and Speculation                     |W. Frederick Zimmerman                       |3.69          |0976540606|9780976540601|en-US        |152        |18           |1                 |
    |10    |Harry Potter Collection (Harry Potter  #1-6)                                                                |J.K. Rowling                                 |4.73          |0439827604|9780439827607|eng          |3342       |27410        |820               |
    |12    |The Ultimate Hitchhiker's Guide: Five Complete Novels and One Story (Hitchhiker's Guide to the Galaxy  #1-5)|Douglas Adams                                |4.38          |0517226952|9780517226957|eng          |815        |3602         |258               |
    |13    |The Ultimate Hitchhiker's Guide to the Galaxy                                                               |Douglas Adams                                |4.38          |0345453743|9780345453747|eng          |815        |240189       |3954              |
    |14    |The Hitchhiker's Guide to the Galaxy (Hitchhiker's Guide to the Galaxy  #1)                                 |Douglas Adams                                |4.22          |1400052920|9781400052929|eng          |215        |4416         |408               |
    |16    |The Hitchhiker's Guide to the Galaxy (Hitchhiker's Guide to the Galaxy  #1)                                 |Douglas Adams-Stephen Fry                    |4.22          |0739322206|9780739322208|eng          |6          |1222         |253               |
    |18    |The Ultimate Hitchhiker's Guide (Hitchhiker's Guide to the Galaxy #1-5)                                     |Douglas Adams                                |4.38          |0517149257|9780517149256|en-US        |815        |2801         |192               |
    |21    |A Short History of Nearly Everything                                                                        |Bill Bryson-William Roberts                  |4.20          |076790818X|9780767908184|eng          |544        |228522       |8840              |
    |22    |Bill Bryson's African Diary                                                                                 |Bill Bryson                                  |3.43          |0767915062|9780767915069|eng          |55         |6993         |470               |
    |23    |Bryson's Dictionary of Troublesome Words: A Writer's Guide to Getting It Right                              |Bill Bryson                                  |3.88          |0767910435|9780767910439|eng          |256        |2020         |124               |
    |24    |In a Sunburned Country                                                                                      |Bill Bryson                                  |4.07          |0767903862|9780767903868|eng          |335        |68213        |4077              |
    |25    |I'm a Stranger Here Myself: Notes on Returning to America After Twenty Years Away                           |Bill Bryson                                  |3.90          |076790382X|9780767903820|eng          |304        |47490        |2153              |
    |26    |The Lost Continent: Travels in Small Town America                                                           |Bill Bryson                                  |3.83          |0060920084|9780060920081|en-US        |299        |43779        |2146              |
    |27    |Neither Here nor There: Travels in Europe                                                                   |Bill Bryson                                  |3.87          |0380713802|9780380713806|eng          |254        |46397        |2127              |
    |28    |Notes from a Small Island                                                                                   |Bill Bryson                                  |3.92          |0380727501|9780380727506|eng          |324        |76476        |3159              |
    |29    |The Mother Tongue: English and How It Got That Way                                                          |Bill Bryson                                  |3.94          |0380715430|9780380715435|eng          |270        |26672        |1986              |
    |30    |J.R.R. Tolkien 4-Book Boxed Set: The Hobbit and The Lord of the Rings                                       |J.R.R. Tolkien                               |4.59          |0345538374|9780345538376|eng          |1728       |97731        |1536              |
    |31    |The Lord of the Rings (The Lord of the Rings  #1-3)                                                         |J.R.R. Tolkien                               |4.49          |0618517650|9780618517657|eng          |1184       |1670         |91                |
    |32    |The Lord of the Rings (The Lord of the Rings  #1-3)                                                         |J.R.R. Tolkien                               |4.49          |0618346244|9780618346240|eng          |1137       |2819         |139               |
    |34    |The Fellowship of the Ring (The Lord of the Rings  #1)                                                      |J.R.R. Tolkien                               |4.35          |0618346252|9780618346257|eng          |398        |2009749      |12784             |
    |35    |The Lord of the Rings (The Lord of the Rings  #1-3)                                                         |J.R.R. Tolkien-Alan  Lee                     |4.49          |0618260587|9780618260584|en-US        |1216       |1606         |139               |
    |36    |The Lord of the Rings: Weapons and Warfare                                                                  |Chris   Smith-Christopher  Lee-Richard Taylor|4.53          |0618391002|9780618391004|eng          |218        |18934        |43                |
    |37    |The Lord of the Rings: Complete Visual Companion                                                            |Jude Fisher                                  |4.50          |0618510826|9780618510825|eng          |224        |343          |6                 |
    |38    |The Lord of the Rings Box Set                                                                               |J.R.R. Tolkien                               |4.49          |0618153977|9780618153978|eng          |1223       |216          |19                |
    +------+------------------------------------------------------------------------------------------------------------+---------------------------------------------+--------------+----------+-------------+-------------+-----------+-------------+------------------+
    only showing top 30 rows
    
    


```scala
booksDF.show(numRows=30, vertical=true, truncate=30)
```

    -RECORD 0--------------------------------------------
     bookID             | 1                              
     title              | Harry Potter and the Half-B... 
     authors            | J.K. Rowling-Mary GrandPré     
     average_rating     | 4.56                           
     isbn               | 0439785960                     
     isbn13             | 9780439785969                  
     language_code      | eng                            
     # num_pages        | 652                            
     ratings_count      | 1944099                        
     text_reviews_count | 26249                          
    -RECORD 1--------------------------------------------
     bookID             | 2                              
     title              | Harry Potter and the Order ... 
     authors            | J.K. Rowling-Mary GrandPré     
     average_rating     | 4.49                           
     isbn               | 0439358078                     
     isbn13             | 9780439358071                  
     language_code      | eng                            
     # num_pages        | 870                            
     ratings_count      | 1996446                        
     text_reviews_count | 27613                          
    -RECORD 2--------------------------------------------
     bookID             | 3                              
     title              | Harry Potter and the Sorcer... 
     authors            | J.K. Rowling-Mary GrandPré     
     average_rating     | 4.47                           
     isbn               | 0439554934                     
     isbn13             | 9780439554930                  
     language_code      | eng                            
     # num_pages        | 320                            
     ratings_count      | 5629932                        
     text_reviews_count | 70390                          
    -RECORD 3--------------------------------------------
     bookID             | 4                              
     title              | Harry Potter and the Chambe... 
     authors            | J.K. Rowling                   
     average_rating     | 4.41                           
     isbn               | 0439554896                     
     isbn13             | 9780439554893                  
     language_code      | eng                            
     # num_pages        | 352                            
     ratings_count      | 6267                           
     text_reviews_count | 272                            
    -RECORD 4--------------------------------------------
     bookID             | 5                              
     title              | Harry Potter and the Prison... 
     authors            | J.K. Rowling-Mary GrandPré     
     average_rating     | 4.55                           
     isbn               | 043965548X                     
     isbn13             | 9780439655484                  
     language_code      | eng                            
     # num_pages        | 435                            
     ratings_count      | 2149872                        
     text_reviews_count | 33964                          
    -RECORD 5--------------------------------------------
     bookID             | 8                              
     title              | Harry Potter Boxed Set  Boo... 
     authors            | J.K. Rowling-Mary GrandPré     
     average_rating     | 4.78                           
     isbn               | 0439682584                     
     isbn13             | 9780439682589                  
     language_code      | eng                            
     # num_pages        | 2690                           
     ratings_count      | 38872                          
     text_reviews_count | 154                            
    -RECORD 6--------------------------------------------
     bookID             | 9                              
     title              | Unauthorized Harry Potter B... 
     authors            | W. Frederick Zimmerman         
     average_rating     | 3.69                           
     isbn               | 0976540606                     
     isbn13             | 9780976540601                  
     language_code      | en-US                          
     # num_pages        | 152                            
     ratings_count      | 18                             
     text_reviews_count | 1                              
    -RECORD 7--------------------------------------------
     bookID             | 10                             
     title              | Harry Potter Collection (Ha... 
     authors            | J.K. Rowling                   
     average_rating     | 4.73                           
     isbn               | 0439827604                     
     isbn13             | 9780439827607                  
     language_code      | eng                            
     # num_pages        | 3342                           
     ratings_count      | 27410                          
     text_reviews_count | 820                            
    -RECORD 8--------------------------------------------
     bookID             | 12                             
     title              | The Ultimate Hitchhiker's G... 
     authors            | Douglas Adams                  
     average_rating     | 4.38                           
     isbn               | 0517226952                     
     isbn13             | 9780517226957                  
     language_code      | eng                            
     # num_pages        | 815                            
     ratings_count      | 3602                           
     text_reviews_count | 258                            
    -RECORD 9--------------------------------------------
     bookID             | 13                             
     title              | The Ultimate Hitchhiker's G... 
     authors            | Douglas Adams                  
     average_rating     | 4.38                           
     isbn               | 0345453743                     
     isbn13             | 9780345453747                  
     language_code      | eng                            
     # num_pages        | 815                            
     ratings_count      | 240189                         
     text_reviews_count | 3954                           
    -RECORD 10-------------------------------------------
     bookID             | 14                             
     title              | The Hitchhiker's Guide to t... 
     authors            | Douglas Adams                  
     average_rating     | 4.22                           
     isbn               | 1400052920                     
     isbn13             | 9781400052929                  
     language_code      | eng                            
     # num_pages        | 215                            
     ratings_count      | 4416                           
     text_reviews_count | 408                            
    -RECORD 11-------------------------------------------
     bookID             | 16                             
     title              | The Hitchhiker's Guide to t... 
     authors            | Douglas Adams-Stephen Fry      
     average_rating     | 4.22                           
     isbn               | 0739322206                     
     isbn13             | 9780739322208                  
     language_code      | eng                            
     # num_pages        | 6                              
     ratings_count      | 1222                           
     text_reviews_count | 253                            
    -RECORD 12-------------------------------------------
     bookID             | 18                             
     title              | The Ultimate Hitchhiker's G... 
     authors            | Douglas Adams                  
     average_rating     | 4.38                           
     isbn               | 0517149257                     
     isbn13             | 9780517149256                  
     language_code      | en-US                          
     # num_pages        | 815                            
     ratings_count      | 2801                           
     text_reviews_count | 192                            
    -RECORD 13-------------------------------------------
     bookID             | 21                             
     title              | A Short History of Nearly E... 
     authors            | Bill Bryson-William Roberts    
     average_rating     | 4.20                           
     isbn               | 076790818X                     
     isbn13             | 9780767908184                  
     language_code      | eng                            
     # num_pages        | 544                            
     ratings_count      | 228522                         
     text_reviews_count | 8840                           
    -RECORD 14-------------------------------------------
     bookID             | 22                             
     title              | Bill Bryson's African Diary    
     authors            | Bill Bryson                    
     average_rating     | 3.43                           
     isbn               | 0767915062                     
     isbn13             | 9780767915069                  
     language_code      | eng                            
     # num_pages        | 55                             
     ratings_count      | 6993                           
     text_reviews_count | 470                            
    -RECORD 15-------------------------------------------
     bookID             | 23                             
     title              | Bryson's Dictionary of Trou... 
     authors            | Bill Bryson                    
     average_rating     | 3.88                           
     isbn               | 0767910435                     
     isbn13             | 9780767910439                  
     language_code      | eng                            
     # num_pages        | 256                            
     ratings_count      | 2020                           
     text_reviews_count | 124                            
    -RECORD 16-------------------------------------------
     bookID             | 24                             
     title              | In a Sunburned Country         
     authors            | Bill Bryson                    
     average_rating     | 4.07                           
     isbn               | 0767903862                     
     isbn13             | 9780767903868                  
     language_code      | eng                            
     # num_pages        | 335                            
     ratings_count      | 68213                          
     text_reviews_count | 4077                           
    -RECORD 17-------------------------------------------
     bookID             | 25                             
     title              | I'm a Stranger Here Myself:... 
     authors            | Bill Bryson                    
     average_rating     | 3.90                           
     isbn               | 076790382X                     
     isbn13             | 9780767903820                  
     language_code      | eng                            
     # num_pages        | 304                            
     ratings_count      | 47490                          
     text_reviews_count | 2153                           
    -RECORD 18-------------------------------------------
     bookID             | 26                             
     title              | The Lost Continent: Travels... 
     authors            | Bill Bryson                    
     average_rating     | 3.83                           
     isbn               | 0060920084                     
     isbn13             | 9780060920081                  
     language_code      | en-US                          
     # num_pages        | 299                            
     ratings_count      | 43779                          
     text_reviews_count | 2146                           
    -RECORD 19-------------------------------------------
     bookID             | 27                             
     title              | Neither Here nor There: Tra... 
     authors            | Bill Bryson                    
     average_rating     | 3.87                           
     isbn               | 0380713802                     
     isbn13             | 9780380713806                  
     language_code      | eng                            
     # num_pages        | 254                            
     ratings_count      | 46397                          
     text_reviews_count | 2127                           
    -RECORD 20-------------------------------------------
     bookID             | 28                             
     title              | Notes from a Small Island      
     authors            | Bill Bryson                    
     average_rating     | 3.92                           
     isbn               | 0380727501                     
     isbn13             | 9780380727506                  
     language_code      | eng                            
     # num_pages        | 324                            
     ratings_count      | 76476                          
     text_reviews_count | 3159                           
    -RECORD 21-------------------------------------------
     bookID             | 29                             
     title              | The Mother Tongue: English ... 
     authors            | Bill Bryson                    
     average_rating     | 3.94                           
     isbn               | 0380715430                     
     isbn13             | 9780380715435                  
     language_code      | eng                            
     # num_pages        | 270                            
     ratings_count      | 26672                          
     text_reviews_count | 1986                           
    -RECORD 22-------------------------------------------
     bookID             | 30                             
     title              | J.R.R. Tolkien 4-Book Boxed... 
     authors            | J.R.R. Tolkien                 
     average_rating     | 4.59                           
     isbn               | 0345538374                     
     isbn13             | 9780345538376                  
     language_code      | eng                            
     # num_pages        | 1728                           
     ratings_count      | 97731                          
     text_reviews_count | 1536                           
    -RECORD 23-------------------------------------------
     bookID             | 31                             
     title              | The Lord of the Rings (The ... 
     authors            | J.R.R. Tolkien                 
     average_rating     | 4.49                           
     isbn               | 0618517650                     
     isbn13             | 9780618517657                  
     language_code      | eng                            
     # num_pages        | 1184                           
     ratings_count      | 1670                           
     text_reviews_count | 91                             
    -RECORD 24-------------------------------------------
     bookID             | 32                             
     title              | The Lord of the Rings (The ... 
     authors            | J.R.R. Tolkien                 
     average_rating     | 4.49                           
     isbn               | 0618346244                     
     isbn13             | 9780618346240                  
     language_code      | eng                            
     # num_pages        | 1137                           
     ratings_count      | 2819                           
     text_reviews_count | 139                            
    -RECORD 25-------------------------------------------
     bookID             | 34                             
     title              | The Fellowship of the Ring ... 
     authors            | J.R.R. Tolkien                 
     average_rating     | 4.35                           
     isbn               | 0618346252                     
     isbn13             | 9780618346257                  
     language_code      | eng                            
     # num_pages        | 398                            
     ratings_count      | 2009749                        
     text_reviews_count | 12784                          
    -RECORD 26-------------------------------------------
     bookID             | 35                             
     title              | The Lord of the Rings (The ... 
     authors            | J.R.R. Tolkien-Alan  Lee       
     average_rating     | 4.49                           
     isbn               | 0618260587                     
     isbn13             | 9780618260584                  
     language_code      | en-US                          
     # num_pages        | 1216                           
     ratings_count      | 1606                           
     text_reviews_count | 139                            
    -RECORD 27-------------------------------------------
     bookID             | 36                             
     title              | The Lord of the Rings: Weap... 
     authors            | Chris   Smith-Christopher  ... 
     average_rating     | 4.53                           
     isbn               | 0618391002                     
     isbn13             | 9780618391004                  
     language_code      | eng                            
     # num_pages        | 218                            
     ratings_count      | 18934                          
     text_reviews_count | 43                             
    -RECORD 28-------------------------------------------
     bookID             | 37                             
     title              | The Lord of the Rings: Comp... 
     authors            | Jude Fisher                    
     average_rating     | 4.50                           
     isbn               | 0618510826                     
     isbn13             | 9780618510825                  
     language_code      | eng                            
     # num_pages        | 224                            
     ratings_count      | 343                            
     text_reviews_count | 6                              
    -RECORD 29-------------------------------------------
     bookID             | 38                             
     title              | The Lord of the Rings Box Set  
     authors            | J.R.R. Tolkien                 
     average_rating     | 4.49                           
     isbn               | 0618153977                     
     isbn13             | 9780618153978                  
     language_code      | eng                            
     # num_pages        | 1223                           
     ratings_count      | 216                            
     text_reviews_count | 19                             
    only showing top 30 rows
    
    

## Spark Data Types

In Spark, it is good to know the certain data types so that we can either interpret or cast, here is the list of data types. In the **Scala Type** column all types are in the `org.apache.spark.sql.types` package.  The latest types API for Scala [can be found here](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/package-summary.html)


| Spark Type             | Scala Type                 | Scala API       |
| -----------------------| :-------------------------:| ---------------:|
| `ByteType`             | `Byte`                     | `ByteType`      |
| `ShortType`            | `Short`                    | `ShortType`     |
| `IntegerType`          | `Int`                      | `IntegerType`   |
| `LongType`             | `Long`                     | `LongType`      |
| `FloatType`            | `Float`                    | `FloatType`     |
| `DoubleType`           | `Double`                   | `DoubleType`    |
| `DecimalType`          | `java.math.BigDecimal`     | `DecimalType`   |
| `StringType`           | `String`                   | `StringType`    |
| `BinaryType`           | `Array[Byte]`              | `BinaryType`    |
| `TimestampType`        | `java.sql.Timestamp`       | `TimestampType` |
| `DateType`             | `java.sql.Date`            | `DateType`      |
| `ArrayType`            | `scala.collection.Seq`     | `ArrayType`     |
| `MapType`              | `scala.collection.Map`     | `MapType`       |
| `StructType`           | `org.apache.spark.sql.Row` | `StructType`    |
| `StructField`          | `StructField`              | `StructField`   |

## Explicitly Setting our Schema

First let's take a look at the original schema


```scala
booksDF.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: string (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- # num_pages: string (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- text_reviews_count: integer (nullable = true)
    
    


```scala
import org.apache.spark.sql.types._
val bookSchema = new StructType(Array(
   new StructField("bookID", IntegerType, false),
   new StructField("title", StringType, false),
   new StructField("authors", StringType, false),
   new StructField("average_rating", FloatType, false),
   new StructField("isbn", StringType, false),
   new StructField("isbn13", StringType, false),
   new StructField("language_code", StringType, false),
   new StructField("num_pages", IntegerType, false),
   new StructField("ratings_count", IntegerType, false),
   new StructField("text_reviews_count", IntegerType, false)))
```




    import org.apache.spark.sql.types._
    bookSchema: org.apache.spark.sql.types.StructType = StructType(StructField(bookID,IntegerType,false), StructField(title,StringType,false), StructField(authors,StringType,false), StructField(average_rating,FloatType,false), StructField(isbn,StringType,false), StructField(isbn13,StringType,false), StructField(language_code,StringType,false), StructField(num_pages,IntegerType,false), StructField(ratings_count,IntegerType,false), StructField(text_reviews_count,IntegerType,false))
    



## Reading the Data Again with a Schema


```scala
val booksSchemaDF = spark.read.format("csv")
                         .schema(bookSchema)
                         .option("header", "true")
                         .load("../data/books.csv")
booksSchemaDF.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: float (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- num_pages: integer (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- text_reviews_count: integer (nullable = true)
    
    




    booksSchemaDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    




```scala
booksSchemaDF.show(10)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|          4.56|0439785960|9780439785969|          eng|      652|      1944099|             26249|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|          4.49|0439358078|9780439358071|          eng|      870|      1996446|             27613|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|          4.47|0439554934|9780439554930|          eng|      320|      5629932|             70390|
    |     4|Harry Potter and ...|        J.K. Rowling|          4.41|0439554896|9780439554893|          eng|      352|         6267|               272|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|          4.55|043965548X|9780439655484|          eng|      435|      2149872|             33964|
    |     8|Harry Potter Boxe...|J.K. Rowling-Mary...|          4.78|0439682584|9780439682589|          eng|     2690|        38872|               154|
    |     9|Unauthorized Harr...|W. Frederick Zimm...|          3.69|0976540606|9780976540601|        en-US|      152|           18|                 1|
    |    10|Harry Potter Coll...|        J.K. Rowling|          4.73|0439827604|9780439827607|          eng|     3342|        27410|               820|
    |    12|The Ultimate Hitc...|       Douglas Adams|          4.38|0517226952|9780517226957|          eng|      815|         3602|               258|
    |    13|The Ultimate Hitc...|       Douglas Adams|          4.38|0345453743|9780345453747|          eng|      815|       240189|              3954|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    only showing top 10 rows
    
    

## Writing the results back to the file system

* A typical job would write back the results to a distributed file system
* We do not have a distributed file system, just a local one for this class
* We can run the same procedure for local
* To write the results we can call `write`, every write has a mode which defines how you write to the filesystem

| Mode            | Description                                                                                  |
|-----------------|----------------------------------------------------------------------------------------------|
| `append`        | Appends the output files to the list of files that already exist at that location            |
| `overwrite`     | Will completely overwrite any data that already exists there                                 |
| `errorIfExists` | Throws an error and fails the write if data or files already exist at the specified location |
| `ignore`        | If data or files exist at the location, do nothing with the current DataFrame                |


```scala
booksSchemaDF
  .write
  .format("csv")
  .option("mode", "overwrite")
  .option("path", "../data/books-prepped.csv")
  .save()
```


    org.apache.spark.sql.AnalysisException: path file:/home/jovyan/data/books-prepped.csv already exists.;

      at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:114)

      at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:104)

      at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:102)

      at org.apache.spark.sql.execution.command.DataWritingCommandExec.doExecute(commands.scala:122)

      at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:131)

      at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:127)

      at org.apache.spark.sql.execution.SparkPlan$$anonfun$executeQuery$1.apply(SparkPlan.scala:155)

      at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)

      at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:152)

      at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)

      at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:80)

      at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:80)

      at org.apache.spark.sql.DataFrameWriter$$anonfun$runCommand$1.apply(DataFrameWriter.scala:676)

      at org.apache.spark.sql.DataFrameWriter$$anonfun$runCommand$1.apply(DataFrameWriter.scala:676)

      at org.apache.spark.sql.execution.SQLExecution$$anonfun$withNewExecutionId$1.apply(SQLExecution.scala:78)

      at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)

      at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:73)

      at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:676)

      at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:285)

      at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:271)

      ... 38 elided

    


## Creating a Custom DataFrame

## Creating from a `RDD`

* Reminder: `RDD` or Resilient Distributed Dataset is often less performant than the `DataFrame`, `DataSet`, and `SparkSQL` counterparts.
* It is still useful and used to create the `DataFrame` in the first place, especially with `parallelize`
* `parralelize` is a method factory from an object called the `SparkContext`. 
* `SparkContext` was used extensively in the 1.x versions of Spark.
* It can be obtained from the `SparkSession` by the `sparkContext` method


```scala
import org.apache.spark.sql.Row
val sparkContext = spark.sparkContext
val rddRows = sparkContext.parallelize(Seq(Row("Abe", null, "Lincoln", 40000),
           Row("Martin", "Luther", "King", 80000),
           Row("Ben", null, "Franklin", 82000),
           Row("Toni", null, "Morrisson", 82000)))

```




    import org.apache.spark.sql.Row
    sparkContext: org.apache.spark.SparkContext = org.apache.spark.SparkContext@731e2b48
    rddRows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = ParallelCollectionRDD[39] at parallelize at <console>:29
    




```scala
val employeeSchema = new StructType(Array(
      StructField("firstName", StringType, nullable = false),
      StructField("middleName", StringType, nullable = true),
      StructField("lastName", StringType, nullable = false),
      StructField("salaryPerYear", IntegerType, nullable = false)
    ))

val dataFrame = spark.createDataFrame(rddRows, employeeSchema)
dataFrame.show()
```

    +---------+----------+---------+-------------+
    |firstName|middleName| lastName|salaryPerYear|
    +---------+----------+---------+-------------+
    |      Abe|      null|  Lincoln|        40000|
    |   Martin|    Luther|     King|        80000|
    |      Ben|      null| Franklin|        82000|
    |     Toni|      null|Morrisson|        82000|
    +---------+----------+---------+-------------+
    
    




    employeeSchema: org.apache.spark.sql.types.StructType = StructType(StructField(firstName,StringType,false), StructField(middleName,StringType,true), StructField(lastName,StringType,false), StructField(salaryPerYear,IntegerType,false))
    dataFrame: org.apache.spark.sql.DataFrame = [firstName: string, middleName: string ... 2 more fields]
    



## Lab: Read Wine Data

For our lab, we will be reading data from the Wine Data Set at Kaggle. https://www.kaggle.com/zynicide/wine-reviews. We already downloaded and made it a part of your notebook in the `data` directory.

**Step 1:** Read wine data from `../data/winemag.csv` first without setting headers or infering the schema

**Step 2:** See what you glean from the data

**Step 3:** Print the schema

**Step 4:** Apply headers, infer the schema

**Step 5:** Print the schema again

**Step 6:** `show` some of the data using some of the varying forms

**Step 7:** Apply your own schema (Warning: it may not work because of nulls)
