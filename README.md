
## Lab: Read Wine Data

For our lab, we will be reading data from the Wine Data Set at Kaggle. https://www.kaggle.com/zynicide/wine-reviews. We already downloaded and made it a part of your notebook in the `data` directory.


**Step 1:** Read wine data from `../data/winemag.csv` first without setting headers or infering the schema

**Step 2:** See what you glean from the data

**Step 3:** Print the schema

**Step 4:** Apply headers, infer the schema

**Step 5:** Print the schema again

**Step 6:** `show` some of the data using some of the varying forms

**Step 7:** Apply your own schema


```scala
val winesDF = spark.read.format("csv")
                     .load("../data/winemag.csv")
```




    winesDF: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 12 more fields]
    




```scala
winesDF.show(10)
```

    +----+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+--------------------+--------------------+------------------+-------------------+
    | _c0|     _c1|                 _c2|                 _c3|   _c4|  _c5|              _c6|                _c7|              _c8|               _c9|                _c10|                _c11|              _c12|               _c13|
    +----+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+--------------------+--------------------+------------------+-------------------+
    |null| country|         description|         designation|points|price|         province|           region_1|         region_2|       taster_name|taster_twitter_ha...|               title|           variety|             winery|
    |   0|   Italy|Aromas include tr...|        Vulkà Bianco|    87| null|Sicily & Sardinia|               Etna|             null|     Kerin O’Keefe|        @kerinokeefe|Nicosia 2013 Vulk...|       White Blend|            Nicosia|
    |   1|Portugal|This is ripe and ...|            Avidagos|    87| 15.0|            Douro|               null|             null|        Roger Voss|          @vossroger|Quinta dos Avidag...|    Portuguese Red|Quinta dos Avidagos|
    |   2|      US|Tart and snappy, ...|                null|    87| 14.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|         @paulgwine |Rainstorm 2013 Pi...|        Pinot Gris|          Rainstorm|
    |   3|      US|Pineapple rind, l...|Reserve Late Harvest|    87| 13.0|         Michigan|Lake Michigan Shore|             null|Alexander Peartree|                null|St. Julian 2013 R...|          Riesling|         St. Julian|
    |   4|      US|Much like the reg...|Vintner's Reserve...|    87| 65.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|         @paulgwine |Sweet Cheeks 2012...|        Pinot Noir|       Sweet Cheeks|
    |   5|   Spain|Blackberry and ra...|        Ars In Vitro|    87| 15.0|   Northern Spain|            Navarra|             null| Michael Schachner|         @wineschach|Tandem 2011 Ars I...|Tempranillo-Merlot|             Tandem|
    |   6|   Italy|Here's a bright, ...|             Belsito|    87| 16.0|Sicily & Sardinia|           Vittoria|             null|     Kerin O’Keefe|        @kerinokeefe|Terre di Giurfo 2...|          Frappato|    Terre di Giurfo|
    |   7|  France|This dry and rest...|                null|    87| 24.0|           Alsace|             Alsace|             null|        Roger Voss|          @vossroger|Trimbach 2012 Gew...|    Gewürztraminer|           Trimbach|
    |   8| Germany|Savory dried thym...|               Shine|    87| 12.0|      Rheinhessen|               null|             null|Anna Lee C. Iijima|                null|Heinz Eifel 2013 ...|    Gewürztraminer|        Heinz Eifel|
    +----+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+--------------------+--------------------+------------------+-------------------+
    only showing top 10 rows
    
    


```scala
winesDF.printSchema
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
     |-- _c10: string (nullable = true)
     |-- _c11: string (nullable = true)
     |-- _c12: string (nullable = true)
     |-- _c13: string (nullable = true)
    
    


```scala
val winesDF = spark.read.format("csv")
                     .option("inferSchema", "true")
                     .option("header", "true")
                     .load("../data/winemag.csv")
```




    winesDF: org.apache.spark.sql.DataFrame = [_c0: string, country: string ... 12 more fields]
    




```scala
winesDF.printSchema
```

    root
     |-- _c0: string (nullable = true)
     |-- country: string (nullable = true)
     |-- description: string (nullable = true)
     |-- designation: string (nullable = true)
     |-- points: string (nullable = true)
     |-- price: string (nullable = true)
     |-- province: string (nullable = true)
     |-- region_1: string (nullable = true)
     |-- region_2: string (nullable = true)
     |-- taster_name: string (nullable = true)
     |-- taster_twitter_handle: string (nullable = true)
     |-- title: string (nullable = true)
     |-- variety: string (nullable = true)
     |-- winery: string (nullable = true)
    
    


```scala
winesDF.show(10)
```

    +---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
    |_c0| country|         description|         designation|points|price|         province|           region_1|         region_2|       taster_name|taster_twitter_handle|               title|           variety|             winery|
    +---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
    |  0|   Italy|Aromas include tr...|        Vulkà Bianco|    87| null|Sicily & Sardinia|               Etna|             null|     Kerin O’Keefe|         @kerinokeefe|Nicosia 2013 Vulk...|       White Blend|            Nicosia|
    |  1|Portugal|This is ripe and ...|            Avidagos|    87| 15.0|            Douro|               null|             null|        Roger Voss|           @vossroger|Quinta dos Avidag...|    Portuguese Red|Quinta dos Avidagos|
    |  2|      US|Tart and snappy, ...|                null|    87| 14.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|          @paulgwine |Rainstorm 2013 Pi...|        Pinot Gris|          Rainstorm|
    |  3|      US|Pineapple rind, l...|Reserve Late Harvest|    87| 13.0|         Michigan|Lake Michigan Shore|             null|Alexander Peartree|                 null|St. Julian 2013 R...|          Riesling|         St. Julian|
    |  4|      US|Much like the reg...|Vintner's Reserve...|    87| 65.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|          @paulgwine |Sweet Cheeks 2012...|        Pinot Noir|       Sweet Cheeks|
    |  5|   Spain|Blackberry and ra...|        Ars In Vitro|    87| 15.0|   Northern Spain|            Navarra|             null| Michael Schachner|          @wineschach|Tandem 2011 Ars I...|Tempranillo-Merlot|             Tandem|
    |  6|   Italy|Here's a bright, ...|             Belsito|    87| 16.0|Sicily & Sardinia|           Vittoria|             null|     Kerin O’Keefe|         @kerinokeefe|Terre di Giurfo 2...|          Frappato|    Terre di Giurfo|
    |  7|  France|This dry and rest...|                null|    87| 24.0|           Alsace|             Alsace|             null|        Roger Voss|           @vossroger|Trimbach 2012 Gew...|    Gewürztraminer|           Trimbach|
    |  8| Germany|Savory dried thym...|               Shine|    87| 12.0|      Rheinhessen|               null|             null|Anna Lee C. Iijima|                 null|Heinz Eifel 2013 ...|    Gewürztraminer|        Heinz Eifel|
    |  9|  France|This has great de...|         Les Natures|    87| 27.0|           Alsace|             Alsace|             null|        Roger Voss|           @vossroger|Jean-Baptiste Ada...|        Pinot Gris| Jean-Baptiste Adam|
    +---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
    only showing top 10 rows
    
    


```scala
winesDF.show(truncate=false)
```

    +---+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------------------------------------------------------------------------------------+------------------+-------------------+
    |_c0|country  |description                                                                                                                                                                                                                                                                                                                |designation                                                     |points|price|province         |region_1           |region_2         |taster_name       |taster_twitter_handle|title                                                                                             |variety           |winery             |
    +---+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------------------------------------------------------------------------------------+------------------+-------------------+
    |0  |Italy    |Aromas include tropical fruit, broom, brimstone and dried herb. The palate isn't overly expressive, offering unripened apple, citrus and dried sage alongside brisk acidity.                                                                                                                                               |Vulkà Bianco                                                    |87    |null |Sicily & Sardinia|Etna               |null             |Kerin O’Keefe     |@kerinokeefe         |Nicosia 2013 Vulkà Bianco  (Etna)                                                                 |White Blend       |Nicosia            |
    |1  |Portugal |This is ripe and fruity, a wine that is smooth while still structured. Firm tannins are filled out with juicy red berry fruits and freshened with acidity. It's  already drinkable, although it will certainly be better from 2016.                                                                                        |Avidagos                                                        |87    |15.0 |Douro            |null               |null             |Roger Voss        |@vossroger           |Quinta dos Avidagos 2011 Avidagos Red (Douro)                                                     |Portuguese Red    |Quinta dos Avidagos|
    |2  |US       |Tart and snappy, the flavors of lime flesh and rind dominate. Some green pineapple pokes through, with crisp acidity underscoring the flavors. The wine was all stainless-steel fermented.                                                                                                                                 |null                                                            |87    |14.0 |Oregon           |Willamette Valley  |Willamette Valley|Paul Gregutt      |@paulgwine           |Rainstorm 2013 Pinot Gris (Willamette Valley)                                                     |Pinot Gris        |Rainstorm          |
    |3  |US       |Pineapple rind, lemon pith and orange blossom start off the aromas. The palate is a bit more opulent, with notes of honey-drizzled guava and mango giving way to a slightly astringent, semidry finish.                                                                                                                    |Reserve Late Harvest                                            |87    |13.0 |Michigan         |Lake Michigan Shore|null             |Alexander Peartree|null                 |St. Julian 2013 Reserve Late Harvest Riesling (Lake Michigan Shore)                               |Riesling          |St. Julian         |
    |4  |US       |Much like the regular bottling from 2012, this comes across as rather rough and tannic, with rustic, earthy, herbal characteristics. Nonetheless, if you think of it as a pleasantly unfussy country wine, it's a good companion to a hearty winter stew.                                                                  |Vintner's Reserve Wild Child Block                              |87    |65.0 |Oregon           |Willamette Valley  |Willamette Valley|Paul Gregutt      |@paulgwine           |Sweet Cheeks 2012 Vintner's Reserve Wild Child Block Pinot Noir (Willamette Valley)               |Pinot Noir        |Sweet Cheeks       |
    |5  |Spain    |Blackberry and raspberry aromas show a typical Navarran whiff of green herbs and, in this case, horseradish. In the mouth, this is fairly full bodied, with tomatoey acidity. Spicy, herbal flavors complement dark plum fruit, while the finish is fresh but grabby.                                                      |Ars In Vitro                                                    |87    |15.0 |Northern Spain   |Navarra            |null             |Michael Schachner |@wineschach          |Tandem 2011 Ars In Vitro Tempranillo-Merlot (Navarra)                                             |Tempranillo-Merlot|Tandem             |
    |6  |Italy    |Here's a bright, informal red that opens with aromas of candied berry, white pepper and savory herb that carry over to the palate. It's balanced with fresh acidity and soft tannins.                                                                                                                                      |Belsito                                                         |87    |16.0 |Sicily & Sardinia|Vittoria           |null             |Kerin O’Keefe     |@kerinokeefe         |Terre di Giurfo 2013 Belsito Frappato (Vittoria)                                                  |Frappato          |Terre di Giurfo    |
    |7  |France   |This dry and restrained wine offers spice in profusion. Balanced with acidity and a firm texture, it's very much for food.                                                                                                                                                                                                 |null                                                            |87    |24.0 |Alsace           |Alsace             |null             |Roger Voss        |@vossroger           |Trimbach 2012 Gewurztraminer (Alsace)                                                             |Gewürztraminer    |Trimbach           |
    |8  |Germany  |Savory dried thyme notes accent sunnier flavors of preserved peach in this brisk, off-dry wine. It's fruity and fresh, with an elegant, sprightly footprint.                                                                                                                                                               |Shine                                                           |87    |12.0 |Rheinhessen      |null               |null             |Anna Lee C. Iijima|null                 |Heinz Eifel 2013 Shine Gewürztraminer (Rheinhessen)                                               |Gewürztraminer    |Heinz Eifel        |
    |9  |France   |This has great depth of flavor with its fresh apple and pear fruits and touch of spice. It's off dry while balanced with acidity and a crisp texture. Drink now.                                                                                                                                                           |Les Natures                                                     |87    |27.0 |Alsace           |Alsace             |null             |Roger Voss        |@vossroger           |Jean-Baptiste Adam 2012 Les Natures Pinot Gris (Alsace)                                           |Pinot Gris        |Jean-Baptiste Adam |
    |10 |US       |Soft, supple plum envelopes an oaky structure in this Cabernet, supported by 15% Merlot. Coffee and chocolate complete the picture, finishing strong at the end, resulting in a value-priced wine of attractive flavor and immediate accessibility.                                                                        |Mountain Cuvée                                                  |87    |19.0 |California       |Napa Valley        |Napa             |Virginie Boone    |@vboone              |Kirkland Signature 2011 Mountain Cuvée Cabernet Sauvignon (Napa Valley)                           |Cabernet Sauvignon|Kirkland Signature |
    |11 |France   |This is a dry wine, very spicy, with a tight, taut texture and strongly mineral character layered with citrus as well as pepper. It's a food wine with its almost crisp aftertaste.                                                                                                                                        |null                                                            |87    |30.0 |Alsace           |Alsace             |null             |Roger Voss        |@vossroger           |Leon Beyer 2012 Gewurztraminer (Alsace)                                                           |Gewürztraminer    |Leon Beyer         |
    |12 |US       |Slightly reduced, this wine offers a chalky, tannic backbone to an otherwise juicy explosion of rich black cherry, the whole accented throughout by firm oak and cigar box.                                                                                                                                                |null                                                            |87    |34.0 |California       |Alexander Valley   |Sonoma           |Virginie Boone    |@vboone              |Louis M. Martini 2012 Cabernet Sauvignon (Alexander Valley)                                       |Cabernet Sauvignon|Louis M. Martini   |
    |13 |Italy    |This is dominated by oak and oak-driven aromas that include roasted coffee bean, espresso, coconut and vanilla that carry over to the palate, together with plum and chocolate. Astringent, drying tannins give it a rather abrupt finish.                                                                                 |Rosso                                                           |87    |null |Sicily & Sardinia|Etna               |null             |Kerin O’Keefe     |@kerinokeefe         |Masseria Setteporte 2012 Rosso  (Etna)                                                            |Nerello Mascalese |Masseria Setteporte|
    |14 |US       |Building on 150 years and six generations of winemaking tradition, the winery trends toward a leaner style, with the classic California buttercream aroma cut by tart green apple. In this good everyday sipping wine, flavors that range from pear to barely ripe pineapple prove approachable but not distinctive.       |null                                                            |87    |12.0 |California       |Central Coast      |Central Coast    |Matt Kettmann     |@mattkettmann        |Mirassou 2012 Chardonnay (Central Coast)                                                          |Chardonnay        |Mirassou           |
    |15 |Germany  |Zesty orange peels and apple notes abound in this sprightly, mineral-toned Riesling. Off dry on the palate, yet racy and lean, it's a refreshing, easy quaffer with wide appeal.                                                                                                                                           |Devon                                                           |87    |24.0 |Mosel            |null               |null             |Anna Lee C. Iijima|null                 |Richard Böcking 2013 Devon Riesling (Mosel)                                                       |Riesling          |Richard Böcking    |
    |16 |Argentina|Baked plum, molasses, balsamic vinegar and cheesy oak aromas feed into a palate that's braced by a bolt of acidity. A compact set of saucy red-berry and plum flavors features tobacco and peppery accents, while the finish is mildly green in flavor, with respectable weight and balance.                               |Felix                                                           |87    |30.0 |Other            |Cafayate           |null             |Michael Schachner |@wineschach          |Felix Lavaque 2010 Felix Malbec (Cafayate)                                                        |Malbec            |Felix Lavaque      |
    |17 |Argentina|Raw black-cherry aromas are direct and simple but good. This has a juicy feel that thickens over time, with oak character and extract becoming more apparent. A flavor profile driven by dark-berry fruits and smoldering oak finishes meaty but hot.                                                                      |Winemaker Selection                                             |87    |13.0 |Mendoza Province |Mendoza            |null             |Michael Schachner |@wineschach          |Gaucho Andino 2011 Winemaker Selection Malbec (Mendoza)                                           |Malbec            |Gaucho Andino      |
    |18 |Spain    |Desiccated blackberry, leather, charred wood and mint aromas carry the nose on this full-bodied, tannic, heavily oaked Tinto Fino. Flavors of clove and woodspice sit on top of blackberry fruit, then hickory and other forceful oak-based aromas rise up and dominate the finish.                                        |Vendimia Seleccionada Finca Valdelayegua Single Vineyard Crianza|87    |28.0 |Northern Spain   |Ribera del Duero   |null             |Michael Schachner |@wineschach          |Pradorey 2010 Vendimia Seleccionada Finca Valdelayegua Single Vineyard Crianza  (Ribera del Duero)|Tempranillo Blend |Pradorey           |
    |19 |US       |Red fruit aromas pervade on the nose, with cigar box and menthol notes riding in the back. The palate is slightly restrained on entry, but opens up to riper notes of cherry and plum specked with crushed pepper. This blend of Merlot, Cabernet Sauvignon and Cabernet Franc is approachable now and ready to be enjoyed.|null                                                            |87    |32.0 |Virginia         |Virginia           |null             |Alexander Peartree|null                 |Quiévremont 2012 Meritage (Virginia)                                                              |Meritage          |Quiévremont        |
    +---+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------------------------------------------------------------------------------------+------------------+-------------------+
    only showing top 20 rows
    
    


```scala
winesDF.show(5, truncate=10)
```

    +---+--------+-----------+-----------+------+-----+----------+----------+----------+-----------+---------------------+----------+----------+----------+
    |_c0| country|description|designation|points|price|  province|  region_1|  region_2|taster_name|taster_twitter_handle|     title|   variety|    winery|
    +---+--------+-----------+-----------+------+-----+----------+----------+----------+-----------+---------------------+----------+----------+----------+
    |  0|   Italy| Aromas ...| Vulkà B...|    87| null|Sicily ...|      Etna|      null| Kerin O...|           @kerino...|Nicosia...|White B...|   Nicosia|
    |  1|Portugal| This is...|   Avidagos|    87| 15.0|     Douro|      null|      null| Roger Voss|           @vossroger|Quinta ...|Portugu...|Quinta ...|
    |  2|      US| Tart an...|       null|    87| 14.0|    Oregon|Willame...|Willame...| Paul Gr...|           @paulgw...|Rainsto...|Pinot Gris| Rainstorm|
    |  3|      US| Pineapp...| Reserve...|    87| 13.0|  Michigan|Lake Mi...|      null| Alexand...|                 null|St. Jul...|  Riesling|St. Julian|
    |  4|      US| Much li...| Vintner...|    87| 65.0|    Oregon|Willame...|Willame...| Paul Gr...|           @paulgw...|Sweet C...|Pinot Noir|Sweet C...|
    +---+--------+-----------+-----------+------+-----+----------+----------+----------+-----------+---------------------+----------+----------+----------+
    only showing top 5 rows
    
    

Something is wrong with the data, yay! All the types right now should be `String` for now


```scala
import org.apache.spark.sql.types._

val wineSchema = new StructType(Array(
      new StructField("_c0", StringType, nullable = true),
      new StructField("country", StringType, nullable = true),
      new StructField("description", StringType, nullable = true),
      new StructField("designation", StringType, nullable = true),
      new StructField("points", StringType, nullable = true),//*
      new StructField("price", StringType, nullable = true),//*
      new StructField("province", StringType, nullable = true),
      new StructField("region_1", StringType, nullable = true),
      new StructField("region_2", StringType, nullable = true),
      new StructField("taster_name", StringType, nullable = true),
      new StructField("taster_twitter_handle", StringType, nullable = true),
      new StructField("title", StringType, nullable = true),
      new StructField("variety", StringType, nullable = true),
      new StructField("winery", StringType, nullable = true)
    ))

val winesDF = spark.read.format("csv")             
                     .option("header", "true")
                     .schema(wineSchema)
                     .load("../data/winemag.csv")

winesDF.show(10, truncate=30)
```

    +---+--------+------------------------------+------------------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+------------------------------+------------------+-------------------+
    |_c0| country|                   description|                   designation|points|price|         province|           region_1|         region_2|       taster_name|taster_twitter_handle|                         title|           variety|             winery|
    +---+--------+------------------------------+------------------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+------------------------------+------------------+-------------------+
    |  0|   Italy|Aromas include tropical fru...|                  Vulkà Bianco|    87| null|Sicily & Sardinia|               Etna|             null|     Kerin O’Keefe|         @kerinokeefe|Nicosia 2013 Vulkà Bianco  ...|       White Blend|            Nicosia|
    |  1|Portugal|This is ripe and fruity, a ...|                      Avidagos|    87| 15.0|            Douro|               null|             null|        Roger Voss|           @vossroger|Quinta dos Avidagos 2011 Av...|    Portuguese Red|Quinta dos Avidagos|
    |  2|      US|Tart and snappy, the flavor...|                          null|    87| 14.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|          @paulgwine |Rainstorm 2013 Pinot Gris (...|        Pinot Gris|          Rainstorm|
    |  3|      US|Pineapple rind, lemon pith ...|          Reserve Late Harvest|    87| 13.0|         Michigan|Lake Michigan Shore|             null|Alexander Peartree|                 null|St. Julian 2013 Reserve Lat...|          Riesling|         St. Julian|
    |  4|      US|Much like the regular bottl...|Vintner's Reserve Wild Chil...|    87| 65.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|          @paulgwine |Sweet Cheeks 2012 Vintner's...|        Pinot Noir|       Sweet Cheeks|
    |  5|   Spain|Blackberry and raspberry ar...|                  Ars In Vitro|    87| 15.0|   Northern Spain|            Navarra|             null| Michael Schachner|          @wineschach|Tandem 2011 Ars In Vitro Te...|Tempranillo-Merlot|             Tandem|
    |  6|   Italy|Here's a bright, informal r...|                       Belsito|    87| 16.0|Sicily & Sardinia|           Vittoria|             null|     Kerin O’Keefe|         @kerinokeefe|Terre di Giurfo 2013 Belsit...|          Frappato|    Terre di Giurfo|
    |  7|  France|This dry and restrained win...|                          null|    87| 24.0|           Alsace|             Alsace|             null|        Roger Voss|           @vossroger|Trimbach 2012 Gewurztramine...|    Gewürztraminer|           Trimbach|
    |  8| Germany|Savory dried thyme notes ac...|                         Shine|    87| 12.0|      Rheinhessen|               null|             null|Anna Lee C. Iijima|                 null|Heinz Eifel 2013 Shine Gewü...|    Gewürztraminer|        Heinz Eifel|
    |  9|  France|This has great depth of fla...|                   Les Natures|    87| 27.0|           Alsace|             Alsace|             null|        Roger Voss|           @vossroger|Jean-Baptiste Adam 2012 Les...|        Pinot Gris| Jean-Baptiste Adam|
    +---+--------+------------------------------+------------------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+------------------------------+------------------+-------------------+
    only showing top 10 rows
    
    




    import org.apache.spark.sql.types._
    wineSchema: org.apache.spark.sql.types.StructType = StructType(StructField(_c0,StringType,true), StructField(country,StringType,true), StructField(description,StringType,true), StructField(designation,StringType,true), StructField(points,StringType,true), StructField(price,StringType,true), StructField(province,StringType,true), StructField(region_1,StringType,true), StructField(region_2,StringType,true), StructField(taster_name,StringType,true), StructField(taster_twitter_handle,StringType,true), StructField(title,StringType,true), StructField(variety,StringType,true), StructField(winery,StringType,true))
    winesDF: org.apache.spark.sql.DataFrame = [_c0: string, country: string ... 12 more fields]
    


