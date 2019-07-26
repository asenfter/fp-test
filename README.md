#Testing fastparquet

I deal with a lot of sensor data which I partition by several attributes in order to increase flexibility.
E.g. partitioning allows me limiting the data by regular expressions which should be loaded and processed.
In a nutshell it's somehow structured like this:
        
    .
    +-- sensor=1
    |   +-- year=2017
    |       +-- part.0.parquet
    |   +-- year=2018
    |       +-- part.0.parquet   
    +-- sensor=2
        +-- year=2017
            +-- part.0.parquet
        +-- year=2018
            +-- part.0.parquet


 
All parquet files have the same columns. One out of it is the column `value` where the actual sensor values are stored. 
All values are floats. But because of historical reasons the dtype of `value` is `object` (with float as string values) 
for the year 2017 and `float` for 2018.   

When I use fastparqet to load the data to pandas DataFrame I actually have the problem that depending 
on the order the files are feeded to fastparquet the app will work or even end in an error:
 * First load float data followed by string data works  
 * First load string data followed by float data ends in a TypeError

        TypeError: expected array of bytes
  

To demonstrate the issue i wrote an unittest in test_fastparquet.py

The script `run.sh` creates a virtual environment with all required packages and executes the tests.

 

