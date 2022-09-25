# Hadoop_MapReduce

##  Context 
The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models.
A MapReduce job usually splits the input data-set into independent chunks which are processed by the map tasks in a completely parallel manner. The framework sorts the outputs of the maps, which are then input to the reduce tasks. Typically both the input and the output of the job are stored in a file-system. The framework takes care of scheduling tasks, monitoring them and re-executes the failed tasks.
In my exemple, I will implements three solution (Anagrams, Sales analysis and Breadth-first search) using hadoop MapReduce with Java.

##  Anagrams

OBJECTIVE: We have a list of common english words. We wish to pinpoint which words are anagrams of one another.

As a reminded, two words are anagrams if their letters are the same but in different orders (such as « melon » and « lemon », for example).

- Download the file data using the command: wget http://cours.tokidev.fr/bigdata/tps/common_words_en_subset.txt


##  Sales analysis


- Download the file data using the command: wget http://cours.tokidev.fr/bigdata/tps/sales_world_10k.csv

- It is a CSV file. Please check its format; it has many columns. Also note it has a
header ligne, which may be problematic when loading the file from your Hadoop
implementation.

- We are interested in specifically five columns: the sales region (« Region »), the
sales country (« Country »), the type of item bought (« Item Type »), the sales
channel (« Sales channel » : online or offline), and finally the total sale profit
(« Total profit »).

- My program is able to perform the following tasks:
      ● Obtain the total profit for any given world region.
      ● Obtain the total profit for any given country.
      ● Obtain the total profit for any given item type.
- For each item type :
      ● How many sales were performed online.
      ● How many sales were performed offline.
             … and for each of those quantities, how much the combined total profit for those sales was.
             
             
              
##  Breadth-first search


- Download the file data using the command: wget http://cours.tokidev.fr/bigdata/tps/graph_input.txt

Here I Implement the graph search algorithm :

![This is an image](/Graph2.png)

the algorithm will run in iterations

![This is an image](/Graph1.png)


             
             
