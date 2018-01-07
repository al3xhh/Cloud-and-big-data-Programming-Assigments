# Cloud and big data: Programming Assignments

<p align="justify">This are the Programming Assignments for the subject Cloud and Big Data at Complutense University of Madrid, course 2017/2018. The following sentences are the statement of each file:</p>

<h2>MapReduce</h2>

<ul>
  <li><p align="justify">P11 -> Develop a distributed version of the grep tool to search words in very large documents. The output should be the numbers of the lines that match a given pattern.</p></li>
  <li><p align="justify">P12 -> Develop a MapReduce job to find the frequency of each URL in a web server log. The output should be the URLs and their frequency.</p></li>
  <li><p align="justify">P13 -> Write a MapReduce job to calculate the average daily stock price at close of Alphabet Inc. (GOOG) per year since 2009. The output should be the year and the average price.</p></li>
  <li><p align="justify">P14 -> Develop a MapReduce job to show movies with an average rating in the ranges:</p>
    <ul>
      <li>(1) 1 or lower</li>
      <li>(2) 2 or lower (but higher than 1)</li>
      <li>(3) 3 or lower (but higher than 2)</li>
      <li>(4) 4 or lower (but higher than 3)</li>
      <li>(5) 5 or lower (but higher than 4)</li>
    </ul>
  <p align="justify">The job should have two MapReduce phases. The output of the first phase should be the movies and their average rating. The output of the second phase should be ranges and the title of the movies.</p></li>
    <li><p align="justify">P15 -> Pig and Hive are higher-level abstractions of MapReduce. They provide an interface that has nothing to do with “map” or “reduce,” but the systems interpret the higher-level language into a series of MapReduce jobs. Much like how a query planner in an RDBMS translates SQL into actual operations on data, Hive and Pig translate their respective languages into MapReduce operations.
Discuss in detail how these two tools could be used to simplify the development of the “Movie Rating Data” exercise.</p></li>
</ul>

<h2>Spark</h2>

<ul>
  <li><p align="justify">P21 -> Develop a Spark version of the grep tool to search words in very large documents. The output should be the numbers of the lines that match a given pattern.</p></li>
  <li><p align="justify">P22 -> Develop a Spark script to find the frequency of each URL in a web server log. The output should be the URLs and their frequency.</p></li>
  <li><p align="justify">P23 -> Write a Spark script to calculate the average daily stock price at close of Alphabet Inc. (GOOG) per year since 2009. The output should be the year and the average price. </p></li>
  <li><p align="justify">P24 -> Develop a Spark script to show movies with an average rating in the ranges:</p>
    <ul>
      <li>(1) 1 or lower</li>
      <li>(2) 2 or lower (but higher than 1)</li>
      <li>(3) 3 or lower (but higher than 2)</li>
      <li>(4) 4 or lower (but higher than 3)</li>
      <li>(5) 5 or lower (but higher than 4)</li>
    </ul>
  <p align="justify">The job should have two MapReduce phases. The output of the first phase should be the movies and their average rating. The output of the second phase should be ranges and the title of the movies.</p></li>
  <li><p align="justify">P25 -> Develop a Spark script to answer the following questions:
    <ul>
      <li>What is the average rating given by each user?.</li>  
      <li>What is the overall average rating?</li> 
      <li>What is the average rating of each movie?.</li> 
      <li>What is the average rating of each genres?.</li> 
      <li>Which are the top 10?. Which are the top 10 each month?</li> 
    </ul>
  </p></li>
  </ul>
</ul>

<h2>P3. Meteorite Landing</h2>
<p align="justify">
The NASA’s Open Data Portal hosts a comprehensive data set from The Meteoritical Society that contains information on all of the known meteorite landings. The Table consists of 34,513 meteorites and includes fields like the type of meteorite, the mass and the year.
https://data.nasa.gov/Space-Science/Meteorite-Landings/gh4g-9sfh
Write a MapReduce job and a Spark script to calculate the average mass per year of a type of meteorite specified as an argument.</p>

<h2>P4. Distributed Markov Shakespeare</h2>
<p align="justify">In this exercise we will explore the application of function compositions, narrow and wide dependencies and stages in the DAG parallelism for a slightly involved distributed computation to gain further insights into this programming approach.
The task is to build a simple statistical language model for the writing style of Shakespeare. Your model should be a simple Markov Chain of order 2. You will use that model to generate novel sentences based on Shakespeare’s original texts.</p>
