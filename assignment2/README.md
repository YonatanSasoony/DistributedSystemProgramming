# DistributedSystemProgramming

## Submitted by 
Yonatan Sasoony 205916265 sasoony@post.bgu.ac.il  
Yossy Carmeli 204752406 yossy@post.bgu.ac.il

## How To Run Our Project?
java -cp ExtractCollations.jar ExtractCollations minPmi relMinPmi
- minPmi is the minimal pmi.
- relMinPmi is the relative minimal pmi.

## How The Program Works?
    
A collocation is a sequence of words or terms that co-occur more often than would be expected by
chance. The identification of collocations - such as 'crystal clear', 'cosmetic surgery', 'סביבה איכות - 'is
essential for many natural language processing and information extraction application.
    
We were asked to code a map-reduce job for collocation extraction for each decade, and run it in the Amazon Elastic MapReduce service. 
 The collocation criteria will be based on the normalized PMI value:
  *  Minimal pmi: in case the normalized PMI is equal or greater than the given minimal PMI
     value (denoted by minPmi), the given pair of two ordered words is considered to be a collocation.
  * Relative minimal pmi: in case the normalized PMI value, divided by the sum of all
    normalized pmi in the same decade (including those which their normalized PMI is less
    than minPmi), is equal or greater than the given relative minimal PMI value (denoted by
    relMinPmi), the given pair of two ordered words is considered to be a collocation.

Our map-reduce job will get the minPmi and the relMinPmi values as parameters.
We Calculated the normalized pmi of each pair of words in each decade, and display all collations above each of the two minimum inputs. 
We Ran our experiments on the 2-grams Hebrew corpus. 
The input of the program is the Hebrew 2-Gram dataset of Google Books Ngrams. 
The output of the program is a list of the collocations for each decade, and there npmi value, ordered by their npmi (descending).
    
A Job Flow is a collection of processing steps that Amazon Elastic MapReduce runs on a specified dataset using a set of Amazon EC2 instances. 
A Job Flow consists of one or more steps, each of which must complete in sequence successfully, for the Job Flow to finish.
    
A Job Flow Step is a user-defined unit of processing, mapping roughly to one algorithm that manipulates the data. 
A step is a Hadoop MapReduce application implemented as a Java jar or a streaming program written in Java, Ruby, Perl, Python, PHP, R, or C++.  

### The Steps:
    1. CalcCw1w2N: calculates N for each decade and C(w1w2) for each bigram per decade, and filters bigrams using a stop-words list.
    2. CalcCw1: calculates C(w1) for each bigram per decade.
    3. CalcCw2: calculates C(w2) for each bigram per decade.
    4. CalcNpmi: calculates npmi value for each bigram per decade.
    5. Filter: filters only bigrams that found as collocations, per decade.
    6. Sort: sorts the filtered bigrams per decade and displays their npmi value, descending.
    
    
### Reports:
* For each decade, the top-10 collocations and there npmi value, ordered by npmi (descending):
    - attached a file, top10.txt
* The number of key-value pairs that were sent from the mappers to the reducers in your map-reduce runs, and their size with and without local aggregation:
    1. with local aggregation:
       * Total map output records: 415,796,445
       * Total map output bytes: 12,564,425,267
    2. without local aggregation:
        * Total Map output records:  486,043,711
        * Total Map output bytes:  12,564,425,267
    
* A list of 5 good collocations and a list of 5 bad collocations, you manually collected from the system output. In the frontal checking you will be asked to say something on why wrong collocations were extracted (a manual analysis).  
    Good collocations:
    * 2000s: כקליפת השום 
    * 1540s: אותות ומופתים 
    * 1800s: נושאי כליו 
    * 2000s: טיפין טיפין 
    * 1750s: הפלא ופלא 
    
    Bad Collocations:
    * 1530s: ועכשיו אתה 
    * 1530s: אתמול אמרת 
    * 1700s: הנך רואה 
    * 1780s: יעברו אליך 
    * 1980s: לרדיו ולטלוויזיה 

