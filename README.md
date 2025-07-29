# CS-167-Twitter-Data-Analysis
## Group 7 Project D: Twitter Data Analysis
By: Francis Cuarteros (fcuar002), Eve Fraczkiewicz (efrac003), Joewe Low (jlow004), and Nathaniel Natividad (nnati004)

### Task 1: Data Preparation 1 (Francis)

First I loaded the json file into a dataframe using the first argument in the command line as the input file.

![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task1_1.png))
![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task1_2.png))

Then I used the createOrReplaceTempView to make the following SQL query:

![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task1_3.png))

Printing the schema for that query gives us this

![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task1_4.png))
![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task1_5.png))

Then we explode the hashtags and run another query to get the top 20 keywords

![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task1_6.png))

Top 20 keywords (10k Dataset): [ALDUBxEBLoveis], [FurkanPalalı], [no309], [LalOn], [chien], [job], [Hiring], [sbhawks], [Top3Apps], [perdu], [trouvé], [CareerArc], [Job], [trumprussia], [trndnl], [Jobs], [hiring], [impeachtrumppence], [ShowtimeLetsCelebr8], [music]

### Task 2: Data Preparation 2 (Eve)

Using scala and sparkSQL, I added a new “topic” column to tweets_clean that contains the topic of each tweet. I accomplished this by first finding the top 20 most frequent hashtags in tweets_clean:

![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task2_1.png))

 Then I selected the necessary columns. In the selection, I also created the topic column, by first converting the list of top 20 hashtags into a comma separated list, and then converting that into an array. Then I used array_intersect to find the intersection between the hashtags column and the new top 20 hashtags array. Since there was a possibility of having multiple hashtags in that intersection, I used element_at to only pick the first hashtag in the intersection. Finally I used COALESCE to return an empty string if there is no common hashtag found for a record.

![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task2_2.png))
 
Afterwards, I used WHERE to make sure to only select the records with at least one topic. For the 10k dataset, I found that the output, tweets_topic, had a total of 257 records.

### Task 3: Topic Prediction (Joewe)

For this task, I first constructed a machine learning pipeline that included a tokenizer, a HashingTF transformer, a StringIndexer, and a Logistic Regression classifier. The tokenizer breaks down tweet text into individual words, and then the transformer maps these words to numeric feature vectors. The StringIndexer converts text topics into numeric indices. I split up the data between a training set and a test set. Finally, these features are used by a Logistic Regression model, which will then predict topics. I evaluated the predictions by calculating accuracy, precision, and recall.

Precision = 0.8095238095238095
Recall = 0.9444444444444444

### Task 4: Temporal Analysis (Nate)
Using java and spark SQL, this part of the project shows the countries with more than 50 tweets and being between a certain date range (formatted: MM/DD/YYYY). The bar chart below gets its information from the tweets_clean.json file that is written from the queries in the TaskD.java file. The bar chart varies for the given start and end date.

![alt text]([http://url/to/img.png](https://github.com/evefraczkiewicz/CS-167-Twitter-Data-Analysis/blob/79529394f14e7e2322c03cff7bae3580fd365abf/screenshots/task4_1.png))
 
Date Range Used: 10/1/2017 - 10/31/2017
The bar chart above visualizes the data for all the countries with more than 50 tweets in October 2017.
