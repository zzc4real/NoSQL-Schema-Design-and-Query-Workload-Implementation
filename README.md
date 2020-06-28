# Task summary

• Design a schema for each storage system based on data and query feature; Implement all queries in each system;
• For each system, select two queries to provide an alternative implementation; 
• Compare the execution performance of the two implementations;
• Document the schema design, query design and performance analysis in a report;


# Data
The data that you will use is the latest dump (publication date: 2018-06-05) of the Artificial Intelligence Stack Exchange question and answer site (https://ai.stackexchange. com/). The dump is released and maintained by stackexchange: https://archive.org/ details/stackexchange. The original dump contains many files in XML format. The assignment uses a subset of the data stored in five csv files. 

• Posts.csv stores information about post; each row represents a post, which could be a question or an answer.

• Users.csv stores user’s profile; each row represents a user, a user can be the author of a post or an answer.

• Topic: Each question may belong to a few topics. The topic(s) of a question are recorded as a list of keywords in the Tags column in Posts.csv. Both answers and comments belong to this questions have the same topic(s) as the question.

• User: Questions, answers and comments are all made by registered users. Users are identified by UserId field in various CSV files. Some users are removed for various reasons. The removed users no longer have an Id and should be ignored in all queries.
