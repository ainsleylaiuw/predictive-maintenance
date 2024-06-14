# predictive-maintenance
Project that showcases the use of a 'data lake' in conjunction with ML

The goal is to create a data pipeline that takes data from a source (csv files housed in Amazon S3) and query them in and maybe make new features with SQL. 

After querying/preprocessing in a Jupyter Notebook, build a ML model that predicts when to do maintenance. This can probably use something like SageMaker.

But for now, since we can work with the datasets locally, just do the ML work locally to see if it will work, then I can make AWS account and start the free trial timer to put the data in S3 and try to query it into my code (perhaps get the data into a local sql table for me to practice on?). In other words, the AWS data housing and SQL queries will be basically 'cosmetic' and only done to simulate industry work. I need to figure out how SageMaker works and how to get it to run my ML model.

Bulk of the work will be figuring out how to preprocess/make features with SQL/python and set up the ML. After that, it will just be setting up the connections.

Perhaps make Tableau visualization at the end?

This should hit many of the topics that industry jobs ask for.