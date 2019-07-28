# shazam-tag-aggregator

# Overview:
This project is a Spark based application written in python to clean raw data in json which is a 3 hour snapshot of the user activity registered in the US. The output of the spark job is determined based on a the set of command line parameters passed by the user at the time of running the spark application. 
For Ex: 
1. A command, which will be either "chart" or "state_chart"
2. A limit, which will determine the number of tracks to output

For example, giving the parameters:
chart 5
Should output the top 5 most tagged tracks irrespective of user location. The output should
look something like this :
![GLOBAL CHARTS](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/chart_global.png)

Similarly, giving the parameters:
state_chart 3
Should output the top 3 tracks in each and every US state.
