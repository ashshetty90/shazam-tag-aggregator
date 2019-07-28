# shazam-tag-aggregator

# Overview:
This project is a Spark based application written in python to clean raw data in json which is a 3 hour snapshot of the user activity registered in the US. The output of the spark job is determined based on a the set of command line parameters passed by the user at the time of running the spark application. 

# Requirement:

For Ex: 
1. A command, which will be either "chart" or "state_chart"
2. A limit, which will determine the number of tracks to output

For example, giving the parameters:
chart 5
Should output the top 5 most tagged tracks irrespective of user location. The output should
look something like this :
![GLOBAL CHARTS](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/chart_global.png)

Similarly, giving the parameters:
state_chart 1
Should output the top track in each and every US state similar to :
![STATE CHARTS](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/chart_state.png)

# Assumptions:

1) After closing analyzing the data its seen that there are different client types, hence the assumption here is client type other than *SHAZAM* also need to be considered in the calculation
2) For scenario-2, the geolocation information is used to determine the state level information of each user activity. Hoever there are a lot of null geolcation data (14909 records to be precise), this will however were not considered as the they did not have any state level information in them
3) This project is an MVP and no memory tweaks were done on the spark side to scale this application up as all default spark configurations were used.

# Architecture:
The Architecture involves the following tech stack.
- Spark v2.4
- Python v3.7.2

I decided to go on with this architecture based on the scope of the project as it gives us better scalability and maintainability.

# What can be done better

1) Use more real time data sources like S3,FileSystems,Database,Kafka or Firehose.
2) Dump the result into a persistent storage than just displaying the results over the command line.
3) Slack/Pager Duty integrations can be added for alert and monitoring

# How to run this application
```sh
### PySpark Application to clean raw data in json format in batch or realtime mode
### Clone the repository [https://github.com/ashshetty90/shazam-tag-aggregator.git]
### Please note that this set-up is valid only for Mac OS machines

### Firstly, make sure you have installed home brew on your local machine . If not please use the below command to install it:
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

Python3 needs xcode as well so installing it
$ xcode-select --install
### Some basic checks
$ brew doctor
### Now lets move to installing the latest python on the machine :
$ brew install python3
### Running the below command will return you the latest python3 version
$ python3 --version 

### Moving on to installing spark in the local machine 

Firstly, you would need java8 installed on your machine, I have installed openjdk8 using:

$ brew cask install homebrew/cask-versions/adoptopenjdk8

