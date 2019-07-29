# shazam-tag-aggregator

# Overview:
This project is a Spark based application written in python to clean raw data in json which is a 3 hour snapshot of the user activity registered in the US. The output of the spark job is determined based on a the set of command line parameters passed by the user at the time of running the spark application. 

# Requirement:

A command line application that would be taking to three parameters as input displays the result:

For example, giving the parameters:
chart 5 raw.json
Should output the top 5 most tagged tracks irrespective of user location. The output should
look something like this :
![GLOBAL CHARTS](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/chart_global.png)

Similarly, giving the parameters:
state_chart 1 raw.json
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

You would need java8 installed on your machine, I have installed openjdk8 using:

$ brew cask install homebrew/cask-versions/adoptopenjdk8

Install apache-spark
$ brew install apache-spark

### In case you have more than one version of python installed in your machine, make sure you enforce python3 as the default python version for your py-spark applications by adding :

export PYSPARK_PYTHON=/usr/local/bin/python3  in the .bash_profile or /usr/local/Cellar/apache-spark/2.4.3/libexec/conf/spark-env.sh.template (Or spark-env.sh whichever is available)

### Install dependecies from Pipfile
$ pipenv install

### Run the tests
$ python3 -m unittest tests/test_shazam_data.py

### Running the application
$ /usr/local/Cellar/apache-spark/2.4.3/bin/spark-submit --py-files <complete path to the driver.py file>/driver.py <complete path to the driver.py file>/driver.py <chart type 'chart' or 'state_chart'> <no of records ot be displayed> <complete path to the raw json file>/shazamtagdata.json
For Ex:
$ /usr/local/Cellar/apache-spark/2.4.3/bin/spark-submit --py-files /User/Workspace/app/driver.py /User/Workspace/app/driver.py chart 10 /User/Workspace/app/raw_json/shazamtagdata.json

```
# Troubleshooting
Considering that you would be running this application locally,the logs for the same can be found at :
http://192.168.1.3:4040/jobs/
(Make sure port 4040 is not used by any other application)

# Screenshots
### Initial Steps
Install open jdk8
![BREW_INSTALL_OPENJDK](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/opne-jdk-install.png "INSTALL OPENJDK")

brew install apache spark
![BREW_INSTALL_APACHESPARK](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/install-apache-spark.png "BREW INSTALL APACHE SPARK")

validate apache spark
![VALIDATE_APACHESPARK](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/spark-installed.png "VALIDATE APACHE SPARK")

python unit test cases
![PYTHON_UNITTESTCASES](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/unit-test-cases.png "PYTHON_UNITTESTCASES")

application ui
![SPARK LOCAL UI](https://github.com/ashshetty90/shazam-tag-aggregator/blob/master/images/spark_local_ui.png "SPARK LOCAL UI")





