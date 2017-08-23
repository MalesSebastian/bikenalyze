from operator import add
from datetime import datetime

from pyspark import SparkContext, SparkConf

import plotly
import plotly.graph_objs as go
from plotly import tools


# Check if a ride was going on in a weekday
# given the correspondent line from dataset
def check_weekend(line):
    date_and_time_1 = line.split(",")[2]
    date_and_time_2 = line.split(",")[5]
    date_1 = datetime.strptime(date_and_time_1.split(" ")[0], "%m/%d/%Y")
    date_2 = datetime.strptime(date_and_time_2.split(" ")[0], "%m/%d/%Y")
    if date_1.weekday() >= 5 and date_2.weekday() <= 6:
        return True
    return False


# Find hour where an interval would be fitted
def fit_hour(line):
    hour_1 = line.split(",")[2].split(" ")[1]
    hour_2 = line.split(",")[5].split(" ")[1]
    hour_obj_1 = datetime.strptime(hour_1, "%H:%M")
    hour_obj_2 = datetime.strptime(hour_2, "%H:%M")
    if hour_obj_2.minute > (60 - hour_obj_1.minute):
        return hour_obj_2.hour
    return hour_obj_1.hour


# Initialize Spark
conf = SparkConf().setAppName("Bike").setMaster("local")
sc = SparkContext(conf=conf)

# Retrieve dataset from HDFS and eliminate the first row in the RDD
dataSet = sc.textFile("hdfs://localhost:9000/201608_trip_data.csv")
first = dataSet.first()
fullset = dataSet.filter(lambda line: line != first).cache()

# Calculate the avarage duration of a ride
time_rdd = fullset.map(lambda line: int(line.split(",")[1])).cache()
sums = time_rdd.reduce(add)
seconds = sums / time_rdd.count()

# Calculate the busiest hours
hours_pair = fullset.map(lambda line: (fit_hour(line), 1))
hours_results = hours_pair.reduceByKey(lambda a, b: a + b).sortBy(lambda a:
                                                                  a[1]).cache()

# Calculate the busiest hours on weekend days
date_and_hours_for_weekend = fullset.filter(lambda line: check_weekend(line))
hours_pair_weekend = date_and_hours_for_weekend.map(
    lambda line: (fit_hour(line), 1))
hours_results_weekend = hours_pair_weekend.reduceByKey(
    lambda a, b: a + b).sortBy(lambda a: a[1]).cache()

# Calculate the hours with the longes rides
hour_and_duration = fullset.map(lambda line: (fit_hour(line),
                                              int(line.split(",")[1])))
hour_and_durations_sumsav = hour_and_duration.aggregateByKey(
    (0, 0), lambda a, b: (a[0] + b, a[1] + 1),
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)
averages = hour_and_durations_sumsav.mapValues(lambda x: x[0] / x[1]).cache()

# Plot data
trace1 = go.Bar(
    x=[t for t in hours_results.keys().toLocalIterator()],
    y=[t for t in hours_results.values().toLocalIterator()],
    name="Overall"
)

trace2 = go.Bar(
    x=[t for t in hours_results_weekend.keys().toLocalIterator()],
    y=[t for t in hours_results_weekend.values().toLocalIterator()],
    name="Weekdays"
)

trace3 = go.Bar(
    x=[t for t in averages.keys().toLocalIterator()],
    y=[t for t in averages.values().toLocalIterator()],
    name="Hours with longes rides"
)

fig = tools.make_subplots(rows=3, cols=1)
fig.append_trace(trace1, 1, 1)
fig.append_trace(trace2, 2, 1)
fig.append_trace(trace3, 3, 1)

fig['layout'].update(height=600, width=1200, title='San Francisco bike rides')

plotly.offline.plot(figure_or_data=fig)
