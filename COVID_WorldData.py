# -*- coding: utf-8 -*-
"""
Created on Mon May 18 11:11:57 2020

@author: Dinesh
"""
import pandas as pd
import csv
from pyspark.sql import SparkSession, SQLContext
import PandasToSpark as pts
from pyspark.sql.functions import *
from pyspark.sql.types import *
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import plotly.io as pio
#pio.renderers.default = 'svg'
pio.renderers.default = 'browser'
import dash
import dash_core_components as dcc
import dash_html_components as html

df = pd.read_csv('https://covid.ourworldindata.org/data/owid-covid-data.csv')
df.to_csv('COVID_World.csv',index=False, quoting=csv.QUOTE_ALL)
cols = ["location", "date", "total_cases", "new_cases"]
df = df[cols]

sc = SparkSession.builder.getOrCreate()
sqlCtx = SQLContext(sc)

#spark_df = sc.createDataFrame(dfcsv)
covid_spark_df = pts.pandas_to_spark(df)
covid_spark_df.registerTempTable('covid_world_data')

query = "select location as country, cast(date as date), cast(total_cases as int), cast(new_cases as int) from covid_world_data where date > '2020-05-20' and location <> 'World'"
covid_spark_filter_df = sqlCtx.sql(query)

# covid_spark_df.registerTempTable('covid_world_data_tmp')
# query = "select location as country, date, total_cases, new_cases from covid_world_data where date > '2020-05-10'"
# covid_spark_filter_df = sqlCtx.sql(query)

#covid_spark_filter_df = covid_spark_filter_df.withColumn("new_cases",col("covid_spark_filter_df").cast(LongType))

cols = ["date", "new_cases"]
covid_spark_filter_df = covid_spark_filter_df.orderBy(cols, ascending=False)

#covid_spark_filter_df.toPandas().to_csv('COVID_WORLD_Summary.csv',quoting=csv.QUOTE_ALL)
covid_spark_filter_df.show()

covid_summary_df = covid_spark_filter_df.toPandas()

covid_date_top = covid_summary_df.groupby("date").apply(lambda x: x.sort_values(["date","new_cases"], ascending = False)).reset_index(drop=True)
covid_date_top = covid_date_top.groupby('date').head(5).sort_values(["date","new_cases"], ascending=False)

covid_date_top.to_csv('COVID_WORLD_Top.csv',quoting=csv.QUOTE_ALL)

# covid_date_top.dtypes
# covid_date_top['date'] = covid_date_top['date'].dt.strftime('%Y-%m-%d')

fig = go.Figure()
fig = make_subplots()
bars = []
for name, group in covid_date_top.groupby('date'):
    trace = go.Bar(x=group["country"],
                   y=group["new_cases"],
                   name=str(name))
    fig.add_trace(trace)
    fig.update_layout(barmode='group')
    bars.append(dcc.Graph(figure=fig))

#fig.show()
    
# fig = go.Figure()
# fig.add_trace(go.Bar(x=covid_summary_df["country"],
#                      y=covid_summary_df["total_cases"],
#                      name="Total Cases"))
# fig.add_trace(go.Bar(x=covid_summary_df["country"],
#                      y=covid_summary_df["new_cases"],
#                      name="New Cases"))

# fig.update_layout(
#     xaxis = dict(
#         tickmode = 'array',
#         tickvals = covid_summary_df["country"],
#         ticktext = covid_summary_df["country"])
# )

# fig.show()

app = dash.Dash()

app.layout = html.Div(children=[
    # dcc.DatePickerSingle(id='date_picker_single', date=dt(2017, 12, 18)),
    html.Div(dcc.Graph(figure=fig))
    ])

app.run_server(debug=True, use_reloader=False)