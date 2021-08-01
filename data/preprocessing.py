#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 12 19:28:46 2021

@author: ruoyu
"""
import os
os.chdir("/Users/ruoyu/Career/Online CS/UIUC/2021 Spring/Data Visualization/Projects/Final-D3/data")
import pandas as pd
pd.set_option('display.max_columns', 10)

df_confirmed = pd.read_csv("./raw_data/time_series_covid19_confirmed_global.csv")
df_death = pd.read_csv("./raw_data/time_series_covid19_deaths_global.csv")
df_recovered = pd.read_csv("./raw_data/time_series_covid19_recovered_global.csv")
df_vaccine = pd.read_csv("./raw_data/time_series_covid19_vaccine_global.csv")
df_vaccine_sum = pd.read_csv("./raw_data/vaccine_data_global.csv")
df_pop = pd.read_csv("./raw_data/world_pop_by_country.csv")
df_pop.columns = ['Country/Region', 'Country Code', '2018 Population']

df_confirmed = df_confirmed.groupby('Country/Region').sum().reset_index().drop(columns=['Lat','Long'])
df_death = df_death.groupby('Country/Region').sum().reset_index().drop(columns=['Lat','Long'])
df_recovered = df_recovered.groupby('Country/Region').sum().reset_index().drop(columns=['Lat','Long'])

df_vaccine_sum = df_vaccine_sum.groupby('Country_Region').sum().reset_index()[['Country_Region','People_partially_vaccinated','People_fully_vaccinated']]
df_vaccine_sum.columns = ['Country/Region', 'People_Partially_Vaccinated', 'People_Fully_Vaccinated']

# daily new cases & 7-day average
all_days = list(df_confirmed.columns[1:])
df_new_cases = df_confirmed[['Country/Region']]
df_7_day_avg = df_confirmed[['Country/Region']]
for d in range(len(all_days)-1):
    prev_day = all_days[d]
    curr_day = all_days[d+1]
    df_new_cases[curr_day] = df_confirmed[curr_day] - df_confirmed[prev_day]
    if d >= 6:
        seven_day_before = all_days[d-6]
        df_7_day_avg[curr_day] = (df_confirmed[curr_day] - df_confirmed[seven_day_before])/7

df_7_day_avg = df_7_day_avg.melt(['Country/Region'], var_name='Date')
#df_7_day_avg.to_csv("./covid_data_7_day_average.csv", index=False)


# daily new vaccinated people
df_vaccine = df_vaccine.groupby(['Country_Region','Date']).sum().reset_index()
df_vaccine['previous_day_report'] = df_vaccine.sort_values(['Country_Region','Date']).groupby('Country_Region')['People_fully_vaccinated'].shift(1)
df_vaccine['newly_vaccinated'] = df_vaccine['People_fully_vaccinated'] - df_vaccine['previous_day_report']
df_vaccine = df_vaccine[['Country_Region','Date','People_fully_vaccinated', 'newly_vaccinated']]
#df_vaccine.to_csv("./covid_data_newly_vaccinated.csv", index=False)

# as of 7/17/2021
df_confirmed_total = df_confirmed[['Country/Region','7/17/21']]
df_confirmed_total.columns = ['Country/Region','total_confirmed']
# df_confirmed_total['New Cases (14 Day Average )'] = (df_confirmed['6/11/21'] - df_confirmed['5/28/21'])/14
# df_confirmed_total['New Cases (30 Day Average)'] = (df_confirmed['6/11/21'] - df_confirmed['5/12/21'])/30

df_death_total = df_death[['Country/Region','7/17/21']]
df_death_total.columns = ['Country/Region','total_deaths']

df_recovered_total = df_recovered[['Country/Region','7/17/21']]
df_recovered_total.columns = ['Country/Region','total_recovered']

# pivot
df_confirmed_pivot = df_confirmed.melt(['Country/Region'], var_name='date')
df_confirmed_pivot = df_confirmed_pivot.rename(columns = {'value': 'confirmed_so_far'}, inplace = False)

df_death_pivot = df_death.melt(['Country/Region'], var_name='date')
df_death_pivot = df_death_pivot.rename(columns = {'value': 'death_so_far'}, inplace = False)

df_recovered_pivot = df_recovered.melt(['Country/Region'], var_name='date')
df_recovered_pivot = df_recovered_pivot.rename(columns = {'value': 'recovered_so_far'}, inplace = False)


df_total = pd.merge(df_confirmed_total, df_death_total, on = ['Country/Region'])
df_total = pd.merge(df_total, df_recovered_total, on = ['Country/Region'])
df_total = pd.merge(df_total, df_vaccine_sum, how = 'left', on = ['Country/Region']).fillna(0)
df_total['People_Partially_Vaccinated'] = [int(n) for n in df_total.People_Partially_Vaccinated]
df_total['People_Fully_Vaccinated'] = [int(n) for n in df_total.People_Fully_Vaccinated]
df_total = pd.merge(df_total, df_new_cases, on = ['Country/Region'])
df_total = pd.merge(df_total, df_pop[['Country/Region', '2018 Population']], on = ['Country/Region'], how='inner')
df_total.loc[df_total['Country/Region'] == 'US', 'total_recovered'] = 306509795

df_pivot = df_total.melt(['Country/Region', 'total_confirmed', 'total_deaths', 
                          'total_recovered','People_Partially_Vaccinated', 
                          'People_Fully_Vaccinated', '2018 Population'], var_name='date')

df_pivot = df_pivot.rename(columns = {'2018 Population': 'population',
                                      'value': 'new_cases'}, inplace = False)

df_pivot = df_pivot.dropna(subset = ['population']).reset_index(drop=True)

import datetime
df_pivot['date'] = [datetime.datetime.strptime(date, '%m/%d/%y').strftime('%m/%d/%y') for date in df_pivot['date']]
df_vaccine['date'] = [datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%m/%d/%y') for date in df_vaccine['Date']]
df_vaccine = df_vaccine.drop(columns = ['Date'])
df_vaccine.columns = ['Country/Region', 'fully_vaccinated_so_far', 'newly_vaccinated', 'date']
df_pivot = pd.merge(df_pivot, df_vaccine, on = ['Country/Region', 'date'], how = 'left').fillna(0)
df_pivot = df_pivot.drop_duplicates().reset_index(drop=True)

# add other pivot tables
df_7_day_avg['Date'] = [datetime.datetime.strptime(date, '%m/%d/%y').strftime('%m/%d/%y') for date in df_7_day_avg['Date']]
df_7_day_avg.columns = ['Country/Region', 'date', '7_day_average']
df_pivot = pd.merge(df_pivot, df_7_day_avg, on = ['Country/Region', 'date'], how = 'left').fillna(0)

df_confirmed_pivot['date'] = [datetime.datetime.strptime(date, '%m/%d/%y').strftime('%m/%d/%y') for date in df_confirmed_pivot['date']]
df_pivot = pd.merge(df_pivot, df_confirmed_pivot, on = ['Country/Region', 'date'], how = 'left').fillna(0)

df_death_pivot['date'] = [datetime.datetime.strptime(date, '%m/%d/%y').strftime('%m/%d/%y') for date in df_death_pivot['date']]
df_pivot = pd.merge(df_pivot, df_death_pivot, on = ['Country/Region', 'date'], how = 'left').fillna(0)

df_recovered_pivot['date'] = [datetime.datetime.strptime(date, '%m/%d/%y').strftime('%m/%d/%y') for date in df_recovered_pivot['date']]
df_pivot = pd.merge(df_pivot, df_recovered_pivot, on = ['Country/Region', 'date'], how = 'left').fillna(0)

df_pivot.to_csv("./covid_data_processed.csv", index=False)

df_ranking = df_pivot[['Country/Region', 'date', 'confirmed_so_far']]
df_ranking["rank"] = df_ranking.groupby("date")["confirmed_so_far"].rank("first", ascending=False)

df_mapping = pd.read_csv("./raw_data/country_to_continent.csv")
df_mapping.columns = ['Continent', 'Country/Region']
df_ranking = pd.merge(df_ranking, df_mapping, on= ['Country/Region'], how = 'left')

df_ranking.to_csv("./ranking.csv", index=False)

# Life_Expectency
# =============================================================================
# df_life = pd.read_csv("./WHO_Life_Expectency.csv", skiprows=1)
# df_life = df_life.loc[df_life['Year'] == 2019, ["Country"," Both sexes"]].reset_index(drop=True)
# df_life.columns = ['Country/Region', 'Life_Expentency']
# 
# df_new = pd.merge(df_total, df_life, on = ['Country/Region'])
# 
# # GDP
# df_gdp = pd.read_csv("./API_GDP.csv", skiprows=2)
# =============================================================================
