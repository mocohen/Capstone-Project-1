import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import lightgbm as lgb
from statsmodels.tsa.seasonal import seasonal_decompose
import matplotlib.pyplot as plt
from pmdarima import auto_arima
import pickle

import os.path
from os import path
# %matplotlib inline

# !ls 1.collect_data/

blockface_detail = pd.read_csv('1.collect_data/blockface_detail.csv')

blockface_detail_locs = blockface_detail[['latitude', 'longitude', 'sourceelementkey']]
blockface_detail_locs.head()

df = pd.read_pickle('1.collect_data/data_files/1hr_1block-average.pkl').replace([np.inf, -np.inf], np.nan).dropna()

df.head()

df.reset_index(inplace=True)

# last_two_years = df[df['OccupancyDateTime'] > '2018-01-01 00:00:00']

# last_two_years.head()

# last_two_years.tail().OccupancyDateTime.dt.dayofweek

# merged = pd.merge(last_two_years, 
#                   blockface_detail_locs, 
#                   how='left', 
#                   left_on='SourceElementKey', 
#                   right_on='sourceelementkey').drop('sourceelementkey', axis=1)

# merged.tail()

# merged['Month'] = merged.OccupancyDateTime.dt.month
# merged['Year'] = merged.OccupancyDateTime.dt.year
# merged['Hour'] = merged.OccupancyDateTime.dt.hour
# merged['DayOfWeek'] = merged.OccupancyDateTime.dt.dayofweek
# merged['Day'] = merged.OccupancyDateTime.dt.day

# merged['Dummy'] = 0

# merged.head()

# y = df['PercentOccupied'].values
# X = df[['Dummy', 'SourceElementKey']]

# df_train, df_test = train_test_split(
#     merged, test_size=0.3, random_state=42)

# baseline_columns = ['Dummy', 'SourceElementKey']

# lgb_train = lgb.Dataset(data=df_train[baseline_columns], 
#                         label=df_train['PercentOccupied'], 
#                         feature_name=baseline_columns, 
#                         categorical_feature=['SourceElementKey'])
# lgb_test = lgb.Dataset(data=df_test[baseline_columns], 
#                        label=df_test['PercentOccupied'], 
#                        feature_name=baseline_columns, 
#                        categorical_feature=['SourceElementKey'],
#                        reference=lgb_train)

# params = {
#     'boosting_type': 'gbdt',
#     'objective': 'regression',
#     'metric': {'l2', 'l1'},
#     'num_leaves': 31,
#     'learning_rate': 0.05,
#     'feature_fraction': 0.9,
#     'bagging_fraction': 0.8,
#     'bagging_freq': 5,
# }

# gbm = lgb.Booster(model_file='baseline_model.txt')

# # print('Starting training...')
# # # train
# # gbm = lgb.train(params,
# #                 lgb_train,
# #                valid_sets=lgb_test)

# # print('Saving model...')
# # # save model to file
# # gbm.save_model('baseline_model.txt')

# y_pred = gbm.predict(df_test[baseline_columns], num_iteration=gbm.best_iteration)

# def smape_error(forecast, actual):
#     numerator = np.absolute(forecast-actual)
#     denominator = np.absolute(forecast) + np.absolute(actual)

#     num_samples = len(numerator)

#     return 100/num_samples * np.sum(numerator/denominator)

# baseline_smape = smape_error(y_pred, df_test['PercentOccupied'])
# print('smape: %f' % baseline_smape)

# df.tail().OccupancyDateTime.dt.day

# simple_columns = ['SourceElementKey', 'Year', 'Hour', 'DayOfWeek']

# simple_train = lgb.Dataset(data=df_train[simple_columns], 
#                            label=df_train['PercentOccupied'],
#                             feature_name=simple_columns, 
#                        categorical_feature=['SourceElementKey', 'DayOfWeek'])
# simple_test = lgb.Dataset(data=df_test[simple_columns], 
#                           label=df_test['PercentOccupied'], 
#                           feature_name=simple_columns, 
#                           categorical_feature=['SourceElementKey', 'DayOfWeek'],
#                           reference=simple_train)



# gbm_simple = lgb.Booster(model_file='simple_ml_model.txt')

# print('Starting training...')
# # train
# gbm_simple = lgb.train(params,
#                 simple_train,
#                 valid_sets=simple_test)

# print('Saving model...')
# # save model to file
# gbm_simple.save_model('simple_ml_model.txt')

# y_pred = gbm_simple.predict(df_test[simple_columns], num_iteration=gbm_simple.best_iteration)

# simple_smape = smape_error(y_pred, df_test['PercentOccupied'])
# print('smape: %f' % simple_smape)

# # shift values by one
# pred_values = pd.concat([pd.Series([0]), df['PercentOccupied']])
# timeseries_smape = smape_error(pred_values.values[:-1], df['PercentOccupied'])
# print('smape: %f' % timeseries_smape)

# # shift values by four (i.e. 1 hour)
# pred_values = pd.concat([pd.Series([0,0,0,0]), df['PaidOccupancy']])
# timeseries_smape = smape_error(pred_values.values[:-4], df['PaidOccupancy'])
# print('smape: %f' % timeseries_smape)

# from sklearn.metrics import mean_squared_error
# from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
# df.head()

# blockface_detail.sourceelementkey.values
import argparse


parser = argparse.ArgumentParser(description='SARIMA.')
parser.add_argument('-s', '--start', help='first block to model (default: 0)', type=int, default=0)
parser.add_argument('-e', '--end', help='last block to model (default: -1)', type=int, default=-1)


args = parser.parse_args()

from pmdarima.arima import ARIMA


for block in blockface_detail.sourceelementkey.values[args.start:args.end]:
  print('\n\nblock %d\n\n' % block)
  filename = 'arima_results3/arima.%d.pkl' % block
  mask = (df['SourceElementKey'] == block) & (df['OccupancyDateTime'] > ('2019-01-01'))

  curr = df[mask]
  pct_occupied = curr.PercentOccupied
  if len(pct_occupied) > 0 and not path.exists(filename):

    num_split = int(.7*len(pct_occupied))
    oob_length = int(0.2*len(pct_occupied))

    time_chunks_per_day = curr.groupby(curr.OccupancyDateTime.dt.dayofyear).count().SourceElementKey.max()


    tr, tt = pct_occupied.iloc[:num_split], pct_occupied.iloc[num_split:]

    mdl = auto_arima(tr, error_action='ignore', trace=True,
                         start_p=2, start_q=2, start_P=2, start_Q=2,
                         max_p=10, max_q=10, max_P=10, max_Q=10,
                         d=0, D=0, out_of_sample_size=oob_length, 
                         max_order=None, information_criterion='oob',
                          seasonal=True, m=time_chunks_per_day)

    with open(filename, 'wb') as pkl:
        pickle.dump(mdl, pkl)
  else:
    print('no vals or already ran')

  # preds, conf_int = mdl.predict(n_periods=tt.shape[0], return_conf_int=True)


  # print("Test RMSE: %.3f" % np.sqrt(mean_squared_error(tt, preds)))

# # #############################################################################
# # Plot the points and the forecasts
# x_axis = np.arange(tr.shape[0] + preds.shape[0])
# x_years = x_axis + 0  # Year starts at 1821
# plt.figure()
# plt.plot(x_years[x_axis[:tr.shape[0]]], tr, alpha=0.75)
# plt.plot(x_years[x_axis[tr.shape[0]:]], preds, alpha=0.75)  # Forecasts
# plt.scatter(x_years[x_axis[tr.shape[0]:]], tt,
#             alpha=0.4, marker='x')  # Test data
# plt.fill_between(x_years[x_axis[-preds.shape[0]:]],
#                  conf_int[:, 0], conf_int[:, 1],
#                  alpha=0.1, color='b')
# plt.title("Parking")
# #plt.xlabel("Year")
# plt.show()

# def using_Grouper(df):
#     level_values = df.index.get_level_values
#     return (df.groupby([level_values(0)]
#                        +[pd.Grouper(freq='1D', level=-1)]).sum())

# daily_data = using_Grouper(pd.read_pickle('1.collect_data/data_files/15min.pkl').dropna())


# daily_data.head()

# # shift values by one
# pred_values = pd.concat([pd.Series([0]), daily_data['PaidOccupancy']])
# timeseries_smape = smape_error(pred_values.values[:-1], daily_data['PaidOccupancy'])
# print('smape: %f' % timeseries_smape)


