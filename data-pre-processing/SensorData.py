import json
import pandas as pd
import numpy as np
import os

class SensorData:
    def __init__(self, sensor_id, sensor_name, lower_limit, upper_limit, t_90, t_90_value, sampling_period, get_service,
                 molar_mass) -> None:
        self.__sensor_id = sensor_id
        self.__sensor_name = sensor_name
        self.__get_service = get_service
        self.__lower_limit = lower_limit
        self.__upper_limit = upper_limit
        self.__t_90 = t_90 / 2
        self.__t_90_value = t_90_value
        self.__sampling_period = sampling_period
        self.__molar_mass__ = molar_mass
        self.web_dataframe = []
        self.sensor_dataframe = []
        self.sensor_dataframe_1hr = []
        self.raw_series = []
        self.valid_series = []
        self.valid_differential_series = []

    def get_samples(self):
        response_json = self.__get_service.get_json_data_from_sensor_id(self.__sensor_id)
        response_dict = json.loads(response_json.content)
        response_dataframe = pd.DataFrame.from_dict(response_dict)
        response_dataframe['DateTime'] = (pd.to_datetime(response_dataframe['date'], 
                                        infer_datetime_format=False, 
                                        format='%d/%m/%Y %H:%M:%S'))
        self.web_dataframe = response_dataframe[["DateTime","measuring", "latitude", "longitude"]]

    def tag_and_prepare_data(self):
        self.sensor_dataframe = self.web_dataframe
        self.sensor_dataframe = ((self.sensor_dataframe.sort_values(by='DateTime', ascending=True)
                                  .reset_index().drop(columns='index')))
        self.sensor_dataframe.index = self.sensor_dataframe['DateTime']
        self.sensor_dataframe = self.sensor_dataframe.drop(columns=['DateTime'])
        
        # Create series
        self.sensor_dataframe = self.sensor_dataframe.resample('15T').mean()
        self.raw_series = self.sensor_dataframe['measuring']

        # Tag according to sensor limits
        self.sensor_dataframe['Tag'] = (self.sensor_dataframe['measuring']
                                        .apply(lambda v: self.__get_tags_from_series__(value=v,
                                                                                       lower_limit=self.__lower_limit,
                                                                                       upper_limit=self.__upper_limit)))
        
        # Calculate derivatives
        self.sensor_dataframe['Diff'] = self.sensor_dataframe['measuring'].resample('15T').mean().diff() 
        self.sensor_dataframe['Tag'] = (self.sensor_dataframe[['Tag', 'Diff']]
                                        .apply(lambda df: self.__tag_data_with_diff__(tagged_df=df), axis=1))
        self.sensor_dataframe['value'] = self.sensor_dataframe['measuring'].map(lambda v: 0.0409*v*self.__molar_mass__/1e3)
        self.valid_differential_series = self.sensor_dataframe[self.sensor_dataframe['Tag'] == 'VALID']['Diff']
        
        # Separate valid dataframe
        valid_dataframe = (self.sensor_dataframe[self.sensor_dataframe['Tag'] == 'VALID'].drop(columns=['Tag']))
        self.valid_series = valid_dataframe['measuring']
        
        # Calculate hourly statistics
        self.sensor_dataframe_1hr = self.__get_hour_statistics__(valid_dataframe, self.sensor_dataframe.index.freq)

    def __reset_index_to_date_time__(self, input_df):
        df = ((input_df.sort_values(by='DateTime', ascending=True)
               .reset_index().drop(columns='index')))
        df.index = input_df['DateTime']
        df = input_df.drop(columns=['DateTime'])
        return df
    
    def __get_tags_from_series__(self, value, lower_limit, upper_limit):
        if (value <= -9000.0  or 
            np.isnan(value))  : return 'MISSING' 
        if value < lower_limit: return 'LTLL'
        if value > upper_limit: return 'GTUL'
        return 'VALID'
    
    def __tag_data_with_diff__(self, tagged_df):
        current_tag = tagged_df[0]
        value = tagged_df[1]
        if ((current_tag != 'VALID') or (np.isnan(value))): return current_tag
        max_diff_value = self.__sampling_period / self.__t_90 * self.__t_90_value
        if ((value > max_diff_value) or (value < -max_diff_value)): return 'BADSPIKE'
        return 'VALID'
    
    def __get_hour_statistics__(self, valid_dataframe, original_freq):
        resampled_dataframe = valid_dataframe.resample('H').mean()
        resampled_dataframe['Hour'] = resampled_dataframe.index.hour
        resampled_dataframe['Count'] = (valid_dataframe.resample('H').count()['measuring'])
        resampled_dataframe['Std'] = (valid_dataframe.resample('H').std()['measuring'])
        resampled_dataframe['% valid'] = (resampled_dataframe['Count']
                                          .map(lambda c:
                                               c / (pd.Timedelta("1 hour") / original_freq) * 100))
        resampled_dataframe['Tag'] = (resampled_dataframe['% valid']
                                        .map(lambda c: 'VALID' if c >= 75 else 'LOWSAMPLES'))
        resampled_dataframe.index = resampled_dataframe.index.map(lambda t: t.replace(minute=30, second=0))
        return resampled_dataframe

    def calculate_and_tag_quantiles(self):
        dataframe = self.sensor_dataframe_1hr[self.sensor_dataframe_1hr['Tag'] == 'VALID']
        global_qtle_01 = dataframe.pivot(columns='Hour')['measuring'].quantile(q=0.01, axis='index', interpolation='lower').dropna()
        global_qtle_99 = dataframe.pivot(columns='Hour')['measuring'].quantile(q=0.99, axis='index', interpolation='higher').dropna()
        self.sensor_dataframe_1hr['GLOBAL_QTLE01'] = self.sensor_dataframe_1hr['Hour'].map(lambda hr: global_qtle_01[hr] if (not np.isnan(hr)) else np.nan)
        self.sensor_dataframe_1hr['GLOBAL_QTLE99'] = self.sensor_dataframe_1hr['Hour'].map(lambda hr: global_qtle_99[hr] if (not np.isnan(hr)) else np.nan)
        self.sensor_dataframe_1hr['Tag'] = (self.sensor_dataframe_1hr[['Tag', 'measuring', 'GLOBAL_QTLE01', 'GLOBAL_QTLE99']]
                                            .apply(lambda df: self.__tag_by_quantiles__(tagged_df=df), axis=1))
        
    def __tag_by_quantiles__(self, tagged_df):
        current_tag = tagged_df[0]
        value = tagged_df[1]
        quantile_01 = tagged_df[2]
        quantile_99 = tagged_df[3]
        if ((current_tag != 'VALID') or (np.isnan(value))): return current_tag
        if value <= quantile_01: return 'LTQTLE01'
        if value >= quantile_99: return 'GTQTLE99'
        return 'VALID'

    def save_to_csv(self):
        directory_path = 'data/output/'

        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        processing_directory_path = '../data-processing/input/' 
        self.sensor_dataframe.to_csv(directory_path + self.__sensor_name + 'sensor_dataframe.csv')
        self.sensor_dataframe_1hr.to_csv(directory_path + self.__sensor_name + 'sensor_dataframe_1hr.csv')
        self.sensor_dataframe.to_csv(processing_directory_path + self.__sensor_name + 'sensor_dataframe.csv')
        self.sensor_dataframe_1hr.to_csv(processing_directory_path + self.__sensor_name + 'sensor_dataframe_1hr.csv')

    def read_from_csv(self):
        directory_path = 'data/input/'
        df = pd.read_csv(directory_path + self.__sensor_name + 'web_dataframe.csv')
        self.web_dataframe = df.drop(df.columns[0], axis='columns')
        self.web_dataframe['DateTime'] = (pd.to_datetime(df['DateTime'], infer_datetime_format=True))