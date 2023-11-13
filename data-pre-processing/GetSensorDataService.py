import requests
import pandas as pd
import datetime as dt
import json

class GetSensorDataService:
    def __init__(self, host, port) -> None:
        self.__host = 'http://' + host + ':' + str(port)

    def get_data_from_file(self, filename, sensor_name):
        df = pd.read_csv(filename)
        date_time_col = ['Year','Month','Day','Hour','Minute','Second']
        df['DateTime'] = (pd.to_datetime(df[date_time_col], infer_datetime_format=False, format='%d/%m/%Y/%H/%M/%S'))
        df = df.drop(columns=date_time_col)
        df = df.drop(columns=['Altitude', 'Device', 'DeviceSt', ' SensorID'])
        df = df.rename(columns={'Value': 'measuring', 'Latitude': 'latitude', 'Longitude': 'longitude'})
        df = (df.where(df['DateTime'] > dt.datetime(2020, 1, 1, 0, 0, 0))
                .where(df['DateTime'] <= dt.datetime.now()).dropna())
        path = "data/input/"
        df.to_csv(path + sensor_name + 'web_dataframe.csv') 
        return df
    
    def get_samples_by_sensor(self, sensor_id):
        """
        Returns all data points in a sensor
        Parameters
        ----------
        sensor_id : String
            The id of the sensor on Renovar API.

        Returns
        -------
        response_dataframe : Pandas Dataframe
            A pandas dataframe with Datetime indexes containing all the data points of the sensor.
            
            index='date'- Datetime indexes
            columns='measuring'
        """

        ENDPOINT = "/sample/sensor/all/"
        REQUEST = self.__host + ENDPOINT
        response_json = requests.get(REQUEST + str(sensor_id))
        response_dict = json.loads(response_json.content)
        response_dataframe = pd.DataFrame.from_dict(response_dict)
        return self.__prepare_date_time_dataframe__(response_dataframe[["date","measuring"]])

    def get_samples_by_sensor_in_range(self, sensor_id, start_date, end_date):
        """
        Returns all data points in a sensor within a date range
        Parameters
        ----------
        sensor_id : String
            The id of the sensor on Renovar API.
        start_date: String
            The start date with the format YYYY-MM-DD
        end_date: String
            The end date with the format YYYY-MM-DD

        Returns
        -------
        response_dataframe : Pandas Dataframe
            A pandas dataframe with Datetime indexes containing all the data points of the sensor.
            
            index='date'- Datetime indexes
            columns='measuring'
        """

        ENDPOINT = "/sample/sensor/range/?sensorID=" + str(sensor_id) + "&startDate=" + start_date + "&endDate=" + end_date
        REQUEST = self.__host + ENDPOINT 
        response_json = requests.get(REQUEST)
        response_dict = json.loads(response_json.content)
        content = response_dict['content']
        measuring_list = [item['measuring'] for item in content]
        date_list = [item['date'] for item in content]
        measuring_dataframe = pd.DataFrame({'measuring': measuring_list, 'date': date_list})
        return self.__prepare_date_time_dataframe__(measuring_dataframe)
    
    def __prepare_date_time_dataframe__(self, dataframe):
        """
        Receives a dataframe with 'date' and 'measuring' columns
        Returns dataframe with DateTime indexes and resampled to a period of 15 mins
        """
        measuring_dataframe = dataframe
        measuring_dataframe['date'] = (pd.to_datetime(measuring_dataframe['date'], infer_datetime_format=True))

        # Resample data with 15 mins period and create sensor dataframe
        measuring_dataframe = measuring_dataframe.sort_values(by='date', ascending=True).reset_index().drop(columns='index')
        measuring_dataframe.index = measuring_dataframe['date']
        measuring_dataframe = measuring_dataframe.drop(columns=['date'])
        return measuring_dataframe 

    def get_last_sample_of_sensor(self, sensor_id):
        """
        Returns the last sample of a sensor
        Parameters
        ----------
        sensor_id : String
            The id of the sensor on Renovar API.

        Returns
        -------
        date : String
            The datetime of the sample
            
        measuring: Double
            The value of the sample
        """

        ENDPOINT = "/sample/sensor/last/" + str(sensor_id)
        REQUEST = self.__host + ENDPOINT 
        response_json = requests.get(REQUEST)
        response_dict = json.loads(response_json.content)
        measuring = response_dict['measuring']
        date = response_dict['date']
        return date, measuring
