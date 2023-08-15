import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class SensorDataAnalysisService:
    def count_tags(tags_list, df):
        tags_list.append('TOTAL')
        count = len(df)
        data_count = pd.DataFrame(index=tags_list, columns=['#', '%'])
        data_count.loc['TOTAL'] = [count, (count/count)*100]

        for tag in data_count.index[:-1]:
            val = (df.where(df['Tag'] == tag)
                    .count()['Tag'])  # Fiz assim pq count() não conta os nan
            data_count.loc[tag] = [val, (val/count)*100]

        return data_count
    
    def plot_mean_vs_std(df):
        fig = plt.figure(figsize=(1.3*7,7))
        plt.scatter(df['Std'], df['measuring'], c=df['% valid'], cmap='jet')
        cax = plt.axes([0.95, 0.1, 0.05
                        , 0.8])
        cbar = plt.colorbar(orientation='vertical', cax=cax)
        cbar.ax.tick_params(labelsize=11, length=0)
        ticks = [np.int64(df['% valid'].min() + 1), 
                np.int64((df['% valid'].max() - df['% valid'].min()) / 2),
                np.int64(df['% valid'].max())]
        cbar.set_ticks(np.array(ticks))
        cbar.ax.tick_params(labelsize=15, length=0)

    def plot_std_in_time(df):
        fig = plt.figure(figsize=(1.3*7,7))
        plt.scatter(df.index, df['Std'], c=df['% valid'], cmap='jet')
        cax = plt.axes([0.95, 0.1, 0.05, 0.8])
        cbar = plt.colorbar(orientation='vertical', cax=cax)
        cbar.ax.tick_params(labelsize=11, length=0)
        ticks = [np.int64(df['% valid'].min() + 1),
                np.int64((df['% valid'].max() - df['% valid'].min()) / 2),
                np.int64(df['% valid'].max())]
        cbar.set_ticks(np.array(ticks))
        cbar.ax.tick_params(labelsize=15, length=0)

    def plot_box_hist(df, bins):
        bottom, height = 0.1, 0.65
        left, width = bottom, height*1.3
        spacing = 0.005
        
        rect_ser = [left-width-spacing, bottom, width, height]
        rect_box = [left, bottom, width, height]
        rect_hist = [left + width + spacing, bottom, height/1.3, height]

        plt.figure(figsize=(5, 5/1.3))

        ax_ser  = plt.axes(rect_ser)
        ax_ser.tick_params(direction='in', top=True, right=True)
        ax_ser.set_title('Serie temporal')

        ax_box  = plt.axes(rect_box)
        ax_box.tick_params(direction='in', labelleft=False)

        ax_hist = plt.axes(rect_hist)
        ax_hist.tick_params(direction='in', labelleft=False)
        ax_hist.set_title('Histograma')

        lim_max = df['measuring'].max()+df['measuring'].max()*10/100
        lim_min = df['measuring'].min()-df['measuring'].min()*10/100

        df['measuring'].plot(ax=ax_ser)
        ax_ser.set_ylim(lim_min, lim_max)

        ax_hist.hist(df['measuring'], bins=bins, orientation='horizontal')
        ax_hist.set_ylim(lim_min, lim_max)

        df = df.dropna(axis='index', how='all', subset=['Hour'])
        df['Hour'] = df['Hour'].astype('int64')
        df.pivot(columns='Hour')['measuring'].dropna(
                axis='columns', how='all').plot.box(
                    ax=ax_box,title='Comportamento médio no período')
        ax_box.set_ylim(ax_hist.get_ylim())