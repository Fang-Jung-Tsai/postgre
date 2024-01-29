#coding=utf-8
import io
import os
import re
import pandas
import requests
import datetime
import geopandas
import matplotlib.pyplot as plt
    

def generate_weather_plots(geodata):
    
    # Create a figure with 2 rows and 2 columns
    plt.figure(figsize=(6, 9))
    plt.subplots_adjust(hspace=0.2, wspace=0.1)
    
    # Get the start_time and end_time from the first row of the DataFrame
    # start_time = geodata.iloc[0]['start_time']
    # end_time = geodata.iloc[0]['end_time']
    # start_time = start_time.strftime('%Y%m%d %H')
    # end_time = end_time.strftime('%H')
    
    # # Set the title of the figure
    # plt.suptitle(f'Taipei Weather \n {start_time}h - {end_time}h', fontsize=20)

    # Set the title of each subplot, Set the variables to be plotted
    variables = ['temperature', 'rainfall_probability', 'relative_humidity']
    titles = ['Temperature', 'Rainfall', 'Humidity']
    positions = [1, 4, 3]
    vmin_values = [15, 0, 0]
    vmax_values = [30, 100, 100]

    for i in range(len(variables)):
        ax = plt.subplot(2, 2, positions[i])
        ax.set_title(titles[i], fontsize=20)
        geodata.plot(ax=ax, column=variables[i], legend=True, cmap='coolwarm', figsize=(3, 4.5), vmin=vmin_values[i], vmax=vmax_values[i], edgecolor='black')
        #geodata.apply(lambda x: plt.text(x.geometry.centroid.x, x.geometry.centroid.y, x[variables[i]], ha='center', fontsize=8), axis=1)
        plt.xticks([])
        plt.yticks([])

    # Save the plots to a virtual memory file (BytesIO)
    image_file = io.BytesIO()
    plt.savefig(image_file, format='png')
    plt.close()

    # Move the file pointer to the beginning of the file
    image_file.seek(0)
    return image_file


if __name__ =='__main__':
    import postgis_CE13058

    postgis = postgis_CE13058.postgis_CE13058()

    gdf = postgis.read_geometry('data_rosa_ctyang_town')
    gdf ['geometry'] = gdf ['geom']

    # reg geom as geometry
    gdf = geopandas.GeoDataFrame(gdf, geometry='geometry')

    io =  generate_weather_plots(gdf)

    #write io to user's home/Downloads
    with open(os.path.expanduser('~/Downloads/weather.png'), 'wb') as f:
        f.write(io.read())
        f.close()