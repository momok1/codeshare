import cdsapi
import numpy as np
from queue import Queue
from time import time
import os
from threading import Thread

#data =filename,year,month,day,var
def retrivedata(data):
    if(os.path.isfile('F:/ERA5/'+filename)):
        print('ok', filename)
    else:
        c = cdsapi.Client()
        try:
            c.retrieve(
                'reanalysis-era5-land',
                {
                    'variable': [
                        data[4]
                    ],
                    'year': data[1],
                    'month': data[2],
                    'day': data[3],
                    'time': [
                        '00:00', '01:00', '02:00',
                        '03:00', '04:00', '05:00',
                        '06:00', '07:00', '08:00',
                        '09:00', '10:00', '11:00',
                        '12:00', '13:00', '14:00',
                        '15:00', '16:00', '17:00',
                        '18:00', '19:00', '20:00',
                        '21:00', '22:00', '23:00',
                    ],
                    'area': [
                        90, -180, -90,
                        180,
                    ],
                    'format': 'netcdf.zip',
                },
                'F:/ERA5/'+data[0])
            print(data[0])
        except Exception as e:
            print('*****exception*******'+data[0])


class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue =queue

    def run(self):
        while True:
            data =self.queue.get()
            retrivedata(data)
            self.queue.task_done()

if __name__ =='__main__':
    vares =['2m_temperature', 'runoff', 'surface_pressure', 'surface_solar_radiation_downwards','total_evaporation','total_precipitation']
    years =np.arange(2002,2005,1).tolist()
    months =['04','06','09','11']
    day =[
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
        ]

    print('queue')
    queue =Queue()
    for x in range(4):
        worker =DownloadWorker(queue)
        worker.daemon =True
        worker.start()

    datas =[]
    for year in years:
        for month in months:
            for var in vares:
                filename =f'ERA5_{year}_{month}_{var}.netcdf.zip'
                datas.append([filename, str(year), month, day, var])
    for link in datas:
        queue.put((link))

    queue.join()