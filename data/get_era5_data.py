#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ERA5 hourly data on single levels from 1959 to present
# https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels
#
# You need to create a .cdsapirc on your machine, see:
# https://cds.climate.copernicus.eu/api-how-to
# or
# https://youtu.be/RakkClsBZxE

import cdsapi

variables = [
    '2m_temperature',
    'convective_rain_rate',
    'convective_snowfall_rate_water_equivalent',
    'snow_depth',
    'precipitation_type', # type table of precipitation (rain, snow, etc.)
    '10m_u_component_of_wind', # to calculate wind speed
    '10m_v_component_of_wind', # to calculate wind speed
    'k_index', # probability of severe weather (thunderstorms)
]

## TODO: maybe take mean values?

years = [
    '1990', '1991', '1992',
    '1993', '1994', '1995',
    '1996', '1997', '1998',
    '1999',
]

for variable in variables:
    for year in years:
        print(variable, year)

        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": ["reanalysis"],
            "variable": [
                variable
            ],
            'year': [
                year
            ],
            'month': [
                '01', '02', '03',
                '04', '05', '06',
                '07', '08', '09',
                '10', '11', '12',
            ],
            'day': [
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
                '31',
            ],
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
                49, 9, 46,
                17,
            ],
            "data_format": "netcdf",
            "download_format": "unarchived",
        }

        client = cdsapi.Client()
        client.retrieve(dataset, request, "era5_data_" + variable + "_" + year + ".nc")
