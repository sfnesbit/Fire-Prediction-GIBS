from datetime import datetime, timedelta
from time import sleep
import multiprocess as mp
import numpy as np
import calendar
import json
import ast
import os
import ee

# get land tiles from data store
def getLandTiles():
    allTiles = []
    with open('../data/tiles/allTiles.txt', 'r') as file:
        for line in file.readlines():
            allTiles.append(ast.literal_eval(line))
    print(f'{len(allTiles)} total tiles retrieved')

    waterProportion = 0.50
    landTiles = [tile for tile in allTiles if tile[1] < waterProportion]
    print(f'{len(landTiles)} tiles retrieved with less than {waterProportion*100:.0f}% water area')
    return landTiles


# make a list of date ranges from start date to end date over an increment
#   like ['2015-07-01', '2015-07-08', '2015-07-15', ...]
# repeat over each year
def getDateRange(startMonth, endMonth, startYear, endYear, dayIncrement=7):
    dates = []
    # inclusive range
    for year in range(startYear, endYear+1):
        yearlyDates = [] # segment by year to prevent a long span across year end

        #collect all dates within that year
        startDate = datetime(year,startMonth,1)
        endDate = datetime(year, endMonth, calendar.monthrange(year, endMonth)[1])
        date = startDate
        while date < endDate:
            yearlyDates.append(date.strftime('%Y-%m-%d'))
            date += timedelta(days=dayIncrement)
        yearlyDates.append(endDate.strftime('%Y-%m-%d'))
        
        # pair with previous date
        for i in range(len(yearlyDates)):
            if i == 0:
                continue
            dates.append((yearlyDates[i-1], yearlyDates[i]))

    return dates

# Returns an <ee.Image> of the world across the date range where firs burned
def filterImageDates(data, start_date, end_date):
    image = data.filter(ee.Filter.date(start_date, end_date)).select('T21').sum()
    return image

# get the fire image dataset
def getFireImage():
    dataset = ee.ImageCollection('FIRMS')
    return dataset

# Returns True if area contains a max burn pixel > tolerance level
def didAreaBurn(image, sw_long, sw_lat, side=0.1, tolerance=0):
    region = ee.Geometry.BBox(sw_long, sw_lat, sw_long+side, sw_lat+side)
    maxDictionary = image.reduceRegion(
        reducer= ee.Reducer.max(),
        geometry= region,
        scale= 250, #meters per pixel
        maxPixels= 1e9)
    
    # get numerical value
    maxBurn = maxDictionary.getInfo()['T21']
    maxBurn = maxBurn if maxBurn is not None else 0

    return (maxBurn > tolerance)

# 2D dictionary like dict[long][lat]
def makeFireDictionary():
    fireData = {}
    for tile in landTiles:
        longitude = str(tile[0][0])
        latitude  = str(tile[0][1])
        if longitude in fireData:
            fireData[longitude][latitude] = None
        else:
            fireData[longitude] = {}
            fireData[longitude][latitude] = None
    return fireData

# easy parallel to retrieve burned area
# if an exception occurs, return area did not burn
def parallelBurnArea(img, longitude, latitude):
    try:
        burned = didAreaBurn(img, longitude, latitude)
        return [(str(longitude), str(latitude)), burned]
    except:
        return [(str(longitude), str(latitude)), False]



# run
if __name__ == '__main__':
    #Start Eath Engine w/ minimal authentication popups
    try:
        ee.Initialize()
        print("Earth Engine initilized successfully!")
    except ee.EEException as e:
        try:
            ee.Authenticate()
            ee.Initialize()
        except ee.EEException as e2:
            print("Earth Engine could not be initialized!")
            exit()

    #define date ranges to retrieve on
    dates = getDateRange(7,10,2015,2020)
    fireImage = getFireImage()
    landTiles = getLandTiles()

    #progress data
    completedDates = 0
    totalDates = len(dates)
    totalTiles = len(landTiles)
    
    #iterate over every set of date ranges to filter fire data
    for dateRange in dates:
        #pass completed data
        if f'fireData-{dateRange[0]}.json' in os.listdir('../data/burn/'):
            completedDates += 1
            continue

        with mp.Pool(4) as p:
            results = []

            #setup data
            filteredImage = filterImageDates(fireImage, dateRange[0], dateRange[1])
            fireData = makeFireDictionary()
            fireData['dates'] = [dateRange[0], dateRange[1]]

            #start parallelization
            r = [p.apply_async(parallelBurnArea, args=(filteredImage,tile[0][0],tile[0][1]), callback=results.append) for tile in landTiles]
            print(f'{dateRange[0]} started')

            #wait for results to be done
            while len(results) != totalTiles:
                sleep(1)
                print(f'\r{len(results):04}/{totalTiles:04}', end='')
            print(f'\rRange {dateRange[0]} completed')

            #populate dicitonary
            for pair in results:
                longitude = pair[0][0]
                latitude  = pair[0][1]
                burned    = pair[1]
                if burned:
                    fireData[longitude][latitude] = dateRange[0]

            #write out
            with open(f'../data/burn/fireData-{dateRange[0]}.json', 'w') as file:
                json.dump(fireData, file, indent=4)
                print(f'\r\tWrote: data/burn/fireData-{dateRange[0]}.json')
            
