from datetime import datetime, timedelta
from random import randint
from time import sleep
import multiprocess as mp
import subprocess as proc
import calendar
import json
import glob
import ast
import os

bands = {
    #"Readable name": "gibs_product_name"
    "Aqua Water Vapor Day": "MODIS_Aqua_Water_Vapor_5km_Day",
    "Terra Water Vapor Day": "MODIS_Terra_Water_Vapor_5km_Day",
    "Aster Color Relief Index": "ASTER_GDEM_Color_Index", 
    "Aster Gray Relief": "ASTER_GDEM_Greyscale_Shaded_Relief", 
    "Aster Colored Relief": "ASTER_GDEM_Color_Shaded_Relief", 
    "SMAP Soil Moisture": "SMAP_L4_Analyzed_Root_Zone_Soil_Moisture", 
    "Surface Humidity Day": "AIRS_L3_Surface_Relative_Humidity_Monthly_Day", 
    "Surface Pressure": "MERRA2_Surface_Pressure_Monthly", 
    "Terra Vegetation Index": "MODIS_Terra_NDVI_8Day", 
    "Biome 2001-2006": "Anthropogenic_Biomes_of_the_World_2001-2006"
}

# generate a random date within the date range given like (month, month2, year, year2)
# where month and month2 are ints, month <= month2
# and year and year2 are ints, year <= year2
def randomDate(dateRange):
    startYear = dateRange[2]
    endYear = dateRange[3]
    startMonth = dateRange[0]
    endMonth = dateRange[1]
    startDate = f'{startYear}-{startMonth:02}-01'

    #get a year offset from start date
    offsetYear = randint(0, endYear-startYear)

    #get total days in the month range for the selected year
    totalDays = sum([calendar.monthrange(startYear+offsetYear, m)[1] for m in range(startMonth, endMonth+1)]) - 7

    #get day offset
    offsetDay = randint(0, totalDays)

    #offset start date to random date
    dateObj = datetime.strptime(startDate, '%Y-%m-%d')
    dateObj += timedelta(days=offsetDay + (offsetYear*365))
    endDate = datetime.strftime(dateObj, '%Y-%m-%d')
    return endDate

# Read land tiles from tile data
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
    
# 2D dictionary like dict[long][lat]
def makeFireDictionary():
    fireData = {}
    for tile in getLandTiles():
        longitude = str(tile[0][0])
        latitude  = str(tile[0][1])
        if longitude not in fireData:
            fireData[longitude] = {}
        fireData[longitude][latitude] = []
    return fireData

# build burned vs unburned tile list
# data returned like [ [(long, lat), None], ...], [ [(long, lat), 'YYYY-MM-DD'], ...]
def getTiles():
    
    #get JSON data files sorted from oldest to newest data
    datadir = '../data/burn/'
    files = sorted(os.listdir(datadir))

    #combine all into dictionary
    data = makeFireDictionary()
    for idx, file in enumerate(files):
        with open(os.path.join(datadir,file)) as f:
            temp = json.load(f)
            if 'dates' in temp: del temp['dates']
            for k, v in temp.items():
                for k2, v2 in temp[k].items():
                    if v2 is not None:
                        data[k][k2].append(v2)

    burnedTiles = []
    unburnedTiles = []

    for k, v in data.items():
        for k2, v2 in data[k].items():
            if len(v2) > 0:
                burnedTiles.append([(k,k2), v2])
            else:
                unburnedTiles.append([(k,k2), v2])
    print(f'{len(burnedTiles)} burned tiles\n{len(unburnedTiles)} unburned tiles')

    return unburnedTiles, burnedTiles


# executes the gdl command and returns the directory name where images will be found
def gdl(product, date, longlat, dir='../data/climate/'):
    lon = float(longlat[0])
    lat = float(longlat[1])
    result = proc.run(["gdl",
                       f'{date}',
                       f'{date}',
                       f'{lat}, {lon}', f'{lat+.1}, {lon+.1}',
                       f'--name={product}',
                       f'--output-path={dir}'],
                    capture_output=True, text=True)
    return '_'.join([
            product.replace('_','-'),
            str(lon),
            str(lat), 
            date.replace('-','') + "-" + date.replace('-','')])

# get date 1 week before the incident, then wrap the GDL call across all bands
def wrapGdl(date, longlat, dir='../data/climate/'):
    #make directory for tile
    tileDir = os.path.join(dir, f'{longlat[0]}_{longlat[1]}')
    try:
        os.mkdir(tileDir)
    except:
        pass

    #look back one week
    dateobj = datetime.strptime(date, '%Y-%m-%d')
    dateobj -= timedelta(days=7)
    date = datetime.strftime(dateobj, '%Y-%m-%d')
    
    #download every product/band
    for name, band in bands.items():
        gdl(band, date, longlat, dir=tileDir) 
    
# easy parallel call for burned tiles
def parallelGdlBurned(tile, directory):
    #skip completed tiles
    if f'{tile[0][0]}_{tile[0][1]}' in os.listdir(directory):
        return True
        
    #get tile data
    date = sorted(tile[1])[0]
    try:
        wrapGdl(date, tile[0], dir=directory)
        return True
    except Exception as e:
        print(e)
        return False

# easy parallel call for unburned tiles
def parallelGdlUnburned(tile, dateRange, directory):
    #skip completed tiles
    if f'{tile[0][0]}_{tile[0][1]}' in os.listdir(directory):
        return True
        
    #get tile data
    try:
        date = randomDate(dateRange)
        wrapGdl(date, tile[0], dir=directory)
        return True
    except Exception as e:
        print(e)
        return False


# run
if __name__ == '__main__':

    # Test to see if GDL is installed
    try:
        result = proc.run(["gdl", "-h"], capture_output=True, text=True)
        print("GDL is installed!")
    except FileNotFoundError:
        print("You do not have GDL installed, or are not running the notebook in the same environment as your GDL install!")

    #get unburned,burned tile data
    unburnedTiles, burnedTiles = getTiles()
    totalBurned = len(burnedTiles)
    totalUnburned = len(unburnedTiles)

    #set directories
    burnedDir = '../data/climate/burned/'
    unburnedDir = '../data/climate/unburned/'

    
    #burned
    with mp.Pool(4) as multi:
        results = [] #output set

        #parallelize
        processes = [multi.apply_async(
                parallelGdlBurned,
                args=(tile, burnedDir),
                callback=results.append)
            for tile in burnedTiles]
        
        print(f'Starting parallel GDL download for burned tiles')
        while len(results) != totalBurned:
            sleep(1)
            print(f'\r{len(results)}/{totalBurned} completed', end='')
        print('\nComplete!')
    
    
    #unburned
    with mp.Pool(4) as multi:
        results = [] #output set
        dateRange = (7, 10, 2015, 2020) #july-oct, 2015-2020
        
        #parallelize
        processes = [multi.apply_async(
                parallelGdlUnburned,
                args=(tile, dateRange, unburnedDir),
                callback=results.append) 
            for tile in unburnedTiles]
       
        print(f'Starting parallel GDL download for unburned tiles')
        while len(results) != totalUnburned:
            sleep(1)
            print(f'\r{len(results)}/{totalUnburned} completed', end='')
        print('\nComplete!')

