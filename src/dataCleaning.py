import shutil
import glob
import sys
import os

# GDL saves images into subdirectories like
#   'product_lat_lon_date-range/original_images/*.{png, jpg, jpeg}'
# This function indexes into each tile directory and pulls images to the base
# of the tile directory for easier access
def flattenImageDirectory(dir='burned'):
    burnedDir = f'../data/climate/{dir}/*/'
    extensions = ['*.png', '*.jpg', '*.jpeg']
    tileDirs = glob.glob(burnedDir)

    totalDirs = len(tileDirs)
    count = 0
    print(f'{totalDirs} {dir} directories found')
    
    #for every directory like '-118.2_33.6'
    for tileDir in tileDirs:
        count += 1
        print(f'\r{count:04}/{totalDirs:04}', end='')

        #retrieve all full image paths
        images = []
        for ext in extensions:
            images += glob.glob(os.path.join(tileDir, '**/', ext), recursive=True)

        #move image to base tile directory 
        for path in images:
            imageName = path.split('/')[-1]
            os.rename(path, os.path.join(tileDir, imageName))


# Occasionally GDL will miss a product, resulting in less images/features than desired
# we cannot use these in the dataset and will therefore delete the tile directories 
# and their data to prevent incomplete data
def removeIncompleteTiles(bandCount=10, dir='burned'):
    burnedDir = f'../data/climate/{dir}/*/'
    extensions = ['*.png', '*.jpg', '*.jpeg']
    tileDirs = glob.glob(burnedDir)

    totalDirs = len(tileDirs)
    count = 0
    foundIncomplete = []
    print(f'{totalDirs} {dir} directories found')
    
    #for every directory like '-118.2_33.6'
    for tileDir in tileDirs:
        count += 1
        print(f'\r{count:04}/{totalDirs:04}', end='')

        #find images in the base of the tile directory
        images = []
        for ext in extensions:
            images += glob.glob(os.path.join(tileDir, ext))

        #store directories to be removed
        if len(images) < bandCount:
            print(f'\nIncomplete: {tileDir}')
            foundIncomplete.append(tileDir)
            
    print(f'\n{len(foundIncomplete)} incomplete directories found')

    if len(foundIncomplete) == 0:
        return

    print('Would you like to delete these directories? (y/n)')
    print('> ', end = '')
    response = input()

    if response.lower() == 'y':
        print('Deleting directories\n')
        for incomplete in foundIncomplete:
            shutil.rmtree(incomplete)
    else:
        print('Directories are preserved\n')
        return
        

# run
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'Usage: >$ python {sys.argv[0]} (option) [bandCount]')
        print('\t(option):')
        print('\t\t"flatten": flatten directories -- pulls images to base of tile directory')
        print('\t\t"clean":   clean directories -- use after flatten, removes any tile directories without all product bands')
        print('\t\t\tbandCount: (optional) -- int, number of bands expected when cleaning (default: 10)')
        print('\t\t"both":    flattens and cleans')
        exit()
    
    if sys.argv[1] == 'flatten':
        flattenImageDirectory('burned')
        flattenImageDirectory('unburned')

    elif sys.argv[1] == 'clean':
        if len(sys.argv) > 2:
            removeIncompleteTiles(int(sys.argv[2]), dir='burned')
            removeIncompleteTiles(int(sys.argv[2]), dir='unburned')
        else:
            removeIncompleteTiles(dir='burned')
            removeIncompleteTiles(dir='unburned')
            

    elif sys.argv[1] == 'both':
        flattenImageDirectory('burned')
        flattenImageDirectory('unburned')
        if len(sys.argv) > 2:
            removeIncompleteTiles(int(sys.argv[2]), dir='burned')
            removeIncompleteTiles(int(sys.argv[2]), dir='unburned')
        else:
            removeIncompleteTiles(dir='burned')
            removeIncompleteTiles(dir='unburned')
    print('Done!')
    
            
    