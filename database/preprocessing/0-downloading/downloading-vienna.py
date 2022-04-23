# Import libraries
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
import time


def webscrape_vienna(path_download_dir, path_chromedriver):
    """
    Function to download files via webscrapping, using selenium and google's 
    chromedriver, which can be downloaded here: https://chromedriver.chromium.org/
    
    How to webscrape:
    0. Download all required packages (incl. selenium and chromedriver)
    1. Open the url with the download links 
    2. right cklick on the first download link and select 'inspect'
    3. right click again on the higlighted entry in firefox developer mode and choose 'copy path' -> 'xpath'
    4. do the same for the last download link 
    
    Args:
    - url_page (str): address of webpage where files should be downloaded
    - path_download_dir (str): path to dir in which downloads should be saved
    - path_driver (str): path to folder in which geckodriver is stored
    - path_xpath1(str): xpath of file where download should start with
    - path_xpath2 (str): xpath of file where download should stop (all download links in between will be downloaded)
    - sleep_time (int): on some pages it takes some time to load all download links; here, a timer can be set to wait 

    Returns:
    - 

    Last update: 21/06/21. By Felix.

    """
    print('Starting to download')
    
    # Set options for webdriver
    options = ChromeOptions()
    options.add_argument("--headless")
    prefs = {"profile.default_content_settings.popups": 0,
             "download.default_directory": path_download_dir,
             "directory_upgrade": True}
    options.add_experimental_option("prefs", prefs)

    # Run webdriver from executable path of your choice
    driver = webdriver.Chrome(executable_path = path_chromedriver, options=options)

    #-----------------------------------------------------------------------------

    # Get web page
    #time.sleep(1)
    i=0
    #loop through columns
    for col in range(78,136+1):
        if col <100:
            str_col = '0'+str(col)
        else:
            str_col = str(col)    
        for row in range(62,107+1):
            # loop through rows
            if col <100:
                str_row = '0'+str(row)
            else:
                str_row = str(row)
            part_a = 'https://www.wien.gv.at/ma41datenviewer/downloads/ma41/geodaten/lod2_gml/'
            part_b = '_lod2_gml.zip'
            path = part_a+str_col+str_row+part_b
            driver.get(path)
            time.sleep(1)
        time.sleep(20)

        print('Downloaded col: ',col)

    
    # Sleep for 5s to ensure that everythings loaded properly
    time.sleep(200)
    driver.close()

    #-----------------------------------------------------------------------------
    print('-------------')
    print('Download sucessfull!')
    print('Files downloaded to '+path_download_dir)



def main():

    # Paths for webscrapper
    driver_dir = "/Users/Felix Wagner/Documents/0_program/0_webscraper/chromedriver"
    # Path of output dir
    download_dir = "/Drive/Downloads/Vienna"

    # Start to webscrape
    webscrape_vienna(download_dir, driver_dir)   

if __name__ == "__main__":
    main()