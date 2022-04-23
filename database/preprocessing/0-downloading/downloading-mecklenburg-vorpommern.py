# Import libraries
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select


def webscrape_meckpomm(url_page,path_download_dir, path_chromedriver,path_xpath1, path_xpath2, sleep_time):
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
    print('Starting to download from '+url_page)
    
    # Set options for webdriver
    options = ChromeOptions()
    #options.add_argument("--headless")
    prefs = {"profile.default_content_settings.popups": 0,
             "download.default_directory": path_download_dir,
             "directory_upgrade": True}
    options.add_experimental_option("prefs", prefs)

    # Run webdriver from executable path of your choice
    driver = webdriver.Chrome(executable_path = path_chromedriver, options=options)

    #-----------------------------------------------------------------------------

    # Get web page
    driver.get(url_page)

    # PREPARATION
    time.sleep(3)  
    # Take care of cookies banner
    print('1. Click on cookie consent')
    driver.find_element_by_xpath("/html/body/div[1]/div/p[2]/button[1]").click()
    driver.set_window_size(1200, 962)

    # Sleep for sleep_time to load feed
    print('2. Waiting sleep_time secs to let all download links load...')
    time.sleep(sleep_time)
    print('-------------')
    

    print('3. Lets open menu with downloadlinks')
    # first wait until button clickable
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".downloadUrl > .label"))).click()
    #Now we click
    print('4. Let the downloads start!')
    

    # DOWNLOAD
    # Find where two xpath strings have different entries
    list_pos_diff = [i for i in range(len(path_xpath1)) if path_xpath1[i] != path_xpath2[i]]
    # Get first position
    pos_diff = list_pos_diff[0]

    # Get pos of '[' 
    str_front = path_xpath1[0:pos_diff]
    # here we use rindex to get last '['
    pos_start = str_front.rindex('[')

    # Get pos of ']' 
    str_end = path_xpath1[pos_start:]
    pos_end = str_end.index(']')
    # Build part 1 and part 2 of xpath
    xpath_pt1 = path_xpath1[0:pos_start+1]
    xpath_pt2 = path_xpath1[pos_start+pos_end:]

    # Get start for range (starting value between [])
    a = int(path_xpath1[pos_start+1:pos_start+pos_end])
    # Get end for range (end value between [])
    b = 1+int(path_xpath2[pos_start+1:(pos_start+pos_end+abs(len(path_xpath2)-len(path_xpath1)))])

    # the range is definded from the xpath path_xpath1 to xpath path_xpath2
    for i in range(a,b):     
        # build xpath for loop 
        path = xpath_pt1+str(i)+xpath_pt2 
        print(path)  
        # use selenium webdriver to click and download
        results = driver.find_elements_by_xpath((path))[0]
        results.click()
        time.sleep(1)

        if i%1000==0:
            print('-------------')
            print('Number of Downloaded files: ',i)
            print('-------------')

    # Sleep for 5s to ensure that everythings loaded properly
    time.sleep(5)
    driver.close()

    #-----------------------------------------------------------------------------
    print('-------------')
    print('Download sucessfull!')
    print('Files downloaded to '+path_download_dir)
    


def main():

    # Paths for webscrapper
    urlpage = 'https://www.geoportal-mv.de/portal/Geowebdienste/INSPIRE_Atom_Feeds?feed=https%3A%2F%2Fwww.geodaten-mv.de%2Fdienste%2Finspire_bu_3dgbm_download#feed=https%3A%2F%2Fwww.geodaten-mv.de%2Fdienste%2Finspire_bu_3dgbm_download%3Ftype%3Ddataset%26id%3Dc0384250-7942-4b6h-8bl42-294146a9g028' 
    xpath1 = '/html/body/div[2]/div[2]/div[2]/div[1]/div[2]/div/div[3]/div/div/div/div[10]/div/ul/li/div[7]/div/ul/li[1]/a'
    xpath2 = '/html/body/div[2]/div[2]/div[2]/div[1]/div[2]/div/div[3]/div/div/div/div[10]/div/ul/li/div[7]/div/ul/li[5471]/a'
    driver_dir = '/Users/Felix/Documents/Studium/PhD/05_Projects/02_Estimate_Building_Heights/preprocessing/Webscraper/chromedriver'
    
    # Path of output dir
    download_dir = "/Users/Felix/Desktop/test"

    # Start to webscrape
    webscrape_meckpomm(urlpage,download_dir, driver_dir, xpath1, xpath2,10)   

if __name__ == "__main__":
    main()

