# Import libraries
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
import time


def webscrape(url_page, path_download_dir, path_geckodriver, path_xpath1, path_xpath2, sleep_time=5):
    """
    Function to download files via webscrapping, using selenium and fireforx's
    geckodriver, which can be downloaded here: https://github.com/mozilla/geckodriver/releases

    How to webscrape:
    0. Download all required packages (incl. selenium and geckodriver)
    1. Open the url with the download links in Firefox
    2. right cklick on the first download link and select 'inspect'
    3. right click again on the higlighted entry in firefox developer mode and choose 'copy path' -> 'xpath'
    4. do the same for the last download link

    Args:
    - url_page (str): address of webpage where files should be downloaded
    - path_download_dir (str): path to dir in which downloads should be saved
    - path_geckodriver (str): path to folder in which geckodriver is stored
    - path_xpath1(str): xpath of file where download should start with
    - path_xpath2 (str): xpath of file where download should stop (all download links in between will be downloaded)
    - sleep_time (int): on some pages it takes some time to load all download links; here, a timer can be set to wait

    Returns:
    -

    Last update: 21/06/21. By Felix.

    """
    # intitialise download
    print('Starting to download from ' + url_page)

    # Set options for webdriver
    options = FirefoxOptions()
    # options.add_argument("--headless")
    #options.headless = True

    # in this case we set the options to avoid pop up windows when downloading
    fp = webdriver.FirefoxProfile()

    # Set 0 = desktop, 1 = default download folder, 2 = specified donwload folder
    fp.set_preference("browser.download.folderList", 2)
    fp.set_preference("browser.download.manager.showWhenStarting", False)

    # Setting for specific download folder (downloadDir)
    fp.set_preference("browser.download.dir", path_download_dir)

    # Setting to disable download pop up window and directly download file
    fp.set_preference(
        "browser.helperApps.neverAsk.saveToDisk",
        "application/zip, application/octet-stream, multipart/x-zip, application/zip-compressed, application/x-zip-compressed")

    # Update preferences
    fp.update_preferences()

    # options.profile(fp)

    # Run firefox webdriver from executable path of your choice
    driver = webdriver.Firefox(firefox_profile=fp, executable_path=path_geckodriver, options=options)
    #driver = webdriver.Firefox(executable_path = path_geckodriver, options=options)

    # -----------------------------------------------------------------------------

    # Get web page
    driver.get(url_page)

    # Sleep for 15s to load feed
    print('Waiting {} secs to let all download links load...'.format(sleep_time))
    time.sleep(sleep_time)
    print('-------------')

    # Find elements by xpath
    #download_elems = driver.find_elements_by_xpath((path_xpath1))
    #print('Number of downloadable files: ', len(download_elems))
    # print('-------------')

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
    xpath_pt1 = path_xpath1[0:pos_start + 1]
    xpath_pt2 = path_xpath1[pos_start + pos_end:]

    # Get start for range (starting value between [])
    a = int(path_xpath1[pos_start + 1:pos_start + pos_end])
    # Get end for range (end value between [])
    b = 1 + int(path_xpath2[pos_start + 1:(pos_start + pos_end + abs(len(path_xpath2) - len(path_xpath1)))])

    # the range is definded from the xpath path_xpath1 to xpath path_xpath2
    for i in range(a, b):
        # build xpath for loop
        path = xpath_pt1 + str(i) + xpath_pt2
        print(path)
        # use selenium webdriver to click and download
        results = driver.find_elements_by_xpath((path))[0]
        results.click()
        time.sleep(1)

        if i % 1000 == 0:
            print('Number of Downloaded files: ', i)

    # -----------------------------------------------------------------------------
    # Sleep for 5s to ensure that everythings loaded properly
    time.sleep(5)
    driver.quit()

    # -----------------------------------------------------------------------------
    print('-------------')
    print('Download sucessfull!')
    print('{} Files downloaded to '.format(b - a) + path_download_dir)


def main():

    # Paths for webscrapper
    urlpage = 'https://services.cuzk.cz/gml/inspire/bu/epsg-4258/'
    xpath1 = '/html/body/div[2]/div/div[7]/div[2]/ul/li[1]/div[6]/div[2]/ul/li[1]/a'
    xpath2 = '/html/body/div[2]/div/div[7]/div[2]/ul/li[1]/div[6]/div[2]/ul/li[3]/a'

    geckodriver_dir = '/Users/Felix/Documents/Studium/PhD/05_Projects/02_Estimate_Building_Heights/preprocessing/Webscraper_Thueringen/geckodriver'

    # Path of output dir
    download_dir = "/Users/Felix/Desktop/test"

    # Start to webscrape
    webscrape(urlpage, download_dir, geckodriver_dir, xpath1, xpath2, 15)


if __name__ == "__main__":
    main()
