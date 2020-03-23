from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time
import bs4
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
#binary = FirefoxBinary('/opt/firefox/firefox', log_file="/usr/local/airflow/dags/firefox.log")

options = Options()
options.headless = True


browser = webdriver.Firefox(options=options, executable_path='/Users/arolambo/Code/harward-media-api/geckodriver')

browser.get('https://app.clickfunnels.com/users/sign_in')

user_email = browser.find_element_by_id("user_email")
user_email.send_keys('matt@harwardmedia.com')
user_password = browser.find_element_by_id("user_password")
user_password.send_keys('7EDw3LM*xJuGWe=V>)4Bhx4g')
user_password.send_keys(Keys.ENTER)
delay = 20

try:
    browser.implicitly_wait(20)
    WebDriverWait(browser, delay).until(EC.presence_of_element_located((By.CLASS_NAME, 'quick-links')))

except TimeoutException:
    print("Loading took too much time!")

ClickFunnelsLink = browser.find_element_by_link_text('Click Funnels')
ClickFunnelsLink.click()
htmlContents = browser.find_elements_by_class_name("groupForFunnel")
for htmlContent in htmlContents:

    groupName = htmlContent.find_element_by_class_name('title').text
    allFunnels = htmlContent.find_elements_by_class_name('pageListingItem')

    for allFunnel in allFunnels:
        funnelName = allFunnel.find_element_by_class_name('pageListingTitle').text
        lastUpdated = allFunnel.find_element_by_class_name('timeago').get_attribute('title')
        steps = allFunnel.find_element_by_class_name('funnelStatSmall').text
        stepLink = allFunnel.find_element_by_class_name('btn-edit')
        stepLinkUrl = stepLink.get_attribute('href')
        urlData = stepLinkUrl.split('/')
        funnelId = str(urlData[-1])
        browser.execute_script("window.open('" + stepLinkUrl + "');")
        time.sleep(10)
        browser.switch_to.window(browser.window_handles[1])
        time.sleep(10)
        sideSortList = browser.find_element_by_class_name('sideSortList')
        visitUrl = sideSortList.find_element_by_xpath("//a[@data-original-title='Visit Funnel URL']").get_attribute(
            'href')
        soup = bs4.BeautifulSoup(sideSortList.get_attribute('innerHTML'), 'html.parser')
        funnelSteps = soup.findAll("div", {"class": "funnel_step"})
        browser.get(visitUrl)
        htmlSource = browser.page_source
        browser.close()
        time.sleep(5)
        browser.switch_to.window(browser.window_handles[-1])
        time.sleep(10)
        stepNameAr = []
        stepTypeAr = []

        for funnelStep in funnelSteps:
            stepName = funnelStep.find("div", {"class": "funnelSideStepPageTitle"}).get_text()
            stepType = funnelStep.find("div", {"class": "funnelSideStepPageViews"}).get_text()

            if stepName:
                stepNameAr.append(stepName)

            if stepType:
                stepTypeAr.append(stepType)

        stepNameAr = [x.replace('\n', '') for x in stepNameAr]
        stepNameAr = ','.join(map(str, stepNameAr))
        stepTypeAr = [x.replace('\n', '') for x in stepTypeAr]
        stepTypeAr = ','.join(map(str, stepTypeAr))

        print(funnelId)