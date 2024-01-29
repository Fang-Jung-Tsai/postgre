#import selenium
from selenium.webdriver import chrome
from selenium.webdriver.common.keys import Keys
import time
import random
import os

class CrawlerNARL:
    account_filename = 'ncree_account.txt'

    def __init__(self, debug=False):
        self.debug = debug
        self.online = True

        # Get the user's home directory
        home_dir = os.path.expanduser("~")
        account_path = os.path.join(home_dir, self.account_filename)

        # Check if the account.txt file exists
        if not os.path.exists(account_path):
            raise Exception(f"The '{self.account_filename}' file does not exist in the user's home directory.")

        # Read the account and password from the file
        with open(account_path, 'r') as f:
            self.account = f.readline().strip()
            self.password = f.readline().strip()

        # Check the account and password
        if self.account == '' or self.password == '':
            raise Exception('Please input the account and password in the file: ' + account_path)
        
        self.driver = chrome.webdriver.WebDriver()

    def login(self, title, body):
        if self.driver:
            # open the login page
            self.driver.get('https://sso.narlabs.org.tw/nidp/idff/sso?RequestID=idHw9trL6JxC1qOtqJLm0tMP0eQPQ&MajorVersion=1&MinorVersion=2&IssueInstant=2023-05-05T07%3A17%3A26Z&ProviderID=https%3A%2F%2Fsso.narlabs.org.tw%3A443%2Fnesp%2Fidff%2Fmetadata&RelayState=MQ%3D%3D&consent=urn%3Aliberty%3Aconsent%3Aunavailable&agAppNa=Portal2&ForceAuthn=false&IsPassive=false&NameIDPolicy=onetime&ProtocolProfile=http%3A%2F%2Fprojectliberty.org%2Fprofiles%2Fbrws-art&target=https%3A%2F%2Fportal.narlabs.org.tw%2Fhome&AuthnContextStatementRef=secure%2Fname%2Fpassword%2Furi')
            time.sleep(1 + random.random())

            # input the account and password
            tb_account = self.driver.find_element('xpath', '//*[@id="Ecom_User_ID"]')
            tb_account.send_keys(self.account)

            tb_password = self.driver.find_element('xpath', '//*[@id="Ecom_Password"]')
            tb_password.send_keys(self.password)

            # click the login button
            btn_login = self.driver.find_element('xpath', '//*[@id="loginButton2"]')
            random.random()
            btn_login.click()
            time.sleep(1 + random.random())
            btn_login_2 = self.driver.find_element('xpath', '/html/body/div[1]/div[1]/div/div/span[2]/div[2]/ul/li[2]/a')
            btn_login_2.click()
            time.sleep(3 + random.random())
            info = self.driver.find_element('xpath', '/html/body/div[6]/div/div/div[2]/div[2]/div/div[1]/section/div/div/div/div[1]/div/div[1]/button')
            info.click()
            into_book = self.driver.find_element('xpath', '//*[@id="myDropdown1"]/a[6]')
            into_book.click()

            time.sleep(1 + random.random())
            self.driver.switch_to.window(self.driver.window_handles[1])
            edit_book = self.driver.find_element('xpath', '/html/body/div/div/div[1]/div[1]/div[2]/table/tbody/tr/td/table/tbody/tr[1]/td/div/div/table/tbody/tr[2]/td/div')
            edit_book.click()
            time.sleep(random.random() + 10)
            title_input = self.driver.find_element('xpath', '/html/body/div/div/div[2]/div/div/div/div/div[3]/div[1]/div/div[1]/table[2]/tbody/tr/td/table/tbody/tr/td[3]/input')
            # clear the title
            title_input.send_keys(Keys.CONTROL + 'a')
            title_input.send_keys(Keys.BACKSPACE)
            title_input.send_keys(title)
            time.sleep(random.random() + 10)
            outter_frame = self.driver.find_element('xpath', '/html/body/div/div/div[2]/div/div/div/div/div[3]/iframe')
            self.driver.switch_to.frame(outter_frame)
            # find the iframe
            inner_frame = self.driver.find_element('xpath', '/html/body/div/table/tbody/tr/td/table/tbody/tr[5]/td/div/div[2]/div[1]/div/div/div/div/div/iframe')
            # switch to the iframe
            self.driver.switch_to.frame(inner_frame)
            # find the body
            body_input = self.driver.find_element('xpath', '/html/body')
            # clear the body by ctrl+a and backspace
            body_input.send_keys(Keys.CONTROL + 'a')
            body_input.send_keys(Keys.BACKSPACE)
            # input the content to html body
            body_input.send_keys('<h1>' + body + '</h1>')

            # switch to the default frame
            self.driver.switch_to.default_content()
            # find save button
            save_btn = self.driver.find_element('xpath', '/html/body/div/div/div[2]/div/div/div/div/div[3]/div[1]/div/div[1]/table[1]/tbody/tr/td/table/tbody/tr/td[25]/button')
            time.sleep(random.random() + 15)
            # click the save button
            save_btn.click()
            time.sleep(random.random() + 10)
            # click the confirm button
            confirm_btn = self.driver.find_element('xpath', '/html/body/div[3]/div[3]/div/div/div/table[2]/tbody/tr/td/table/tbody/tr/td/button')
            confirm_btn.click()

if __name__ == '__main__':
    title = input("Enter the title: ")
    body = input("Enter the body: ")
    crawler = CrawlerNARL(debug=True)
    crawler.login(title, body)
    input("Press any key to exit...")
