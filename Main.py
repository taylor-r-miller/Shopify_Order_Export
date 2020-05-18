import sys
print(sys.version)
from time import sleep
import requests
import gspread
from datetime import datetime
from datetime import timedelta
from oauth2client.service_account import ServiceAccountCredentials
import ShopifyETLFunctions


'''
exports orders from shopify into a google sheet

'''


# shop url must maintan this format; used in api calls and will fail if anything else is added to it
# see how it is concated in functions for further refrence
SHOP_URL = 'https://YOUR_SHOP.myshopify.com'

# generate an access token through shopify, will be passed as header so use a dictionary
SHOPIFY_ACCESS_TOKEN = {
    "X-Shopify-Access-Token": "your_shop_token"}


# sheet info for the doc the function outputs to
SPREADSHEET = ''  # document name
SHEET_NAME = ''  # sheet name to output to, should be an empty sheet

'''
generate service acct credentials and give acces to sheets/drive api
if the files are not in the same directory provide a path as well
'''
GOOGLE_CREDS_FILE = 'path_to_file/fake_creds.json'


def main():
    # pass our credentials as global variables
    try:

        # function also returns shopify token which is not needed in this function so unpack to unnamed var
        credentials, _, _, _, _ = GetCredentials()

        gc = gspread.authorize(credentials)

        # get orders
        orders = ShopifyETLFunctions.GetOrders()

        output_sheet = gc.open(SPREADSHEET)

        # find the last row of the sheet

        last_row = str(len(output_sheet.worksheet(
            SHEET_NAME).col_values(1)) + 1)

        post to our spread sheet
        output_sheet.values_update(
            SHEET_NAME + '!A' + last_row,
            params={'valueInputOption': 'RAW'},
            body={'values': orders}
        )

        # now that we have posted our orders check to see if that have been any orders that were missed in the past 4 weeks

        missed_orders = ShopifyETLFunctions.CheckForMissedEntries()

        if missed_orders is not None:
            # sleep to give google a time to catch up with what we just did then post any missed orders
            sleep(10)

            last_row = str(len(output_sheet.worksheet(
                SHEET_NAME).col_values(1)) + 1)

            output_sheet.values_update(
                SHEET_NAME + '!A' + last_row,
                params={'valueInputOption': 'RAW'},
                body={'values': missed_orders}
            )

        # testing how long it takes for git to sync

        print('posted', len(orders), 'orders')

    except Exception as e:
        print(f'--MAJOR ERROR-- Shopify ETL Failed \n{e}')


def GetCredentials():
    '''
    pass the creds through a function so we can import them into our functions file
    '''

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_CREDS_FILE, scope)

    header = SHOPIFY_ACCESS_TOKEN

    return credentials, header, SPREADSHEET, SHEET_NAME, SHOP_URL


if __name__ == '__main__':
    main()
