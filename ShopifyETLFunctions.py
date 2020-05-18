from datetime import datetime
from datetime import timedelta
import requests
from time import sleep
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import re
import Main

# pass our gspread credentials as global variables

credentials, header, SPREADSHEET, SHEET_NAME, SHOP_URL = Main.GetCredentials()


gc = gspread.authorize(credentials)


# this function builds the date Ranges to use in our API Call
# should always return the last full period (monday-sunday)

# to reuse code just change the dates in date range generator
def DateRangeGenerator(weeks_ago):

    # convert the weeks adjust being passed to the function to days
    adjusted_days = weeks_ago * 7

    today = datetime.today()

    if today.weekday() == 0:

        last_monday = today - timedelta(today.weekday() + 7)

    else:

        last_monday = today - timedelta(today.weekday())

    # make sure we are always using a date range that is past

    day_difference = abs((last_monday - today).days)

    if day_difference < 7:

        last_monday -= timedelta(7)

    last_sunday = last_monday + timedelta(6)

    # if we are passing an adjustment to the function account for it
    last_monday -= timedelta(adjusted_days)

    # format the dates to what shopify is expecting
    date_ending = 'T' + "00:00:00" + '-' + "04:00"
    date_ending_2 = 'T' + "23:59:59" + '-' + "04:00"

    monday = last_monday.strftime('%Y-%m-%d') + date_ending
    sunday = last_sunday.strftime('%Y-%m-%d') + date_ending_2

    print("getting orders from", last_monday.strftime(
        '%m/%d/%Y'), " to", last_sunday.strftime('%m/%d/%Y'))

    return monday, sunday


# calls shopify api to retrive the order ids of every order placed  within our date range


def GetOrderIds(weeks_ago):

    # make our first request to get the ids of the orders in our date range

    monday, sunday = DateRangeGenerator(weeks_ago)

    url = SHOP_URL + "/admin/api/2020-01/orders.json?created_at_min=" + \
        monday + "&created_at_max=" + sunday + "&limit=250&fields=id"

    order_ids = []

    # shopify handles rest api pagination by generating a cursor object to iterate
    # through
    cursor = requests.get(url, headers=header)

    more_info = True

    while more_info:

        cursor = requests.get(url, headers=header)

        try:

            for row in cursor.json()['orders']:
                order_ids.append(row)

            # extract the url passed back in the server response header
            url = list(re.search('<(.+)>', cursor.headers['Link']).group())

            url = "".join(url[1:-1])

            # extract the relationship of the url
            # passed back in the server response header

            relationship = re.search('".+"', cursor.headers['Link']).group()

            if relationship == '"previous"':
                more_info = False

        except KeyError:
            more_info = False

    # loop through the list of dictionaries and grab our ids as a string
    order_ids = [str(orders['id']) for orders in order_ids]

    print(f'found {len(order_ids)} orders for that date range ')

    return order_ids


def GetOrders(weeks_ago=0):
    '''
    week adjust is passed through all our funtions so that we can use everything in our redundant saftey function
    that checks the previous amount of weeks to ensure everything was imported correctl

    '''

    order_ids = GetOrderIds(weeks_ago)

    # loop through the ids and make a second api call returning the order information
    # api call limit is 40 with leaking bucket @ 2/sec ,so! if the length is less that 39 run it quickly, otherwise run it very slowly

    order_feilds = "name,email,created_at,line_items,billing_address,note,tags"

    orders = []

    if len(order_ids) < 39:
        # run quickly
        for order_id in order_ids:

            raw_order_info = requests.get(SHOP_URL + "/admin/api/2020-01/orders/" +
                                          order_id + ".json?fields=" + order_feilds, headers=header).json()

            # grab all our info that is not line item spesific
            order_info = raw_order_info

            # we want these to break the code if there is no value because that means somthing is very wrong with shopify
            order_number = raw_order_info['order']['name']
            created_at = raw_order_info['order']['created_at']

            # these are all optional values

            # handle for no email
            try:
                email = raw_order_info['order']['email']
            except KeyError:
                email = None
            # handle for no notes
            try:
                note = raw_order_info['order']['note']
            except KeyError:
                note = None
            # handle for no tags

            try:
                tag = raw_order_info['order']['tags']
            except KeyError:
                tag = None

            try:
                customer_name = raw_order_info['order']['billing_address']['name']
            except KeyError:
                customer_name = None

            #
            # grab our line item information and append it with the rest of the info to the order list

            for info in raw_order_info['order']['line_items']:

                line_item_name = info['name']
                line_item_qty = info['quantity']

                orders.append([customer_name, order_number, email,
                               created_at, line_item_qty, line_item_name, note, tag])

    else:
        # run slowly
        # leakby bucket rate is 2/sec so .8 sleep should keep us from bucket overflow

        for order_id in order_ids:
            sleep(.8)

            raw_order_info = requests.get(SHOP_URL + "/admin/api/2020-01/orders/" +
                                          order_id + ".json?fields=" + order_feilds, headers=header).json()

            # grab all our info that is not line item spesific
            order_info = raw_order_info

            # we want these to break the code if there is no value because that means somthing is very wrong with shopify
            order_number = raw_order_info['order']['name']
            created_at = raw_order_info['order']['created_at']

            # these are all optional values

            # handle for no email
            try:
                email = raw_order_info['order']['email']
            except KeyError:
                email = None
            # handle for no notes
            try:
                note = raw_order_info['order']['note']
            except KeyError:
                note = None
            # handle for no tags

            try:
                tag = raw_order_info['order']['tag']
            except KeyError:
                tag = None

            try:
                customer_name = raw_order_info['order']['billing_address']['name']
            except KeyError:
                customer_name = None

            #
            # grab our line item information and append it with the rest of the info to the order list

            for info in raw_order_info['order']['line_items']:

                line_item_name = info['name']
                line_item_qty = info['quantity']

                orders.append([customer_name, order_number, email,
                               created_at, line_item_qty, line_item_name, note, tag])

    return orders


def CheckForMissedEntries():

    try:

        posted_order_nums = gc.open(SPREADSHEET).worksheet(
            SHEET_NAME).col_values(2)
        posted_line_items = gc.open(SPREADSHEET).worksheet(
            SHEET_NAME).col_values(6)

        # zip two lists together an join them to create a unique col to check against with
        posted_uniques = ["".join(list(a))
                          for a in zip(posted_order_nums, posted_line_items)]
        # grab all orders between this past sunday and monday x amt of weeks ago
        new_orders = GetOrders(weeks_ago=4)

        new_order_uniques = [row[1] + row[5] for row in new_orders]

        missed_orders = []

        for index, row in enumerate(new_order_uniques):

            if row not in posted_uniques:
                missed_orders.append(new_orders[index])

        print(f'Found {len(missed_orders)} missing line_items')

        # correct the date on the missed orders so that our system will find them

        new_date = (datetime.today() - timedelta(2)
                    ).strftime('%Y-%m-%dT:00:00:00-06:00')

        for row in missed_orders:
            row[3] = new_date

        # only return if we have missed orders
        if len(missed_orders) != 0:
            return missed_orders

    except Exception as e:
        print(f'Error in redundant system {e}')
