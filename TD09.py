import requests, json, sqlite3, urllib, hmac, hashlib, time

API_KEY = 'wvjKIjep99fBUxwAgyx0Vc2MtWIBLsF1Org4enIHJF7jcUP5jmcrFJTUJfMkoBsL'
API_SECRET = 'cG0lCLfxCUmTtooGY2DsQzxYxawAETMsmT4dmMSwVH5fEjEu6nPDkKcOlGq97hkH'

#see list of available asset pairs
def getCurrencies():
    print(requests.get("https://api.binance.com/api/v3/ticker/price").json())
    
#get ask or bid price for specific asset 
def getDepth(direction, asset): 
    r = requests.get("https://api.binance.com/api/v3/ticker/bookTicker?symbol=" + asset).json()
    if direction == 'BUY':
        print('The ask price of ' + asset + ' is : ' + r['askPrice'])
    elif direction == 'SELL':
        print('The bid price of ' + asset + ' is : ' + r['bidPrice'])

#get 5 last bid and ask prices for specific asset
def orderBook(asset):
    print(requests.get("https://api.binance.com/api/v1/depth?symbol=" + asset + "&limit=5").json())

#get connection element to sqlite database
def sqlConnection():
    try:
        con = sqlite3.connect('datas.db')
        return con
    except Error:
        print(Error)

def createCandleTable(con, asset, duration):
    cursorObj = con.cursor()
    setTableName = str("Binance_" + asset + "_"+ duration)
    tableCreationStatement = """CREATE TABLE """ + setTableName + """(opentime
    INT, open REAL, high REAL, low REAL, close REAL,volume REAL, closetime int, quotevolume
    REAL, nbtrades REAL, takerbuybaseassetvolume REAL, takerbuyquoteassetvolume REAL, ignore REAL)"""
    cursorObj.execute(tableCreationStatement)
    con.commit()
    con.close()
    
def createTradeTable(con, asset):
    cursorObj = con.cursor()
    setTableName = str("Binance_" + asset)
    tableCreationStatement = """CREATE TABLE """ + setTableName + """(aggTradeId INT, price REAL, 
    quantity REAL, firstTradeId INT, lastTradeId INT, timestamp INT, buyerMaker BOOL, tradeBestPriceMatch BOOL)"""
    cursorObj.execute(tableCreationStatement)
    con.commit()
    con.close()

#get candles data for specific asset and duration
#store data in sqlite table created with createCandleTable()
def refreshDataCandles(con, asset, duration):
    r = requests.get("https://api.binance.com/api/v3/klines?symbol=" + asset + "&interval=" + duration).json()
    setTableName = str("Binance_" + asset + "_"+ duration)
    c = con.cursor()
    c.executemany('INSERT INTO ' + setTableName + ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', r)
    con.commit()
    con.close()

#get all trade data for specific asset 
#store aggregated data in sqlite table created with createTradeTable()
def refreshData(con, asset):
    r = requests.get("https://api.binance.com/api/v3/aggTrades?symbol=" + asset).json()
    setTableName = str("Binance_" + asset)
    c = con.cursor()
    for i in range(len(r)):
        l = list(r[i].values())
        c.executemany('INSERT INTO ' + setTableName + ' VALUES (?,?,?,?,?,?,?,?)', (l,))
    con.commit()
    con.close()

#create a new order to buy/sell specific asset with binance account
#using API key (secret needed to authentify)
def createOrder(api_key, secret_key, direction, price, amount, asset, orderType):
    params = {
        'symbol' : asset,
        'price' : price,
        'quantity' : amount,
        'side' : direction,
        'type' : orderType,
        'timeInForce' : 'GTC',
        'recvWindow' : 5000, 
        'timestamp' : int(time.time() * 1000 - 5000)
    }
    params['signature'] = hmac.new(secret_key.encode('utf-8'), urllib.parse.urlencode(params).encode('utf-8'), hashlib.sha256).hexdigest()
    r = requests.post(url = "https://api.binance.com/api/v3/order", headers = {'X-MBX-APIKEY': api_key}, params = params).json()
    print(r)

#cancel order passed using API key 
#uuid or order id needed
def cancelOrder(api_key, secret_key, asset, uuid):
    params = {
        'symbol' : asset,
        'orderId' : uuid,
        'recvWindow' : 5000, 
        'timestamp' : int(time.time() * 1000 - 5000)
    }
    params['signature'] = hmac.new(secret_key.encode('utf-8'), urllib.parse.urlencode(params).encode('utf-8'), hashlib.sha256).hexdigest()
    r = requests.delete(url = "https://api.binance.com/api/v3/order", headers = {'X-MBX-APIKEY': api_key}, params = params).json()
    print(r)
    
con = sqlConnection()
#getCurrencies()
#getDepth('SELL', 'ETHBTC')
#orderBook('BTCEUR')
#createCandleTable(con, 'BTCEUR', '5m')
#createTradeTable(con, 'BTCEUR')
#refreshDataCandles(con, 'BTCEUR', '5m')
#refreshData(con, 'BTCEUR')
#createOrder(API_KEY, API_SECRET, 'SELL', '100', '1', 'ETHEUR', 'LIMIT')
#cancelOrder(API_KEY, API_SECRET, 'BTCEUR', 0)