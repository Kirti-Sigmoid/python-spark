import csv
import requests
import json

# ticker_symbols = ["A","R","ACER","AAPL","U","F","Y","W","D","H","V","G","K","L","X","FIND","KO","MEME", "ACN",
# 				  "T","ALL","ITC","SO","SPY"]

ticker_symbols = ["ABCB", "ABG", "ABM", "ABTX", "ACA", "ACLS", "ADC", "ADTN", "ADUS", "AEIS", "AEL", "AGO", "AGYS",
                  "AHH",
                  "AIN", "AIR", "AIT", "AJRD", "AKR", "ALEX", "ALG", "ALGT", "ALRM", "AMBC", "AMCX"]
url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
headers = {
    'X-RapidAPI-Key': '7216a28845mshd2666b792fb806dp1e8267jsn506215766b33',
    'X-RapidAPI-Host': 'stock-market-data.p.rapidapi.com'
}

file_base_path = "/Users/kirti_sigmoid/PycharmProjects/pythonSparkProject/Data/"

for ticker_value in ticker_symbols:
    print(ticker_value)
    querystring = {"ticker_symbol": ticker_value, "years": "5", "format": "json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    query = json.loads(response.text)
    print(query)
    # open the file in the write mode
    file_name = file_base_path + ticker_value + '.csv'

    count = 1
    if not query['historical prices']:
        continue

    csvfile = open(file_name, "w")
    csvwriter = csv.writer(csvfile)
    list = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Date"]
    csvwriter.writerow(list)

    for result in query['historical prices']:
        lis = [result["Open"], result["High"], result["Low"], result["Close"], result["Adj Close"], result["Volume"],
               result["Date"][:10]]
        csvwriter.writerow(lis)
        count += 1
        print(result)
