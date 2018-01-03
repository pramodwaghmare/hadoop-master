# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import requests
from bs4 import BeautifulSoup
        
def usdConversionRate():
    url = 'https://in.finance.yahoo.com/q?s=INR=X'
    response = requests.get(url)    
    html = response.content    
    soup = BeautifulSoup(html)    
    listOfSpans = soup.find_all("span")    
    relevantSpans = []    
    for item in listOfSpans:
        text = item.attrs.get('id')
        if text != None:
            if "yfs_l10_inr" in text:
                relevantSpans.append(item.text)
    return relevantSpans[0]
    
def inrToUsd(AmountInRupees):
    rate = float(usdConversionRate())
    USD_Equivalent = AmountInRupees/rate
    return USD_Equivalent

def main():
    print("Please input the INR amount")
    print "\n"
    inr = input()
    usd = inrToUsd(inr)
    print("USD equivalent of INR {} is US ${}".format(inr,usd))

main()
