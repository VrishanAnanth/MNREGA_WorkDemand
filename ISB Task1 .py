#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess

numbers=[]
cleannum=[]
districtnames=[]

mayhousehold=[]
class StateScraper(scrapy.Spider):
    name = 'StateScraper'
    def start_requests(self):
        url= 'http://mnregaweb4.nic.in/netnrega/demand_emp_demand.aspx?lflag=eng&file1=dmd&fin=2019-2020&fin_year=2019-2020&source=national&Digest=kW/j9JdpWS32ueCNesmYTQ'
        yield scrapy.Request(url=url, callback=self.findurls)
        
    def findurls(self, response):
        stateurls=[]
        for i in range(0,50):
            links=response.xpath(f'//*[@id="ContentPlaceHolder1_Repeater1_Label2_{i}"]/a/@href').extract()
            for link in links:
                stateurls.append(link)
        for url in stateurls:
            yield response.follow(url=url, callback= self.getdata)
                                  
        
    
    def getdata(self,response):
        for i in range(0,80):
            links=response.xpath(f'//*[@id="ContentPlaceHolder1_Repeater1_Label2_{i}"]/a/text()').extract()
            for link in links:
                districtnames.append(link)
        num= response.xpath('//td[@align="right"]/text()').extract()
        for n in num:
            numbers.append(n)
        
        
        
        

                            
        
                                 
process= CrawlerProcess()
process.crawl(StateScraper)
process.start()
#print(len(districtnames))

for i in numbers:
    try:
        int(i)
        cleannum.append(int(i))
    except:
        numbers.remove(i)



n=12
cleannum=[cleannum[i:i+n] for i in range(0,len(cleannum),n)]



april=[cleannum[i][0] for i in range(0,len(districtnames))]
may=[cleannum[i][1] for i in range(0,len(districtnames))]
june=[cleannum[i][2] for i in range(0,len(districtnames))]
july=[cleannum[i][3] for i in range(0,len(districtnames))]
aug=[cleannum[i][4] for i in range(0,len(districtnames))]
sep=[cleannum[i][5] for i in range(0,len(districtnames))]
octo=[cleannum[i][6] for i in range(0,len(districtnames))]
nov=[cleannum[i][7] for i in range(0,len(districtnames))]
dec=[cleannum[i][8] for i in range(0,len(districtnames))]
jan=[cleannum[i][9] for i in range(0,len(districtnames))]
feb=[cleannum[i][10] for i in range(0,len(districtnames))]
mar=[cleannum[i][11] for i in range(0,len(districtnames))]

final={'District':districtnames,'April':april, 'May':may,'June':june,'July':july,'August':aug,'September':sep,'October':octo,'November':nov,'December':dec,'January':jan,'February':feb,'March':mar}

mnrega = pd.DataFrame(final)
print(mnrega.head(8))


# In[ ]:






