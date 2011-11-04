#!/usr/bin/env python

"""
Google Android Market Crawler
For the sake of research
1) database file name
2 through n) all the types we want to explore
"""

import sys
import re
import urllib2
import urlparse
import sqlite3 as sqlite
import threading
from BeautifulSoup import BeautifulSoup

__author__ = "Sergio Bernales"

if len(sys.argv) < 2:
    sys.exit("Not Enough arguments!");
else:
    dbfilename = sys.argv[1]
    argLen = len(sys.argv) - 1

    categories = [x.upper() for x in sys.argv[2::]]

#DB Connection: create it and/or just open it
connection = sqlite.connect(dbfilename)
cursor = connection.cursor()

#table that will contain all the permissions of an app of a certain category
cursor.execute('CREATE TABLE IF NOT EXISTS app_permissions (id INTEGER PRIMARY KEY, appname VARCHAR(256), category VARCHAR(256), permission VARCHAR(256), url VARCHAR(256))')
#cursor.execute('CREATE TABLE IF NOT EXISTS urls_to_crawl (category VARCHAR(256), url VARCHAR(256))')

connection.commit()

class MarketCrawler(threading.Thread):
    mainURL = "https://market.android.com"
    topfreeURL = "https://market.android.com/details?id=apps_topselling_free&num=24&cat="
    toppaidURL = "https://market.android.com/details?id=apps_topselling_paid&num=24&cat="
    pageIncrements = 24;

    """
    run()
    This will be the entry point for the thread and it will loop through every
    category provided by the user
    crawl process
    """
    def run(self):
        for cat in categories:
            print cat
            self.crawlAppsForCategory(cat)

    def crawlAppsForCategory(self, cat):
        pageIndex = 0
        curl = self.topfreeURL + cat + "&start="
        currentURL = curl + str(pageIndex)

        while True:
            try:
                request = urllib2.Request(currentURL)
                request.add_header("User-Agent", "PermissionCrawler")
                handle = urllib2.build_opener()
                content = handle.open(request).read()
                soup = BeautifulSoup(content)

                print "Currently on page " + pageIndex + " of the list of app for this Category"
                appURLS = self.extractAppUrls(soup)
                self.extractPermissionsIntoDB(appURLS, cat)

                pageIndex+=24
                currentURL = curl + str(pageIndex)

            except urllib2.HTTPError, error:
                if error.code == 404:
                    print >> sys.stderr, "404 ERROR: %s -> %s" % (error, error.url)
                if error.code == 403:
                    print >> sys.stderr, "403 (NO MORE APP PAGES FOR THIS CATEGORY)ERROR: %s -> %s" % (error, error.url)
                else:
                    print >> sys.stderr, "ERROR: %s" % error
                break
            except Exception, e:
                print >> sys.stderr, "iSERROR: %s" % e
    

    """
    From the page the lists a page of 24 apps of the particular category,
    extract the links to those apps
    """
    def extractAppUrls(self, soup):
        tags = soup('a')
        #to get rid of duplicates since the href get returns links twice
        skip = False         

        appURLS = []
        for tag in  tags:
            href = tag.get("href")
            if skip:
                skip = False
                continue
            if href is not None and re.match('/details', href) and not re.search('apps_editors_choice', href):
                #print href
                appURLS.append(self.mainURL+href)
                skip = True
        
        return appURLS


    """
    Fetch all the URLS in appURLS and extract the permissions.
    Put these permission into the DB
    """
    def extractPermissionsIntoDB(self, appURLS, cat):
        #we can put this URL stuff into its own object /code repetition
        for url in appURLS:
            request = urllib2.Request(url)
            request.add_header("User-Agent", "PyCrawler")
            handle = urllib2.build_opener()
            content = handle.open(request).read()
            soup = BeautifulSoup(content)
            
            appName = soup.find('h1','doc-banner-title').contents[0]
            permissions = soup.findAll('div','doc-permission-description')
            self.pushToDB(appName, cat, permissions, url)
    
    """
    Pushes permissions of a certain app into the DB
    cursor.execute('CREATE TABLE IF NOT EXISTS app_permissions (id INTEGER, appname VARCHAR(256), category VARCHAR(256), permission VARCHAR(256), url VARCHAR(256))')
    """
    def pushToDB(self, appName, cat, permissions, url):
        for p in permissions:
            #print appName, cat, p.contents[0], url 
            cursor.execute("INSERT INTO app_permissions VALUES ((?), (?), (?), (?), (?))", (None, appName, cat, p.contents[0], url ) )
            connection.commit()

if __name__ == "__main__":
    #run the crawler thread
    MarketCrawler().run()
