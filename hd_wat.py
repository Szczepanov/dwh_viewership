import base64
import threading
import urllib.request
import urllib.error
import urllib.parse
from datetime import timedelta, date
import pypyodbc
from bs4 import BeautifulSoup
from urllib.parse import urlparse


class HDWAT(threading.Thread):
    running = True
    connection = None
    channel_links = []
    start_date = date(2016, 7, 1)
    end_date = date(2016, 7, 8)
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.19 (KHTML, like Gecko) Ubuntu/12.04 Chromium/18.0.1025.168 Chrome/18.0.1025.168 Safari/535.19'

    def __init__(self, id):
        self.id = str(id)
        threading.Thread.__init__(self)

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    # def getID(self, tablename, idcolumn, columnname, value):
    #     cursor = self.connection.cursor()
    #     SQLQuery = 'SELECT ' + idcolumn + ' FROM ' + tablename + ' WHERE ' + columnname + ' = \'' + value + '\';'
    #     return cursor.execute(SQLQuery).fetchone()[0]

    def getPrograms(self):
        if self.channel_links:
            channel_links_temp = self.channel_links
            channel = channel_links_temp.pop()
            try:
                airDate = str(channel[str(channel).find('dzien=') + len('dzien='):])
            except Exception:
                airDate = ''
            file = open('D:\szkola\HD_WAT\datascraper\Program.txt', 'a+', encoding='UTF-8')
            # THREAD ID + channel for testing
            # file.write(str(self.id) + ': '+ str(channel) +'\n')
            response = urllib.request.urlopen(
                urllib.request.Request(channel, headers={'User-Agent': self.user_agent}))
            soup = BeautifulSoup(response, "html.parser")
            x = soup.find('div', {"class": "lista"}).select('ul li')
            try:
                for line2 in x:
                    # print(line2.find('span').text.strip() + ':' + line2.find('span').string.strip())
                    try:
                        programName = line2.find('span').text.strip()
                    except Exception:
                        programName = ''
                    try:
                        programDescription = line2.find('p').text.strip()
                    except Exception:
                        programDescription = ''
                    try:

                        timeStart = line2.find('em').text.strip()
                    except Exception:
                        timeStart = '0'
                    try:
                        index = x.index(line2)
                        next_value = x[index + 1] if index + 1 < len(x) else None
                        if next_value is not None:
                            timeEnd = next_value.find('em').text.strip()
                        else:
                            timeEnd = '0'
                    except Exception:
                        timeEnd = '0'
                    try:
                        url2 = "http://www.telemagazyn.pl" + line2.a['href']

                        response2 = urllib.request.urlopen(
                            urllib.request.Request(url2, headers={'User-Agent': self.user_agent}))
                        soup = BeautifulSoup(response2, "html.parser")
                        try:
                            programCategory = soup.find("meta", {"itemprop": "genre"})['content']
                        except TypeError:
                            programCategory = '0'
                        try:
                            ageLimitTemp = soup.find("h1")['class']
                            ageLimit = ''.join(ageLimitTemp)[4:]
                        except Exception:
                            ageLimit = '0'
                        try:
                            duration = soup.find("meta", {"itemprop": "timeRequired"})['content']
                        except Exception:
                            duration = '0'
                        file.write(str(programName) + '|' + str(ageLimit) + '|' + str(programDescription) + '|' + str(
                            programCategory) + '|' + str(timeStart) + '|' + str(timeEnd) + '|' + str(
                            duration) + '|' + str(airDate) + '\n')
                    except Exception:
                        pass
            except TypeError:
                pass
            file.close()
        else:
            self.running = False

    # def getProgramsCategory(self):
    #     # self.dbConnection()
    #     if self.channel_links:
    #         channel_links_temp = self.channel_links
    #         channel = channel_links_temp.pop()
    #         channel_url = channel
    #         try:
    #             response = urllib.request.urlopen(
    #                 urllib.request.Request(channel_url, headers={'User-Agent': self.user_agent}))
    #             soup = BeautifulSoup(response, "html.parser")
    #             x = soup.find('div', {"class": "lista"}).select('ul li')
    #             # find program category and insert into database
    #             for line in x:
    #                 dictColumnValues = {}
    #                 url2 = "http://www.telemagazyn.pl" + line.a['href']
    #                 response2 = urllib.request.urlopen(
    #                     urllib.request.Request(url2, headers={'User-Agent': self.user_agent}))
    #                 soup = BeautifulSoup(response2, "html.parser")
    #                 x2 = soup.find("meta", {"itemprop": "genre"})['content']
    #                 dictColumnValues.update({'ProgramCategoryName': x2})
    #                 # self.dbInsert('dimProgramCategory', dictColumnValues)
    #                 # writeToFile('D:\szkola\HD_WAT\datascraper\dimProgramCategory.txt', dictColumnValues + '\n')
    #         except Exception as e:
    #             print(e)
    #     else:
    #         self.running = False

    @staticmethod
    def getChannelLinks(self):
        self.dbConnection(self)
        url = "http://www.telemagazyn.pl/stacje/"
        response = urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': self.user_agent}))
        soup = BeautifulSoup(response, "html.parser")
        try:
            x = soup.find('div', {"class": "listaStacji"}).select('.polska > a')
            for link in x:
                for single_date in self.daterange(self, self.start_date, self.end_date):
                    url = (
                        "http://www.telemagazyn.pl" + link['href'] + "?dzien=" + single_date.strftime(
                            "%Y-%m-%d")).strip(
                        '\'"')
                    url_test = urlparse(url)
                    if url_test.scheme == 'http':
                        self.channel_links.append(url)

        except Exception as e:
            print(e)

    # @staticmethod
    # def getChannels(self):
    #     self.dbConnection(self)
    #     url2 = "http://www.telemagazyn.pl/stacje/"
    #     response = urllib.request.urlopen(urllib.request.Request(url2, headers={'User-Agent': self.user_agent}))
    #     soup = BeautifulSoup(response, "html.parser")
    #     # try:
    #     x = soup.find('div', {"class": "listaStacji"}).select('.polska > a')
    #     for link in x:
    #         dictColumnValues = {'ChannelName': link.text.strip()}
    #         self.dbInsert(self, 'dimChannel', dictColumnValues)
    #         # except Exception as e:
    #         #     print(e)

    @staticmethod
    def insertHeaders():
        file = open('D:\szkola\HD_WAT\datascraper\Program.txt', 'w', encoding='UTF-8')
        file.write('ProgramName|ageLimit|ProgramDescription|programCategory|timeStart|timeEnd|duration|Date\n')
        file.close()

    # def dbConnection(self):
    #     # decoding password bytes from base64 to string
    #     password = base64.b64decode('dummy').decode("utf-8")
    #     self.connection = pypyodbc.connect('Driver={SQL Server};'
    #                                        'Server=10.22.24.123;'
    #                                        'Database=TV_DW;'
    #                                        'uid=sa;pwd=' + password)

    # def dbInsert(self, tablename, dictColumnValues):
    #     cursor = self.connection.cursor()
    #     tempdict = dictColumnValues
    #     checkIfExistsSQL = 'SELECT COUNT(1) FROM ' + tablename + ' where ' + list(tempdict.keys())[0] + '= \'' + \
    #                        list(tempdict.values())[0] + '\';'
    #
    #     if not cursor.execute(checkIfExistsSQL).fetchone()[0]:
    #         query = 'INSERT INTO ' + tablename + '({0}) VALUES ({1});'
    #         columns = ','.join(tempdict.keys())
    #         placeholders = ','.join(['?'] * len(tempdict))
    #         values = tuple(tempdict.values())
    #         query = query.format(columns, placeholders)
    #         cursor.execute(query, values)
    #         cursor.commit()

    def run(self):
        while self.running:
            self.getPrograms()
            # self.getPrograms()


# HDWAT.getChannels(HDWAT)
HDWAT.insertHeaders()
HDWAT.getChannelLinks(HDWAT)
i = 0
threads = [HDWAT(i) for i in range(0, 10)]
for t in threads:
    # try:
    t.start()
    # except Exception as e:
    #    print(e)
