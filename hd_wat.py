import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import timedelta, date
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import logging


class HDWAT(threading.Thread):
    running = True
    connection = None
    channel_links = set()
    start_date = date(2010, 1, 1)
    end_date = date(2016, 7, 2)
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.19 (KHTML, like Gecko) Ubuntu/12.04 Chromium/18.0.1025.168 Chrome/18.0.1025.168 Safari/535.19'
    FORMAT = "%(asctime)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    fileHandler = logging.FileHandler('D:\szkola\HD_WAT\datascraper\logs.txt', mode='w+', encoding='UTF_8')
    formatter = logging.Formatter(FORMAT)
    fileHandler.setFormatter(formatter)
    logger = logging.getLogger(__name__)
    logger.addHandler(fileHandler)
    logger.propagate = False

    def __init__(self, id, lock):
        self.logger.info('Initializing thread #%s', id)
        self.id = str(id)
        self.lock = lock
        threading.Thread.__init__(self)

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def getPrograms(self):
        if self.channel_links:
            channel = self.channel_links.pop()
            self.logger.debug('Thread #%s: Popped channel=%s', self.id, channel)
            try:
                channelNameURL = str(channel[str(channel).find('http://www.telemagazyn.pl/') + len(
                    'http://www.telemagazyn.pl/'):str(channel).find('/?dzien=')])
            except Exception:
                channelNameURL = ''
            self.logger.debug('Thread #%s: channelNameURL=%s', self.id, channelNameURL)
            try:
                airDate = str(channel[str(channel).find('dzien=') + len('dzien='):])
                airDateTempList = airDate.replace('-', ' ').split()
                year = str(airDateTempList[0])
                month = str(airDateTempList[1])
            except Exception:
                airDate = ''
                year = '2016'
                month = '01'
            self.logger.debug('Thread #%s: airDate=%s', self.id, airDate)
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
                        programName = str(line2.find('span').text.strip()).replace('\r\n', ' ')
                    except Exception:
                        programName = ''
                    self.logger.debug('Thread #%s: programName=%s', self.id, programName)
                    try:
                        programDescription = str(line2.find('p').text.strip()).replace('\r\n', ' ').replace('\r',
                                                                                                            '').replace(
                            '\n', ' ')
                    except Exception:
                        programDescription = ''
                    self.logger.debug('Thread #%s: programDescription=%s', self.id, programDescription)
                    try:

                        timeStart = line2.find('em').text.strip()
                    except Exception:
                        timeStart = '0'
                    self.logger.debug('Thread #%s: timeStart=%s', self.id, timeStart)
                    try:
                        index = x.index(line2)
                        next_value = x[index + 1] if index + 1 < len(x) else None
                        if next_value is not None:
                            timeEnd = next_value.find('em').text.strip()
                        else:
                            timeEnd = '0'
                    except Exception:
                        timeEnd = '0'
                    self.logger.debug('Thread #%s: timeEnd=%s', self.id, timeEnd)
                    try:
                        url2 = "http://www.telemagazyn.pl" + line2.a['href']

                        response2 = urllib.request.urlopen(
                            urllib.request.Request(url2, headers={'User-Agent': self.user_agent}))
                        soup = BeautifulSoup(response2, "html.parser")
                        try:
                            programCategory = soup.find("meta", {"itemprop": "genre"})['content']
                        except Exception:
                            programCategory = '0'
                        self.logger.debug('Thread #%s: programCategory=%s', self.id, programCategory)
                        try:
                            ageLimitTemp = soup.find("h1")['class']
                            ageLimit = ''.join(ageLimitTemp)[4:]
                        except Exception:
                            ageLimit = '0'
                        self.logger.debug('Thread #%s: ageLimit=%s', self.id, ageLimit)
                        try:
                            duration = soup.find("meta", {"itemprop": "timeRequired"})['content']
                        except Exception:
                            duration = '0'
                        self.logger.debug('Thread #%s: duration=%s', self.id, duration)
                        content_to_write = '%s|%s|%s|%s|%s|%s|%s|%s|%s\n' % (
                            programName, ageLimit, programDescription, programCategory, timeStart, timeEnd, duration,
                            airDate, channelNameURL)
                        self.lock.acquire()
                        self.logger.debug('Thread #%s: LOCK ACQUIRED for writing' % self.id)
                        with open('D:\szkola\HD_WAT\datascraper\\%s\Programs_%s.txt' % (year, month), 'a+',
                                  encoding='UTF_8') as file:
                            file.write(content_to_write)
                        self.logger.debug('Thread #%s: %s', self.id, content_to_write)
                        self.lock.release()
                        self.logger.debug('Thread #%s: LOCK RELEASED for writing' % self.id)
                    except Exception as mainException:
                        self.logger.debug('Thread #%s: Exception %s' % mainException)
            except TypeError:
                pass
                # file.close()
        else:
            self.running = False

    @staticmethod
    def getChannelLinks(self):
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
                        self.logger.debug('scheme=%s url=%s', url_test.scheme, url)
                        self.channel_links.add(url)
        except Exception:
            pass

    @staticmethod
    def getChannels(self):
        file = open('D:\szkola\HD_WAT\datascraper\Channels.txt', 'w', encoding='UTF_8')
        file.write(
            'channelName|channelNameURL\n')
        url2 = "http://www.telemagazyn.pl/stacje/"
        response = urllib.request.urlopen(urllib.request.Request(url2, headers={'User-Agent': self.user_agent}))
        soup = BeautifulSoup(response, "html.parser")
        try:
            x = soup.find('div', {"class": "listaStacji"}).select('.polska > a')
            for link in x:
                content = str(link.text.strip() + '|' + link['href'].replace('/', '') + '\n')
                file.write(content)
                self.logger.debug('content=%s', content)
        except Exception:
            pass
        file.close()

    @staticmethod
    def insertHeaders():
        for year in range(2010, 2016 + 1):
            for month in range(1, 12 + 1):
                if month < 10:
                    filename = 'Programs_0'
                else:
                    filename = 'Programs_'
                with open('D:\szkola\HD_WAT\datascraper\\%s\%s%s.txt' % (year, filename, month), 'w',
                          encoding='UTF_8') as file:
                    file.write(
                        'ProgramName|ageLimit|ProgramDescription|programCategory|timeStart|timeEnd|duration|Date|channelNameURL\n')

    def run(self):
        while self.running:
            self.getPrograms()


# HDWAT.getChannels(HDWAT)
HDWAT.insertHeaders()
HDWAT.getChannelLinks(HDWAT)
i = 0
lock = threading.Lock()
threads = [HDWAT(id=i, lock=lock) for i in range(0, 30)]
for t in threads:
    try:
        t.start()
    except Exception as e:
        print(e)
