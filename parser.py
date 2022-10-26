import logging
import requests
import bs4
import pandas as pd
import datetime
import lxml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('cian')

start_link = 'https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=1&room'


# Links
def create_link(link, rooms_start=1, rooms_end=2):
    links = []
    for add_link in range(rooms_start, rooms_end+1, 1):
        links.append(f'{link}{add_link}=1&p=')
    return links


class Parser:

    def __init__(self):
        self.data = None
        self.count = None
        self.urls = None
        self.page = None
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
            'Accept-Language': 'ru',
        }
        self.result = []

    def load_page(self, pages: str):
        url = pages
        res = self.session.get(url=url)
        res.raise_for_status()
        return res.text

    def parse_urls(self, text: str):
        soup = bs4.BeautifulSoup(text, 'lxml')
        try:
            container = soup.find_all('a', {'class': '_93444fe79c--link--eoxce'})
            self.count = []
            for block in container:
                self.count.append(block.get("href"))
                self.urls.append(block.get('href'))
        except Exception:
            logger.info(f'Error: in search')

        return self.urls

    def parse_block(self, text: str, url: str, count: int):

        soup_block = bs4.BeautifulSoup(text, 'lxml')
        # PRICE
        block_price = float('nan')
        try:
            block_price = soup_block.find('span', {'class': 'a10a3f92e9--price_value--lqIK0'})
            block_price = int(block_price.select('span')[0].text.replace('₽', '').replace(' ', ''))
        except Exception:
            logger.info(f'Error: NaN price {url}')

        # ADDRESS
        try:
            address = []
            block_address = soup_block.find('address', {'class': 'a10a3f92e9--address--F06X3'})
            block_address = block_address.select('a')
            for block in block_address:
                address.append(block.text)
            address = ' '.join(address)
        except Exception:
            logger.info(f'Error: NaN address {url}')

        # METRO
        block_metro = float('nan')
        try:
            block_metro = soup_block.find('a', {'class': 'a10a3f92e9--underground_link--Sxo7K'}).text
        except Exception:
            logger.info(f'Error: NaN metro')

        # METRO TIME
        metro_time = float('nan')
        try:
            block_metro_time = soup_block.find('span', {'class': 'a10a3f92e9--underground_time--iOoHy'}).text
            metro_time = block_metro_time.replace(' ⋅  ', '')
        except Exception:
            logger.info(f'Error NaN metro time {url}')

        # INFO
        try:
            info_blocks = soup_block.find_all('div', {'class': 'a10a3f92e9--info-value--bm3DC'})
        except Exception:
            logger.info(f'Error: NaN info {url}')

        # AREA
        area = float('nan')
        try:
            area_block = info_blocks[0].text
            area = area_block.replace(' м²', '').replace(',', '.')
            area = float(area)
        except Exception:
            logger.info(f'Error: NaN area {url}')

        # FLOOR
        floor = float('nan')
        floors = float('nan')
        try:
            for info_block_floor in info_blocks:
                if info_block_floor.text.find('из') >= 0:
                    floor_block = info_block_floor.text.split(' ')
            floor = int(floor_block[0])
            floors = int(floor_block[2])
        except Exception:
            logger.info(f'Error: NaN info of floor {url}')

        # BUILDING TIME
        building_time = 'сдан'
        try:
            for info_block_building in info_blocks:
                if info_block_building.text.find('кв') >= 0:
                    building_time = info_block_building.text
        except Exception:
            logger.info(f'Error: NaN building time {url}')

        # PRICE OF METROS
        price_of_metros = float('nan')
        try:
            price_of_metros_block = soup_block.find('span', {
                'class': 'a10a3f92e9--color_gray60_100--MlpSF a10a3f92e9--lineHeight_5u--cJ35s a10a3f92e9--fontWeight_normal--P9Ylg a10a3f92e9--fontSize_14px--TCfeJ a10a3f92e9--display_block--pDAEx a10a3f92e9--text--g9xAG a10a3f92e9--text_letterSpacing__0--mdnqq a10a3f92e9--text_whiteSpace__nowrap--Akbtc'}).text
            price_of_metros = int(price_of_metros_block.replace('₽/м²', '').replace(' ', ''))
        except Exception:
            logger.info(f'Error: NaN price of metros {url}')

        # DATA
        try:
            data_block = soup_block.find_all('td', {'class': 'a10a3f92e9--event-date--BvijC'})[-1].text
        except Exception:
            data_block = float('nan')

        # OBJECT INFO
        try:
            object_info_block = soup_block.find_all('span', {'class': 'a10a3f92e9--name--x7_lt'})
            object_block = soup_block.find_all('span', {'class': 'a10a3f92e9--value--Y34zN'})
            objects = []
            specifications = []

            for params in object_info_block:
                objects.append(params.text)
                objects_str = '$'.join(objects)

            for specification in object_block:
                specifications.append(specification.text)
                specification_str = '$'.join(specifications)

        except Exception:
            objects_str = float('nan')
            specification_str = float('nan')

        new_row = {
            'Адресс': address,
            'Площадь': area,
            'Цена': block_price,
            'Цена за метр': price_of_metros,
            'Метро': block_metro,
            'Время до метро': metro_time,
            'Этаж': floor,
            'Всего этажей': floors,
            'Окончание стройки': building_time,
            'Url': url,
            'Комнаты': count,
            'Дата': data_block,
            'Общая информация характеристики': objects_str,
            'Общая информация параметры': specification_str
        }
        return pd.DataFrame([new_row])

    def run(self, page_count: int, pages: str, count_room: int):

        headers = [
            'Адресс',
            'Площадь',
            'Цена',
            'Цена за метр',
            'Метро',
            'Время до метро',
            'Этаж',
            'Всего этажей',
            'Окончание стройки',
            'Url',
            'Комнаты',
            'Дата',
            'Общая информация характеристики',
            'Общая информация параметры'
        ]

        self.data = pd.DataFrame(columns=headers)
        self.urls = []

        for count in range(1, page_count + 1):
            self.page = pages + str(count)
            text = self.load_page(self.page)
            self.parse_urls(text=text)

        logger.info(f'Всего объявлений (комнат - {count_room}): {len(self.urls)}\n____________________')

        for url in self.urls:
            text = self.load_page(url)
            self.data = pd.concat([self.data, self.parse_block(text=text, url=url, count=count_room)], ignore_index=True)

        self.data.to_csv(f'datas/data_{count_room}.csv', index=False)
        logger.info(f'____________________\nTable create. \nTables omissions: \n{self.data.isna().sum()}')
        logger.info(f'END\n{datetime.datetime.now()}')

        pass


if __name__ == '__main__':
    pages = create_link(start_link, 1, 4)
    parser_urls = Parser()
    for room in range(len(pages)):
        parser_urls.run(1, pages[room], (room + 1))
