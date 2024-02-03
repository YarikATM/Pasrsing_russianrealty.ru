import datetime
from bs4 import BeautifulSoup
import json
import requests
import logging
import asyncio
import aiohttp
import time
import re
from fake_useragent import UserAgent
import os.path

ua = UserAgent()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/118.0.0.0 YaBrowser/23.11.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
              "application/signed-exchange;v=b3;q=0.7"
}


async def load_page(urls: list):
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ttl_dns_cache=300)) as session:
            tasks = []
            for url in urls:
                task = asyncio.create_task(get_page(session, url))
                tasks.append(task)

            return await asyncio.gather(*tasks)

    except Exception as e:
        logging.error(" | " + str(e))


async def get_page(session: aiohttp.ClientSession, url):
    HEADERS["User-Agent"] = ua.random

    try:
        async with session.get(url=url, headers=HEADERS, ssl=False) as response:

            data = await response.text()
            assert response.status == 200

            return BeautifulSoup(data, "lxml")

    except Exception as e:
        logging.error(str(e))


def json_save(path, data):
    with open(path, "w", encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def json_read(path):
    with open(path, "r", encoding='utf-8') as f:
        return json.load(f)


def get_pagination() -> int:
    """
    :return: Количество страниц
    """
    req = requests.get(
        url="https://ekaterinburg.russianrealty.ru/Продажа-вторичных-квартир/?c=1&r=67&l=905&a=100&p=100&n=0&f=1&z"
            "=DATEADD&zt=DESChttps://ekaterinburg.russianrealty.ru/Продажа-вторичных-квартир/?c=1&r=67&l=905&a=100&p"
            "=100&n=0&f=1&z=DATEADD&zt=DESC",
    )

    soup = BeautifulSoup(req.text, "lxml")

    pagination = int(soup.findAll(class_="max left")[-1].text)
    logging.info(f"Найдено {pagination} страниц")
    return pagination


def get_apart_urls(page: BeautifulSoup):
    row_urls = page.find(id="catalog_list").findAll(class_="hproduct")
    urls = [f'https:{row_url.find("a")["href"]}' for row_url in row_urls]

    return urls


def get_apart_data(soup):
    try:
        obj = {}

        # Актуальность обьявления
        available = True
        try:
            available_soup = soup.find(class_="article-body").find("h3")
            if available_soup is not None:
                if available_soup.text == soup.find(class_="article-body").find("h3").text:
                    available = False
        except Exception as e:
            print(str(e))


        # Дата
        date = {}
        create_date = None
        update_date = None
        try:
            date_block = soup.find(class_="item-status").findAll("div")
            for i in date_block:
                label = i.text.strip().split(": ")
                match label[0]:
                    case "Добавлено":
                        create_date = str(datetime.datetime.strptime(label[1], "%d.%m.%Y")).replace(" ", "T") + "Z"
                    case "Обновлено":
                        update_date = str(datetime.datetime.strptime(label[1], "%d.%m.%Y")).replace(" ", "T") + "Z"

        except Exception as e:
            logging.warning(f"Не удалось получить дату | {str(e)}")

        date["create_date"] = create_date
        date["update_date"] = update_date
        date["available"] = available


        # Цена
        price = None
        try:
            price = soup.find(class_="price-total").text
            price = int(price.replace(" руб.", "").replace(" ", ''))
        except Exception as e:
            logging.warning(f"Не удалось получить цену | {str(e)}")

        obj['price'] = price


        # Ссылка
        url = None
        try:
            for link in soup.findAll("link"):
                if link["rel"][0] == "canonical":
                    url = f'https:{link["href"]}'
        except Exception as e:
            logging.warning(f"Не удалось получить ссылку | {str(e)}")

        obj['url'] = url


        # Контактная информация
        contact_information = {}
        contact = None
        company = None
        phones = None
        try:
            if available:
                contact_card = soup.find(class_="list-contact vcard").findAll("div")

                for mc in contact_card:
                    label = mc.find("label").text
                    match label:
                        case "Контакт:":
                            contact = mc.find("strong").text
                        case "Компания:":
                            company = mc.find("strong").text
                        case "Телефоны:":
                            row_phones = mc.find("script").text.split("html('")[1].split("<p>")[0].split("</a>")[:-1]
                            phones = [phone.split('href="tel:')[1].split('"')[0] for phone in row_phones]
                        case "Телефон:":
                            row_phones = mc.find("script").text.split("html('")[1].split("<p>")[0].split("</a>")[:-1]
                            phones = [phone.split('href="tel:')[1].split('"')[0] for phone in row_phones]

        except Exception as e:
            logging.warning(f"Не удалось получить контактную информацию | {str(e)}")

        contact_information["phone"] = phones
        contact_information["contact"] = contact
        contact_information["company"] = company


        # Расположение
        location = {}

        # Адрес
        try:
            address = soup.find(class_="street-address").text.replace("Адрес: ", "").split(", ")
            match len(address):
                case 2:
                    location["region"] = address[0]
                    location["city"] = address[1]
                case 3:
                    location["region"] = address[0]
                    location["city"] = address[1]
                    location["street"] = address[2]
                case 4:
                    location["region"] = address[0]
                    location["city"] = address[1]
                    location["street"] = address[2]
                    location["building_number"] = address[3]
                case 5:
                    location["region"] = address[0]
                    location["city"] = address[1]
                    location["district"] = address[2]
                    location["street"] = address[3]
                    location["building_number"] = address[4]
                case 6:
                    location["region"] = address[0]
                    location["city"] = address[1]
                    location["district"] = address[2]
                    location["microdistrict"] = address[3]
                    location["street"] = address[4]
                    location["building_number"] = address[5]
                case 7:
                    location["region"] = address[0]
                    location["city"] = address[2]
                    location["district"] = address[3]
                    location["street"] = address[5]
                    location["building_number"] = address[6]

                case _:
                    logging.warning(address)
        except Exception as e:
            logging.warning(f"Не удалось получить адрес | {str(e)}")

        # Координаты
        coordinates = None
        try:
            coordinates = soup.find(class_="article-notice").findAll("script")[-1].text \
                .strip().split("coords = ")[1].split(",\n")[0]
            if coordinates != 'false':
                coordinates = coordinates[1:-1].split(",")
                coordinates = [float(cord) for cord in coordinates][::-1]
        except Exception as e:
            logging.warning(f"Не удалось получить координаты | {str(e)}")

        location["coordinates"] = coordinates


        # Параметры квартиры
        apartment_parameters = {}
        apart_type = None
        try:
            apart_type = soup.find(class_="catalog-card").find("header").find('h1').text.split(" ")[1].replace("ой",
                                                                                                               "ая")
        except Exception as e:
            logging.warning(f"Не удалось получить параметры квартиры | {str(e)}")

        apartment_parameters["apart_type"] = apart_type

        # Площадь квартиры
        total_area = None
        living_area = None
        kitchen_area = None
        data_block = None
        try:
            data_block = soup.find(class_="col-lg-4 col-md-6 col-sm-12 desc-list")

            area = data_block.find(class_="item-space")
            area_titles = area.find("thead").findAll("th")
            area_values = area.find("tbody").findAll("td")

            for index, title in enumerate(area_titles):
                value = float(area_values[index].text.replace(" м²", ""))
                match title.text:
                    case "Общая":
                        total_area = value
                    case "Жилая":
                        living_area = value
                    case "Кухня":
                        kitchen_area = value

        except Exception as e:
            logging.warning(f"Не удалось получить площадь | {str(e)}")

        apartment_parameters["total_area"] = total_area
        apartment_parameters["living_area"] = living_area
        apartment_parameters["kitchen_area"] = kitchen_area


        # Этажность
        floor = None
        floors = None
        sale_status = None
        try:
            # print(data_block.find("ul"))
            params = data_block.find("ul").findAll("li")

            for param in params:
                if "этаж" in param.text:
                    floor = int(param.text.split(" этаж")[0])
                    if param.text.split(" этаж")[1] != '':
                        floors = int(param.text.split(" этаж")[1].replace("-этажного дома", '').replace(" ", ''))
                if "Статус продажи:" in param.text:
                    sale_status = param.text.split("Статус продажи: ")[1].strip()

        except Exception as e:
            logging.warning(f"Не удалось получить этажность' | {str(e)}")

        apartment_parameters["floor"] = floor
        apartment_parameters["floors"] = floors
        apartment_parameters["sale_status"] = sale_status


        # Описание
        description = None
        try:
            description = soup.find(class_="item-desc").text.replace("Распечатать\n", "").replace("Описание\n", "") \
                .replace("\n\n\n", "").replace("                                        ", '') \
                .replace("\n                        ", "")

        except Exception as e:
            logging.warning(f"Не удалось получить описание | {str(e)}")

        apartment_parameters["description"] = description

        # ID
        ID = None
        try:
            ID = int(url.split("kvartiry-")[1].split("-")[0])
        except Exception as e:
            logging.warning(f"Не удалось получить ID | {str(e)}")

        obj["ID"] = ID


        # Микрорайон
        microdistrict = None
        try:
            if "микpорaйoн" in description:
                microdistrict = description.replace("\n", "").strip().split("br /")[0].split("микpорaйoнa ")[-1][:-1]
                location["microdistrict"] = microdistrict
        except Exception as e:
            logging.warning(f"Не удалось получить район | {str(e)}")

        location["microdistrict"] = microdistrict


        # Фотографии
        photos = None
        try:
            if soup.find(class_="gallery-slider") is not None:
                photos_soup = soup.find(class_="gallery-slider").findAll("a")
                photos = [f"https:{photo['href']}" for photo in photos_soup]
        except Exception as e:
            logging.warning(f"Не удалось получить фотографии | {str(e)}")

        apartment_parameters["photos"] = photos



        obj["date"] = date
        obj["location"] = location
        obj["contact_information"] = contact_information
        obj["apartment_parameters"] = apartment_parameters
        return obj


    except Exception as e:
        logging.error(f"Произошла ошибка при парсинге страницы квартиры | {str(e)}")


def get_all_data(pagination: int):
    for page_num in range(1, pagination + 1):
        if os.path.isfile(f"json/{page_num}_page.json"):
            finded_data = json_read(f"json/{page_num}_page.json")
            if len(finded_data) == 20:
                logging.info(f"Найдена {page_num} страница!")
                continue
        start_time = time.time()
        page_res = []

        page_url = f"https://ekaterinburg.russianrealty.ru/Продажа-вторичных-квартир/{page_num}" \
                   f"/?c=1&r=67&l=905&a=100&p=100&n=0&f=1&z=DATEADD&zt=DESC&_p={page_num}"
        if page_num == 1:
            page_url = "https://ekaterinburg.russianrealty.ru/Продажа-вторичных-квартир" \
                       "/?c=1&r=67&l=905&a=100&p=100&n=0&f=1&z=DATEADD&zt=DESC"

        req = requests.get(
            url=page_url
        )

        page_soup = BeautifulSoup(req.text, 'lxml')

        apart_urls = get_apart_urls(page_soup)

        aparts_soups = asyncio.run(load_page(apart_urls))

        for soup in aparts_soups:
            data = get_apart_data(soup)
            if data is not None:
                page_res.append(data)

        json_save(f"json/{page_num}_page.json", page_res)
        logging.info(f"Page {page_num} was parsed successfully, time taken: {time.time() - start_time}")


def normalize(pagination):
    res = []
    cnt = 0
    for i in range(1, pagination + 1):
        data = json_read(f"json/{i}_page.json")
        for val in data:
            if "photos" in val["apartment_parameters"].keys():
                if val["apartment_parameters"]["photos"] is not None:
                    cnt += 1
                    res.append(val)
    logging.info(f"Найдено {cnt} объявлений")
    json_save("result.json", res)


def main():
    logging.basicConfig(level=logging.INFO, filemode="a",
                        format="%(asctime)s %(levelname)s %(message)s")

    if not os.path.isdir("json"):
        os.mkdir("json")

    pagination = get_pagination()

    get_all_data(pagination)

    normalize(pagination)


def test():
    r = requests.get(
        url="https://ekaterinburg.russianrealty.ru/prodazha-kvartiry-418469505-4-komnatnaya-Ekaterinburg-ulitsa-Hohryakova-Geologicheskaya/"
    )

    soup = BeautifulSoup(r.text, "lxml")

    get_apart_data(soup)


if __name__ == '__main__':
    main()
    # test()
