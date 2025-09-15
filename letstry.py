import json
import requests
import pandas as pd
import logging
from datetime import datetime
import re
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

# Настройка логирования
log_file = f"price_update_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)
logger = logging.getLogger()

# Конфигурация API
API_URL = "https://api2.autotrade.su/?json"
AUTH_KEY = "a7cdc008ab34eb358c019211f613c706"
BATCH_SIZE = 60
RETRY_COUNT = 5
RETRY_DELAY = 10
CHECKPOINT_FILE = "checkpoint.json"

# Конфигурация SMTP
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "autotradeprice@gmail.com"
SMTP_PASSWORD = "lbph rwdh ketm daei"
EMAIL_RECIPIENTS = ["delevinrero@gmail.com", "rdelev@mail.ru"]

# Группы складов для определения статуса
GROUP1 = [
    "Н(Т)", "Н(Р)", "Н(Б)", "Н(Д)", "Н(ДУ)", "Н(ГП)", "Н(ПП)", "То(Б)", "Б(П)", "П(Т)",
    "Бе(Л)", "Ке(К)", "ЛК(К)", "Ке(М)", "Ке(П)", "Ба(П)", "Ба(Поп)", "Бий(К)", "Бий(М)", "ГА(К)"
]
GROUP2 = [
    "Аб(И)", "К(О)", "К(Г)", "К(М)", "К(Ш)", "К(С)", "К(Кр)", "Е(Л)"
]

# Словарь соответствия полных названий складов и коротких обозначений
STORAGE_MAPPING = {
    "Новосибирск (Троллейная)": "Н(Т)",
    "Новосибирск (Рудокопровая)": "Н(Р)",
    "Новосибирск (Большая)": "Н(Б)",
    "Новосибирск (Дунаевского)": "Н(Д)",
    "Новосибирск (Дуси Ковальчук)": "Н(ДУ)",
    "Новосибирск (Гусинобродское шоссе)": "Н(ГП)",
    "Новосибирск (Петухова)": "Н(ПП)",
    "Томск (Балтийская)": "То(Б)",
    "Барнаул (Покровская)": "Б(П)",
    "Пермь (Танкистов)": "П(Т)",
    "Белгород (Луговая)": "Бе(Л)",
    "Кемерово (Кузнецкий)": "Ке(К)",
    "Липецк (Катукова)": "ЛК(К)",
    "Кемерово (Мартемьянова)": "Ке(М)",
    "Кемерово (Проездная)": "Ке(П)",
    "Барнаул (Попова)": "Ба(Поп)",
    "Барнаул (Павловский тракт)": "Ба(П)",
    "Бийск (Кожзаводская)": "Бий(К)",
    "Бийск (Митрофанова)": "Бий(М)",
    "Горно-Алтайск (Коммунистический)": "ГА(К)",
    "Абакан (Игарская)": "Аб(И)",
    "Красноярск (Одесская)": "К(О)",
    "Красноярск (Грунтовая)": "К(Г)",
    "Красноярск (Металлургов)": "К(М)",
    "Красноярск (Шахтеров)": "К(Ш)",
    "Красноярск (Северное шоссе)": "К(С)",
    "Красноярск (Красноярск)": "К(Кр)",
    "Екатеринбург (Лукиных)": "Е(Л)",
    "Иркутск (Ракитная)": "И(Р)",
    "Улан-Удэ (пр-т Автомобилистов)": "УУ(А)",
    "Иркутск (Автоград)": "И(А)",
    "Чита (Ленина)": "Ч(Л)",
    "Владивосток (Кубанская)": "В(К)",
    "Владивосток (Камская)": "В(П)",
    "Братск (Коммунальная)": "Б(К)",
    "Благовещенск (Театральная)": "Б(Т)",
    "Рязань (Лермонтова)": "Р(Л)",
    "Москва (Апаринки)": "М(А)",
    "Ростов-на-Дону (Металлургическая)": "РнД(М)",
    "Ростов-на-Дону (Доватора)": "РнД(Д)",
    "Находка (Вторая)": "На(В)",
    "Сургут (Рационализаторов)": "Су(Р)",
    "Уссурийск (Чичерина)": "Ус(Б)",
    "Иркутск (Академическая)": "И(Ак)",
    "Краснодар (Метальникова)": "Крд (М)",
    "Тюмень (Дружбы)": "Тю (Д)",
    "Нижний Новгород (Ларина)": "НН(Л)",
    "Артем (Вокзальная)": "Ар(В)",
    "Воронеж (Конструкторов)": "В(К)",
    "Новокузнецк (Рудокопровая)": "Н(Р)"
}


# --- Часть 1: Парсинг API ---

def call_api(method, params):
    logger.info(f"Вызов API: метод={method}")
    payload = {"data": json.dumps({"auth_key": AUTH_KEY, "method": method, **params})}
    logger.info(f"Отправляемый запрос: {payload}")
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.post(
                API_URL,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            logger.info(f"Получен ответ API: {json.dumps(data, ensure_ascii=False, indent=2)}")
            if "code" in data and data["code"] != 0:
                logger.error(f"Ошибка API: code={data['code']}, message={data['message']}")
                raise Exception(f"Ошибка API: {data['message']}")
            return data
        except requests.RequestException as e:
            logger.error(f"Ошибка HTTP-запроса (попытка {attempt + 1}/{RETRY_COUNT}): {str(e)}")
            if attempt < RETRY_COUNT - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Не удалось выполнить запрос после {RETRY_COUNT} попыток")
    raise Exception(f"Не удалось выполнить запрос после {RETRY_COUNT} попыток")


def get_storages():
    try:
        data = call_api("getStoragesList", {})
        storage_ids = []
        for storage in data.values():
            storage_id = storage.get("id")
            if storage.get("for_realization", 0) == 1 or storage.get("for_delivery", 0) == 1:
                storage_ids.append(str(storage_id))
        logger.info(f"Получено {len(storage_ids)} складов: {storage_ids}")
        return storage_ids
    except Exception as e:
        logger.error(f"Ошибка получения складов: {str(e)}")
        raise


def clean_price(price):
    if isinstance(price, str):
        price = re.sub(r"[^\d.]", "", price)
        try:
            return float(price)
        except ValueError:
            return 0.0
    return float(price) if price else 0.0


def calculate_markup(price):
    p = price
    if p < 100:
        return 25
    elif p < 200:
        return 50
    elif p < 300:
        return 75
    elif p < 500:
        return 100
    elif p < 700:
        return 150
    elif p < 900:
        return 200
    elif p < 1100:
        return 300
    elif p < 1500:
        return 400
    elif p < 2000:
        return 450
    elif p < 3000:
        return 500
    elif p < 4500:
        return 650
    elif p < 6000:
        return 750
    elif p < 8000:
        return 800
    elif p < 10000:
        return 900
    elif p < 12000:
        return 1100
    elif p < 15000:
        return 1300
    elif p < 18000:
        return 1500
    elif p < 25000:
        return 2500
    elif p < 35000:
        return 3000
    elif p < 45000:
        return 4500
    elif p < 55000:
        return 6000
    elif p < 65000:
        return 7000
    elif p < 100000:
        return 8000
    elif p < 150000:
        return 10000
    elif p < 300000:
        return 15000
    elif p < 800000:
        return 50000
    else:
        return 50000


def determine_status_api(item_data):
    if not item_data:
        return "Под заказ 14-21 дней"
    stocks = item_data.get("stocks", {})
    has_group1 = any(
        stock.get("quantity_unpacked", 0) > 0 and stock.get("name", "") in GROUP1
        for stock in stocks.values()
    )
    if has_group1:
        return "В наличии"
    has_group2 = any(
        stock.get("quantity_unpacked", 0) > 0 and stock.get("name", "") in GROUP2
        for stock in stocks.values()
    )
    if has_group2:
        return "Под заказ 2-5 дней"
    has_any = any(stock.get("quantity_unpacked", 0) > 0 for stock in stocks.values())
    if has_any:
        return "Под заказ 7-14 дней"
    else:
        return "Под заказ 14-21 дней"


def save_checkpoint(last_batch_index, results):
    checkpoint = {
        "last_batch_index": last_batch_index,
        "results": results
    }
    try:
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранено состояние: обработано до индекса {last_batch_index}")
    except Exception as e:
        logger.error(f"Ошибка сохранения чекпоинта: {str(e)}")
        raise


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                checkpoint = json.load(f)
            logger.info(f"Загружено состояние: обработано до индекса {checkpoint['last_batch_index']}")
            return checkpoint["last_batch_index"], checkpoint["results"]
        except Exception as e:
            logger.error(f"Ошибка загрузки чекпоинта: {str(e)}")
            return 0, []
    return 0, []


def process_items(items, storage_ids, total_rows):
    start_index, results = load_checkpoint()
    total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(start_index, len(items), BATCH_SIZE):
        batch = items[i:i + BATCH_SIZE]
        items_dict = {row["Артикул"]: {row["Бренд"]: 1} for _, row in batch.iterrows()}
        success = False
        for attempt in range(RETRY_COUNT):
            try:
                response = call_api("getStocksAndPrices", {
                    "params": {
                        "storages": storage_ids,
                        "items": items_dict,
                        "withDelivery": 1,
                        "checkTransit": 1
                    }
                })
                api_items = response.get("items", {})
                success = True
                break
            except Exception as e:
                logger.error(
                    f"Ошибка обработки партии {i}-{i + BATCH_SIZE}, попытка {attempt + 1}/{RETRY_COUNT}: {str(e)}")
                if attempt < RETRY_COUNT - 1:
                    time.sleep(RETRY_DELAY)
        for _, row in batch.iterrows():
            article = row["Артикул"]
            brand = row["Бренд"]
            old_price = clean_price(row["Цена"])
            old_status = row["Статус"]
            if not success or article not in api_items:
                logger.warning(f"Нет данных API для артикула {article} ({brand})")
                results.append({
                    "Артикул": article,
                    "Бренд": brand,
                    "Старая цена": old_price,
                    "Новая цена": old_price,  # Сохраняем старую цену
                    "Старый статус": old_status,
                    "Новый статус": "Под заказ 14-21 дней",
                    "Изменение цены (%)": 0,
                    "Склад с наличием": "N/A",
                    "Изменение": "Ошибка"
                })
            else:
                item_data = api_items[article]
                wholesale_price = clean_price(item_data.get("price", 0))
                new_price = wholesale_price + calculate_markup(wholesale_price)
                new_status = determine_status_api(item_data)
                price_change = (new_price - old_price) / old_price * 100 if old_price > 0 else 0
                change_detected = abs(price_change) >= 10 or old_status != new_status
                stock_info = ", ".join([
                    f"{stock.get('name')} ({stock.get('quantity_unpacked')})"
                    for stock in item_data.get("stocks", {}).values()
                    if stock.get("quantity_unpacked", 0) > 0
                ]) if item_data.get("stocks") else "N/A"
                results.append({
                    "Артикул": article,
                    "Бренд": brand,
                    "Старая цена": old_price,
                    "Новая цена": new_price,
                    "Старый статус": old_status,
                    "Новый статус": new_status,
                    "Изменение цены (%)": round(price_change, 2) if old_price > 0 else "N/A",
                    "Склад с наличием": stock_info,
                    "Изменение": "Да" if change_detected else "Нет"
                })

        processed_rows = min(i + BATCH_SIZE, len(items))
        logger.info(f"Обработано {processed_rows}/{total_rows} строк ({(processed_rows / total_rows) * 100:.2f}%)")
        print(f"Обработано {processed_rows}/{total_rows} строк ({(processed_rows / total_rows) * 100:.2f}%)")
        save_checkpoint(i + BATCH_SIZE, results)

    logger.info(f"Количество строк в changes_report после парсинга: {len(results)}")
    return results


# --- Часть 2: Обновление прайса и отправка ---

def determine_status_from_stock_info(stock_info):
    if pd.isna(stock_info) or stock_info == "N/A" or not stock_info:
        return "Под заказ 14-21 дней"

    stocks = {}
    items = stock_info.split(", ")
    for item in items:
        match = re.match(r"(.+) $$   ([0-9]+)   $$", item.strip())
        if match:
            name = match.group(1).strip()
            qty = int(match.group(2))
            stocks[name] = qty

    has_group1 = any(
        qty > 0 and STORAGE_MAPPING.get(name, "") in GROUP1
        for name, qty in stocks.items()
    )
    if has_group1:
        return "В наличии"

    has_group2 = any(
        qty > 0 and STORAGE_MAPPING.get(name, "") in GROUP2
        for name, qty in stocks.items()
    )
    if has_group2:
        return "Под заказ 2-5 дней"

    has_any = any(qty > 0 for qty in stocks.values())
    if has_any:
        return "Под заказ 7-14 дней"
    else:
        return "Под заказ 14-21 дней"


def process_changes_report(changes_file):
    logger.info(f"Чтение файла изменений: {changes_file}")
    try:
        changes_df = pd.read_excel(changes_file)
        logger.info(f"Количество строк в changes_report до dedup: {len(changes_df)}")
        changes_df = changes_df.drop_duplicates(subset=["Артикул", "Бренд"])
        logger.info(f"Загружено {len(changes_df)} уникальных строк из {changes_file}")
    except Exception as e:
        logger.error(f"Ошибка чтения {changes_file}: {str(e)}")
        print(f"Ошибка чтения {changes_file}: {str(e)}")
        return None

    changes_df["Новый статус"] = changes_df["Склад с наличием"].apply(determine_status_from_stock_info)

    for idx, row in changes_df.iterrows():
        if row["Новая цена"] == 0 or row["Новая цена"] == 25:
            changes_df.at[idx, "Новая цена"] = row["Старая цена"]
            changes_df.at[idx, "Новый статус"] = "Под заказ 14-21 дней"
            logger.info(
                f"Артикул {row['Артикул']}: Цена изменена на старую ({row['Старая цена']}), статус на 'Под заказ 14-21 дней'")

    return changes_df


def update_output(original_file, changes_file):
    logger.info(f"Чтение исходного файла: {original_file}")
    try:
        output_df = pd.read_excel(original_file)
        logger.info(f"Количество строк в output.xlsx: {len(output_df)}")
    except Exception as e:
        logger.error(f"Ошибка чтения {original_file}: {str(e)}")
        print(f"Ошибка чтения {original_file}: {str(e)}")
        return None

    changes_df = process_changes_report(changes_file)
    if changes_df is None:
        logger.error("Не удалось обработать changes_report, возвращаем output без изменений")
        return output_df

    output_df = output_df.merge(
        changes_df[["Артикул", "Бренд", "Новый статус", "Новая цена"]],
        on=["Артикул", "Бренд"],
        how="left"
    )

    logger.info(f"Количество строк в output_df после merge: {len(output_df)}")

    updated_count = 0
    for idx, row in output_df.iterrows():
        if pd.notna(row["Новый статус"]) and pd.notna(row["Новая цена"]):
            output_df.at[idx, "Статус"] = row["Новый статус"]
            output_df.at[idx, "Цена"] = row["Новая цена"]
            logger.info(
                f"Обновлен артикул {row['Артикул']}: Новый статус={row['Новый статус']}, Цена={row['Новая цена']}")
            updated_count += 1
        else:
            logger.warning(f"Артикул {row['Артикул']} ({row['Бренд']}) не обновлен: нет данных в changes_report")

    output_df = output_df.drop(columns=["Новый статус", "Новая цена"], errors="ignore")
    logger.info(f"Обновлено {updated_count} строк, итоговое количество строк: {len(output_df)}")

    return output_df


def send_email(file_path):
    logger.info(f"Отправка файла {file_path} на {', '.join(EMAIL_RECIPIENTS)}")
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(EMAIL_RECIPIENTS)
    msg["Subject"] = f"Итоговый прайс {datetime.now().strftime('%Y-%m-%d')}"

    body = f"В прикреплении итоговый прайс за {datetime.now().strftime('%Y-%m-%d')}."
    msg.attach(MIMEText(body, "plain"))

    try:
        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(file_path)}"
        )
        msg.attach(part)
    except Exception as e:
        logger.error(f"Ошибка прикрепления файла {file_path}: {str(e)}")
        print(f"Ошибка прикрепления файла {file_path}: {str(e)}")
        return False

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, EMAIL_RECIPIENTS, msg.as_string())
        server.quit()
        logger.info(f"Письмо успешно отправлено на {', '.join(EMAIL_RECIPIENTS)}")
        print(f"Письмо успешно отправлено на {', '.join(EMAIL_RECIPIENTS)}")
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки письма: {str(e)}")
        print(f"Ошибка отправки письма: {str(e)}")
        return False


def main():
    logger.info("Программа запущена")
    date = datetime.now().strftime('%Y%m%d')
    changes_file = f"changes_report_{date}.xlsx"
    original_file = "output.xlsx"
    final_file = f"price_{date}.xlsx"

    # Часть 1: Парсинг данных с API
    logger.info("Запуск парсинга данных с API")
    try:
        df = pd.read_excel(original_file)
        logger.info(f"Загружено {len(df)} строк из {original_file}")
        print(f"Загружено {len(df)} строк из {original_file}")
    except Exception as e:
        logger.error(f"Ошибка чтения {original_file}: {str(e)}")
        print(f"Ошибка чтения {original_file}: {str(e)}")
        return

    required_columns = ["Артикул", "Бренд", "Цена", "Статус"]
    if not all(col in df.columns for col in required_columns):
        logger.error(f"В файле {original_file} отсутствуют необходимые колонки: {required_columns}")
        print(f"В файле {original_file} отсутствуют необходимые колонки: {required_columns}")
        return

    try:
        storage_ids = get_storages()
    except Exception as e:
        logger.error(f"Ошибка получения складов: {str(e)}")
        print(f"Ошибка получения складов: {str(e)}")
        return

    try:
        results = process_items(df, storage_ids, len(df))
        report_df = pd.DataFrame(results)
        report_df.to_excel(changes_file, index=False)
        logger.info(f"Отчет сохранен: {changes_file} с {len(report_df)} строками")
        print(f"Отчет сохранен: {changes_file} с {len(report_df)} строками")
    except Exception as e:
        logger.error(f"Ошибка сохранения отчета {changes_file}: {str(e)}")
        print(f"Ошибка сохранения отчета {changes_file}: {str(e)}")
        return

    # Часть 2: Обновление прайса
    logger.info("Запуск обновления прайса")
    updated_df = update_output(original_file, changes_file)
    if updated_df is None:
        logger.error("Обновление прайса не удалось")
        print("Обновление прайса не удалось")
        return

    logger.info(f"Количество строк перед сохранением: {len(updated_df)}")
    try:
        updated_df.to_excel(final_file, index=False)
        logger.info(f"Итоговый файл сохранен: {final_file} с {len(updated_df)} строками")
        print(f"Итоговый файл сохранен: {final_file} с {len(updated_df)} строками")
    except Exception as e:
        logger.error(f"Ошибка сохранения {final_file}: {str(e)}")
        print(f"Ошибка сохранения {final_file}: {str(e)}")
        return

    # Удаление чекпоинта только после успешного создания обоих файлов
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            logger.info(f"Файл чекпоинта {CHECKPOINT_FILE} удален")
        except Exception as e:
            logger.error(f"Ошибка удаления чекпоинта {CHECKPOINT_FILE}: {str(e)}")
            print(f"Ошибка удаления чекпоинта {CHECKPOINT_FILE}: {str(e)}")

    # Часть 3: Отправка письма
    if os.path.exists(final_file):
        send_email(final_file)
    else:
        logger.error(f"Файл {final_file} не существует")
        print(f"Файл {final_file} не существует")


if __name__ == "__main__":
    main()