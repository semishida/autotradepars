import json
import requests
import pandas as pd
import logging
from datetime import datetime
import re
import time
import os

# Конфигурация API
API_URL = "https://api2.autotrade.su/?json"
AUTH_KEY = "a7cdc008ab34eb358c019211f613c706"
BATCH_SIZE = 60  # Максимум 60 артикулов за запрос
RETRY_COUNT = 3  # Количество попыток при ошибке
RETRY_DELAY = 5  # Задержка между попытками в секундах
CHECKPOINT_FILE = "checkpoint.json"

# Настройка логирования
log_file = f"api_process_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)
logger = logging.getLogger()

# Группы складов для определения статуса
GROUP1 = [
    "Н(Т)", "Н(Р)", "Н(Б)", "Н(Д)", "Н(ДУ)", "Н(ГП)", "Н(ПП)", "То(Б)", "Б(П)", "П(Т)",
    "Бе(Л)", "Ке(К)", "ЛК(К)", "Ке(М)", "Ке(П)", "Ба(П)", "Ба(Поп)", "Бий(К)", "Бий(М)", "ГА(К)"
]
GROUP2 = [
    "Аб(И)", "К(О)", "К(Г)", "К(М)", "К(Ш)", "К(С)", "К(Кр)", "Е(Л)"
]

# Вызов JSON API
def call_api(method, params):
    logger.info(f"Вызов API: метод={method}")
    payload = {"data": json.dumps({"auth_key": AUTH_KEY, "method": method, **params})}
    logger.info(f"Отправляемый запрос: {payload}")
    try:
        response = requests.post(
            API_URL,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        )
        response.raise_for_status()
        data = response.json()
        logger.info(f"Получен ответ API: {json.dumps(data, ensure_ascii=False, indent=2)}")
        if "code" in data and data["code"] != 0:
            logger.error(f"Ошибка API: code={data['code']}, message={data['message']}")
            raise Exception(f"Ошибка API: {data['message']}")
        return data
    except requests.RequestException as e:
        logger.error(f"Ошибка HTTP-запроса: {e}")
        raise

# Получение списка складов
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
        logger.error(f"Ошибка получения складов: {e}")
        raise

# Очистка цены от символов
def clean_price(price):
    if isinstance(price, str):
        price = re.sub(r"[^\d.]", "", price)  # Удаляем всё, кроме цифр и точки
        try:
            return float(price)
        except ValueError:
            return 0.0
    return float(price) if price else 0.0

# Расчет наценки на основе оптовой цены
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

# Определение статуса на основе API и правил
def determine_status(item_data):
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

# Сохранение промежуточного состояния
def save_checkpoint(last_batch_index, results):
    checkpoint = {
        "last_batch_index": last_batch_index,
        "results": results
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    logger.info(f"Сохранено состояние: обработано до индекса {last_batch_index}")

# Загрузка промежуточного состояния
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)
        logger.info(f"Загружено состояние: обработано до индекса {checkpoint['last_batch_index']}")
        return checkpoint["last_batch_index"], checkpoint["results"]
    return 0, []

# Обработка артикулов партиями с ретраями и логированием прогресса
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
                logger.error(f"Ошибка обработки партии {i}-{i+BATCH_SIZE}, попытка {attempt+1}/{RETRY_COUNT}: {e}")
                time.sleep(RETRY_DELAY)
        if not success:
            logger.error(f"Не удалось обработать партию {i}-{i+BATCH_SIZE} после {RETRY_COUNT} попыток")
            for _, row in batch.iterrows():
                results.append({
                    "Артикул": row["Артикул"],
                    "Бренд": row["Бренд"],
                    "Старая цена": clean_price(row["Цена"]),
                    "Новая цена": 0,
                    "Старый статус": row["Статус"],
                    "Новый статус": "Под заказ 14-21 дней",
                    "Изменение цены (%)": "N/A",
                    "Склад с наличием": "N/A",
                    "Изменение": "Ошибка"
                })
        else:
            for _, row in batch.iterrows():
                article = row["Артикул"]
                brand = row["Бренд"]
                old_price = clean_price(row["Цена"])
                old_status = row["Статус"]
                item_data = api_items.get(article, {})
                wholesale_price = clean_price(item_data.get("price", 0)) if item_data else 0
                new_price = wholesale_price + calculate_markup(wholesale_price) if item_data else 0
                new_status = determine_status(item_data)
                price_change = (new_price - old_price) / old_price * 100 if old_price > 0 else 0
                change_detected = abs(price_change) >= 10 or old_status != new_status
                stock_info = ", ".join([
                    f"{stock.get('name')} ({stock.get('quantity_unpacked')})"
                    for stock in item_data.get("stocks", {}).values()
                    if stock.get("quantity_unpacked", 0) > 0
                ]) if item_data else "N/A"
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

        # Логирование прогресса
        processed_rows = min(i + BATCH_SIZE, len(items))
        logger.info(f"Обработано {processed_rows}/{total_rows} строк ({(processed_rows/total_rows)*100:.2f}%)")
        print(f"Обработано {processed_rows}/{total_rows} строк ({(processed_rows/total_rows)*100:.2f}%)")

        # Сохранение промежуточного состояния
        save_checkpoint(i + BATCH_SIZE, results)

    return results

def main():
    logger.info("Программа запущена")
    logger.info(f"Используется auth_key: {AUTH_KEY}")

    # Чтение input.xlsx (все строки)
    try:
        df = pd.read_excel("output.xlsx")
        logger.info(f"Загружено {len(df)} строк из output.xlsx")
        print(f"Загружено {len(df)} строк из output.xlsx")
    except Exception as e:
        logger.error(f"Ошибка чтения output.xlsx: {e}")
        print(f"Ошибка чтения output.xlsx: {e}")
        return

    # Проверка наличия необходимых колонок
    required_columns = ["Артикул", "Бренд", "Цена", "Статус"]
    if not all(col in df.columns for col in required_columns):
        logger.error(f"В файле output.xlsx отсутствуют необходимые колонки: {required_columns}")
        print(f"В файле output.xlsx отсутствуют необходимые колонки: {required_columns}")
        return

    # Получение списка складов
    try:
        storage_ids = get_storages()
    except Exception as e:
        logger.error(f"Ошибка получения складов: {e}")
        print(f"Ошибка получения складов: {e}")
        return

    # Обработка артикулов
    results = process_items(df, storage_ids, len(df))

    # Создание отчета
    try:
        report_df = pd.DataFrame(results)
        report_file = f"changes_report_{datetime.now().strftime('%Y%m%d')}.xlsx"
        report_df.to_excel(report_file, index=False)
        logger.info(f"Отчет сохранен: {report_file}")
        print(f"Отчет сохранен: {report_file}")
        # Удаление чекпоинта после успешного завершения
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            logger.info(f"Файл чекпоинта {CHECKPOINT_FILE} удален")
    except Exception as e:
        logger.error(f"Ошибка сохранения отчета: {e}")
        print(f"Ошибка сохранения отчета: {e}")

if __name__ == "__main__":
    main()