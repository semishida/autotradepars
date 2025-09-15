import pandas as pd
import logging
from datetime import datetime
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

# Настройка логирования
log_file = f"finalize_price_{datetime.now().strftime('%Y%m%d')}.log"
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

# Конфигурация SMTP для отправки email
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "autotradeprice@gmail.com"  # Замените на ваш email
SMTP_PASSWORD = "lbph rwdh ketm daei"  # Замените на пароль приложения Gmail
EMAIL_RECIPIENTS = ["delevinrero@gmail.com", "rdelev@mail.ru"]


# Функция для определения статуса на основе строки складов
def determine_status_from_stock_info(stock_info):
    if pd.isna(stock_info) or stock_info == "N/A" or not stock_info:
        return "Под заказ 14-21 дней"

    stocks = {}
    items = stock_info.split(", ")
    for item in items:
        match = re.match(r"(.+) \(([0-9]+)\)", item.strip())
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


# Обработка changes_report
def process_changes_report(changes_file):
    logger.info(f"Чтение файла изменений: {changes_file}")
    try:
        changes_df = pd.read_excel(changes_file)
        logger.info(f"Количество строк в changes_report до dedup: {len(changes_df)}")
        changes_df = changes_df.drop_duplicates(subset=["Артикул", "Бренд"])
        logger.info(f"Загружено {len(changes_df)} уникальных строк из {changes_file}")
    except Exception as e:
        logger.error(f"Ошибка чтения {changes_file}: {e}")
        print(f"Ошибка чтения {changes_file}: {e}")
        return None

    changes_df["Новый статус"] = changes_df["Склад с наличием"].apply(determine_status_from_stock_info)

    for idx, row in changes_df.iterrows():
        if row["Новая цена"] == 0 or row["Новая цена"] == 25:
            changes_df.at[idx, "Новая цена"] = row["Старая цена"]
            changes_df.at[idx, "Новый статус"] = "Под заказ 14-21 дней"
            logger.info(
                f"Артикул {row['Артикул']}: Цена изменена на старую ({row['Старая цена']}), статус на 'Под заказ 14-21 дней'")

    return changes_df


# Обновление output.xlsx
def update_output(original_file, changes_file):
    logger.info(f"Чтение исходного файла: {original_file}")
    try:
        output_df = pd.read_excel(original_file)
        logger.info(f"Количество строк в output.xlsx: {len(output_df)}")
    except Exception as e:
        logger.error(f"Ошибка чтения {original_file}: {e}")
        print(f"Ошибка чтения {original_file}: {e}")
        return None

    changes_df = process_changes_report(changes_file)
    if changes_df is None:
        logger.error("Не удалось обработать changes_report, возвращаем output без изменений")
        return output_df

    # Создаем временные столбцы для обновления
    output_df = output_df.merge(
        changes_df[["Артикул", "Бренд", "Новый статус", "Новая цена"]],
        on=["Артикул", "Бренд"],
        how="left"
    )

    # Логирование количества строк после merge
    logger.info(f"Количество строк в output_df после merge: {len(output_df)}")

    # Обновляем Статус и Цена, если есть новые значения
    updated_count = 0
    for idx, row in output_df.iterrows():
        if pd.notna(row["Новый статус"]) and pd.notna(row["Новая цена"]):
            output_df.at[idx, "Статус"] = row["Новый статус"]
            output_df.at[idx, "Цена"] = row["Новая цена"]
            logger.info(
                f"Обновлен артикул {row['Артикул']}: Новый статус={row['Новый статус']}, Цена={row['Новая цена']}")
            updated_count += 1

    # Удаляем временные столбцы
    output_df = output_df.drop(columns=["Новый статус", "Новая цена"], errors="ignore")
    logger.info(f"Обновлено {updated_count} строк, итоговое количество строк: {len(output_df)}")

    return output_df


# Отправка файла по email
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
        logger.error(f"Ошибка прикрепления файла {file_path}: {e}")
        print(f"Ошибка прикрепления файла {file_path}: {e}")
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
        logger.error(f"Ошибка отправки письма: {e}")
        print(f"Ошибка отправки письма: {e}")
        return False


def main():
    logger.info("Программа запущена")

    date = datetime.now().strftime('%Y%m%d')
    changes_file = f"changes_report_{date}.xlsx"
    original_file = "output.xlsx"
    final_file = f"price_{date}.xlsx"

    updated_df = update_output(original_file, changes_file)
    if updated_df is None:
        logger.error("Обновление output не удалось")
        print("Обновление output не удалось")
        return

    try:
        updated_df.to_excel(final_file, index=False)
        logger.info(f"Итоговый файл сохранен: {final_file} с {len(updated_df)} строками")
        print(f"Итоговый файл сохранен: {final_file} с {len(updated_df)} строками")
    except Exception as e:
        logger.error(f"Ошибка сохранения {final_file}: {e}")
        print(f"Ошибка сохранения {final_file}: {e}")
        return

    if os.path.exists(final_file):
        send_email(final_file)
    else:
        logger.error(f"Файл {final_file} не существует")
        print(f"Файл {final_file} не существует")


if __name__ == "__main__":
    main()