# -*- coding: utf-8 -*-
import mysql.connector
import pandas as pd
from datetime import datetime
import random

mysql_connect = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='',
    database='Telemedicine'
)
cursor = mysql_connect.cursor()

required_values = {'Steps', 'Glucose', 'Temperature', 'Pulse', 'Breath',
                   'Oxygenation', 'BloodPressure', 'EDA', 'Ecg',
                   'AvgPulse', 'Vitality', 'Adoptability',
                   'Neuroplasticity', 'Age'}


def parsing_csv():
    """
    Загружает данные из файла 'Обезличенные данные.csv' и вставляет их в таблицу Anonymized_data.
    :return: None
    """

    data = pd.read_csv('../Обезличенные данные.csv', delimiter=';', skiprows=1,
                       names=['id', 'user_id', 'tracker_id', 'type', 'value', 'sys', 'dia', 'time'],
                       dtype=str).fillna(None)

    insert_query = """
    INSERT INTO Anonymized_data (id, user_id, tracker_id, type, value, sys, dia, time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = data.values.tolist()

    cursor.executemany(insert_query, records)
    mysql_connect.commit()


def update_user_data():
    """
    Читает данные из CSV файла 'users.csv', вычисляет возраст пользователей на основе их даты рождения и
    выводит информацию о росте, весе и возрасте для каждого пациента. В случае некорректной даты рождения
    возраст не вычисляется и выводится 'N/A'.
    :return: None
    """

    df = pd.read_csv('../users.csv', sep=';')
    today = datetime.now()

    def calculate_age(birth_date):
        try:
            birth_date = str(birth_date)
            birth_date_obj = datetime.strptime(birth_date, "%Y-%m-%d %H:%M:%S.%f")
            age = (today - birth_date_obj).days // 365
            return age
        except (ValueError, TypeError):
            return None

    for index, row in df.iterrows():
        user_id = row['id']
        height = row['height']
        weight = row['weight']
        age = calculate_age(row['birth_date'])
        if age is not None:
            cursor.execute("""
                UPDATE type
                SET age = %s, height = %s, weight = %s
                WHERE user_id = %s
            """, (age, height, weight, user_id))
        print(user_id, age, weight, height)
    mysql_connect.commit()


def update_user_gender():
    """
    Читает данные о поле пользователей из CSV файла 'gender.csv', преобразует значения в числовой формат
    (1 для мужчин, 0 для женщин), и обновляет соответствующие записи в таблице базы данных 'type'
    для каждого пациента по его user_id.
    :return: None
    """
    gender_data = pd.read_csv("../gender.csv", sep=";")
    gender_data['gender'] = gender_data['avatar_gender'].apply(lambda x: '1' if x == 'MALE' else '0')

    for _, row in gender_data.iterrows():
        user_id = row['user_id']
        gender = row['gender']

        cursor.execute("""
            UPDATE type SET gender = %s WHERE user_id = %s
        """, (gender, user_id))
        print(user_id)

    mysql_connect.commit()


def insert_unique_user_ids():
    """
    Извлекает уникальные user_id из Anonymized_data и вставляет их в таблицу Patient, избегая дублирования.
    :return: None
    """
    cursor.execute("SELECT DISTINCT user_id FROM Anonymized_data")
    rows = cursor.fetchall()

    user_ids = [(str(row[0]),) for row in rows]

    insert_query = """
    INSERT INTO Patient (user_id) VALUES (%s)
    ON DUPLICATE KEY UPDATE user_id=user_id
    """

    cursor.executemany(insert_query, user_ids)
    mysql_connect.commit()


from datetime import datetime


def insert_unique_times():
    """
    Извлекает уникальные значения времени из Anonymized_data, форматирует их и вставляет в таблицу Time, избегая дублирования.
    :return: None
    """
    cursor.execute("SELECT DISTINCT time FROM Anonymized_data")
    rows = cursor.fetchall()

    times = []
    for row in rows:
        try:
            timestamp_ms = int(row[0])
            dt = datetime.fromtimestamp(timestamp_ms / 1000)
            formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
            times.append((formatted_time,))
        except (ValueError, TypeError):
            continue

    insert_query = """
    INSERT INTO Time (time) VALUES (%s)
    ON DUPLICATE KEY UPDATE time=time
    """

    cursor.executemany(insert_query, times)
    mysql_connect.commit()


def update_patient_times():
    """
    Обновляет время в таблице Patient, записывая уникальные временные метки из Anonymized_data для каждого user_id.
    Результаты также записываются в файл patient_times.txt.
    :return: None
    """

    cursor.execute("SELECT user_id FROM Patient")
    user_ids = [user_id[0] for user_id in cursor.fetchall()]

    if user_ids:
        placeholders = ', '.join(['%s'] * len(user_ids))
        query = f"SELECT user_id, time FROM Anonymized_data WHERE user_id IN ({placeholders})"
        cursor.execute(query, user_ids)
        results = cursor.fetchall()

        user_times = {}
        for user_id, time in results:
            if user_id not in user_times:
                user_times[user_id] = set()
            user_times[user_id].add(time)

        with open('patient_times.txt', 'w') as file:
            update_data = []
            for user_id, times in user_times.items():
                formatted_times = ', '.join(sorted(times))
                output_line = f"{user_id}: {formatted_times}\n"

                update_data.append((formatted_times, user_id))

                print(output_line.strip())
                file.write(output_line)

            update_query = "UPDATE Patient SET time = %s WHERE user_id = %s"
            cursor.executemany(update_query, update_data)
            mysql_connect.commit()


def update_patient_parameters():
    """
    Обновляет данные параметров пациентов в таблице Patient_parametr, извлекая и форматируя данные из Anonymized_data.
    :return: None
    """
    cursor.execute("SELECT user_id, time FROM Patient")
    patient_records = cursor.fetchall()

    for user_id, time_data in patient_records:
        time_list = [int(time) for time in time_data.split(', ')]
        placeholders = ', '.join(['%s'] * len(time_list))
        query = f"""
            SELECT 
                user_id, time, type, value,
                CASE WHEN type = 'BloodPressure' THEN sys ELSE NULL END AS BloodPressure_sys,
                CASE WHEN type = 'BloodPressure' THEN dia ELSE NULL END AS BloodPressure_dia,
                CASE WHEN type = 'Temperature' THEN value ELSE NULL END AS Temperature,
                CASE WHEN type = 'Glucose' THEN value ELSE NULL END AS Glucose,
                CASE WHEN type = 'Steps' THEN value ELSE NULL END AS Steps
            FROM 
                Anonymized_data 
            WHERE 
                user_id = %s AND time IN ({placeholders})
        """
        cursor.execute(query, [user_id] + time_list)
        results = cursor.fetchall()

        data_dict = {}
        for record in results:
            _, time, type_, value, sys_value, dia_value, temperature, glucose, steps = record
            if (user_id, time) not in data_dict:
                data_dict[(user_id, time)] = {
                    "temperature": None,
                    "blood_pressure_sys": None,
                    "blood_pressure_dia": None,
                    "glucose": None,
                    "steps": None
                }

            if type_ == 'Temperature':
                data_dict[(user_id, time)]["temperature"] = value
            elif type_ == 'BloodPressure':
                data_dict[(user_id, time)]["blood_pressure_sys"] = sys_value
                data_dict[(user_id, time)]["blood_pressure_dia"] = dia_value
            elif type_ == 'Glucose':
                data_dict[(user_id, time)]["glucose"] = value
            elif type_ == 'Steps':
                data_dict[(user_id, time)]["steps"] = value

        update_data = [(
                user_id,
                time,
                params["temperature"],
                params["glucose"],
                params["blood_pressure_sys"],
                params["blood_pressure_dia"],
                params["steps"]
            )for (user_id, time), params in data_dict.items()]

        insert_query = """
            INSERT INTO Patient_parametr (user_id, time, temperature, glucose, blood_pressure_sys, blood_pressure_dia, steps)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                temperature = VALUES(temperature),
                glucose = VALUES(glucose),
                blood_pressure_sys = VALUES(blood_pressure_sys),
                blood_pressure_dia = VALUES(blood_pressure_dia),
                steps = VALUES(steps)
        """
        cursor.executemany(insert_query, update_data)
        mysql_connect.commit()


def patient_types():
    """
    Извлекает данные пациентов из таблицы `Patient`, обрабатывает параметры времени и типов измерений для каждого пациента,
    а затем вставляет или обновляет записи в таблице `type` с параметрами пациента.
    :return: None
    """
    cursor.execute("SELECT user_id, time FROM Patient")
    patient_records = cursor.fetchall()

    for user_id, time_data in patient_records:
        time_list = time_data.split(', ')
        time_list = [int(time) for time in time_list]

        placeholders = ', '.join(['%s'] * len(time_list))
        query = f"""
            SELECT 
                user_id, time, type, value,
                CASE WHEN type = 'BloodPressure' THEN sys ELSE NULL END AS BloodPressure_sys,
                CASE WHEN type = 'BloodPressure' THEN dia ELSE NULL END AS BloodPressure_dia
            FROM 
                Anonymized_data 
            WHERE 
                user_id = %s AND time IN ({placeholders})
        """
        cursor.execute(query, [user_id] + time_list)
        results = cursor.fetchall()

        data_dict = {}

        for record in results:
            user_id, time, type_, value, sys_value, dia_value = record

            if (user_id, time) not in data_dict:
                data_dict[(user_id, time)] = {
                    "steps": None,
                    "glucose": None,
                    "temperature": None,
                    "pulse": None,
                    "breath": None,
                    "oxygenation": None,
                    "blood_pressure_sys": None,
                    "blood_pressure_dia": None,
                    "EDA": None,
                    "ecg": None,
                    "avg_pulse": None,
                    "vitality": None,
                    "adoptability": None,
                    "neuroplasticity": None,
                    "age": None
                }

            if type_ == 'Steps':
                data_dict[(user_id, time)]["steps"] = value
            elif type_ == 'Glucose':
                data_dict[(user_id, time)]["glucose"] = value
            elif type_ == 'Temperature':
                data_dict[(user_id, time)]["temperature"] = value
            elif type_ == 'Pulse':
                data_dict[(user_id, time)]["pulse"] = value
            elif type_ == 'Breath':
                data_dict[(user_id, time)]["breath"] = value
            elif type_ == 'Oxygenation':
                data_dict[(user_id, time)]["oxygenation"] = value
            elif type_ == 'BloodPressure':
                data_dict[(user_id, time)]["blood_pressure_sys"] = sys_value
                data_dict[(user_id, time)]["blood_pressure_dia"] = dia_value
            elif type_ == 'EDA':
                data_dict[(user_id, time)]["EDA"] = value
            elif type_ == 'Ecg':
                data_dict[(user_id, time)]["ecg"] = value
            elif type_ == 'AvgPulse':
                data_dict[(user_id, time)]["avg_pulse"] = value
            elif type_ == 'Vitality':
                data_dict[(user_id, time)]["vitality"] = value
            elif type_ == 'Adoptability':
                data_dict[(user_id, time)]["adoptability"] = value
            elif type_ == 'Neuroplasticity':
                data_dict[(user_id, time)]["neuroplasticity"] = value
            elif type_ == 'Age':
                data_dict[(user_id, time)]["age"] = value

        for (user_id, time), params in data_dict.items():
            insert_query = """
                INSERT INTO type 
                (user_id, time, steps, glucose, temperature, pulse, breath, oxygenation, blood_pressure_sys, 
                 blood_pressure_dia, EDA, ecg, avg_pulse, vitality, adoptability, neuroplasticity, age)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    steps = VALUES(steps),
                    glucose = VALUES(glucose),
                    temperature = VALUES(temperature),
                    pulse = VALUES(pulse),
                    breath = VALUES(breath),
                    oxygenation = VALUES(oxygenation),
                    blood_pressure_sys = VALUES(blood_pressure_sys),
                    blood_pressure_dia = VALUES(blood_pressure_dia),
                    EDA = VALUES(EDA),
                    ecg = VALUES(ecg),
                    avg_pulse = VALUES(avg_pulse),
                    vitality = VALUES(vitality),
                    adoptability = VALUES(adoptability),
                    neuroplasticity = VALUES(neuroplasticity),
                    age = VALUES(age)
            """

            cursor.execute(insert_query, (
                user_id,
                time,
                params["steps"],
                params["glucose"],
                params["temperature"],
                params["pulse"],
                params["breath"],
                params["oxygenation"],
                params["blood_pressure_sys"],
                params["blood_pressure_dia"],
                params["EDA"],
                params["ecg"],
                params["avg_pulse"],
                params["vitality"],
                params["adoptability"],
                params["neuroplasticity"],
                params["age"]
            ))

        mysql_connect.commit()


def delete_users():
    """
    Удаляет данные пациентов по user_id из test_data.txt
    :return: None
    """
    with open('test_data.txt', 'r') as file:
        user_ids = [line.strip() for line in file.readlines()]

    for user_id in user_ids:
        cursor.execute("DELETE FROM type WHERE user_id = %s", (user_id,))
        print(user_id)

    mysql_connect.commit()


def generate_oxygenation():
    return int(random.triangular(80, 99, 98))


def generate_blood_pressure_sys():
    return random.randint(100, 143)


def generate_blood_pressure_dia():
    return random.randint(65, 99)


def update_pulse():
    """
    Вычисляет среднее значение пульса для каждого пациента и обновляет записи в таблице `type`,
    где поле `avg_pulse` ещё не заполнено.
    :return: None
    """

    cursor.execute("""
        SELECT user_id, AVG(pulse) AS avg_pulse FROM type WHERE pulse IS NOT NULL GROUP BY user_id
    """)
    avg_pulse_data = cursor.fetchall()

    for user_id, avg_pulse in avg_pulse_data:
        cursor.execute("""
            UPDATE type SET avg_pulse = %s WHERE user_id = %s AND avg_pulse IS NULL
        """, (avg_pulse, user_id))

    mysql_connect.commit()


def fill_oxygenation():
    """
    Заполняет значения уровня кислорода (oxygenation) для записей в таблице 'type',
    где данное значение еще не задано.
    :return: None
    """

    cursor.execute("SELECT id FROM type WHERE oxygenation IS NULL")
    rows = cursor.fetchall()

    for (row_id,) in rows:
        oxygenation_value = generate_oxygenation()
        cursor.execute(
            "UPDATE type SET oxygenation = %s WHERE id = %s",
            (oxygenation_value, row_id)
        )

    mysql_connect.commit()


def fill_blood_pressure():
    """
    Заполняет значения для систолического (blood_pressure_sys) и диастолического
    (blood_pressure_dia) давления для записей в таблице 'type', где хотя бы одно из
    этих значений отсутствует.
    :return: None
    """

    cursor.execute("SELECT id FROM type WHERE blood_pressure_sys IS NULL OR blood_pressure_dia IS NULL")
    rows = cursor.fetchall()

    for (row_id,) in rows:
        blood_pressure_sys_value = generate_blood_pressure_sys()
        blood_pressure_dia_value = generate_blood_pressure_dia()

        cursor.execute("""
            UPDATE type 
            SET blood_pressure_sys = COALESCE(blood_pressure_sys, %s), 
                blood_pressure_dia = COALESCE(blood_pressure_dia, %s) 
            WHERE id = %s
        """, (blood_pressure_sys_value, blood_pressure_dia_value, row_id))

    mysql_connect.commit()


cursor.close()
mysql_connect.close()
