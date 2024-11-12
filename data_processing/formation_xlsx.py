import mysql.connector
from openpyxl import Workbook
from datetime import datetime

mysql_connect = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='',
    database='Telemedicine'
)

cursor = mysql_connect.cursor()

def export_to_excel():
    """
    Извлекает данные из таблицы 'type', конвертирует значения времени
    из миллисекунд в формат 'YYYY-MM-DD HH:MM:SS', а также преобразует числовые значения
    в тип float. Результаты сохраняются в Excel файл 'type_time.xlsx'.
    :return: None
    """

    query = "SELECT * FROM type"
    cursor.execute(query)

    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    float_columns = [
        "steps", "glucose", "temperature", "pulse", "breath",
        "oxygenation", "blood_pressure_sys", "blood_pressure_dia",
        "EDA", "ecg", "avg_pulse", "vitality", "adoptability", "neuroplasticity"
    ]

    wb = Workbook()
    ws = wb.active
    ws.title = "type_time"

    ws.append(columns)

    time_index = columns.index('time') if 'time' in columns else None

    float_indices = [columns.index(col) for col in float_columns if col in columns]

    for row in data:
        row = list(row)

        if time_index is not None and row[time_index] is not None:
            timestamp_ms = int(row[time_index])
            dt = datetime.fromtimestamp(timestamp_ms / 1000)
            row[time_index] = dt.strftime('%Y-%m-%d %H:%M:%S')

        for index in float_indices:
            try:
                row[index] = float(row[index]) if row[index] is not None else None
            except ValueError:
                row[index] = None

        ws.append(row)

    wb.save("type_time.xlsx")


cursor.close()
mysql_connect.close()
