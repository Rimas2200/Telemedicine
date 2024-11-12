import pandas as pd


def imt_classify(file_path, output_path='imt.xlsx'):
    """
    Классифицирует вес на основе индекса массы тела (ИМТ) и возраста, и сохраняет результат в Excel-файл.

    :param file_path: str, путь к Excel-файлу с данными, содержащими столбцы 'weight', 'height' и 'age'.
    :param output_path: str, путь для сохранения нового файла с классификацией (по умолчанию 'imt.xlsx').
    :return: None
    """

    data = pd.read_excel(file_path)

    if 'weight' not in data.columns or 'height' not in data.columns or 'age' not in data.columns:
        print("Ошибка: В данных отсутствуют необходимые столбцы 'weight', 'height' или 'age'")
        return

    # Вычисление роста в метрах и ИМТ
    data['height_m'] = data['height'] / 100
    data['ИМТ'] = data['weight'] / (data['height_m'] ** 2)

    # Классификация веса в зависимости от возраста
    def classify_weight_by_age(row):
        bmi = row['ИМТ']
        age = row['age']

        if age < 65:
            if bmi < 18.5:
                return 'Астенический'
            elif 18.5 <= bmi < 25:
                return 'Нормостенический'
            elif 25 <= bmi < 30:
                return 'Мезоморф'
            else:
                return 'Ожирение'
        elif 65 <= age < 75:
            if bmi < 22:
                return 'Астенический'
            elif 22 <= bmi < 26.9:
                return 'Нормостенический'
            elif 27 <= bmi < 30:
                return 'Мезоморф'
            else:
                return 'Ожирение'
        else:
            if bmi <= 23:
                return 'Астенический'
            elif 23 < bmi < 28:
                return 'Нормостенический'
            elif 28 <= bmi < 30:
                return 'Мезоморф'
            else:
                return 'Ожирение'

    # Применение функции классификации
    data['Группа по ИМТ'] = data.apply(classify_weight_by_age, axis=1)

    data.to_excel(output_path, index=False)

    # Подсчет и вывод количества людей в каждой группе по ИМТ
    group_counts = data['Группа по ИМТ'].value_counts()
    print(group_counts)


imt_classify('unique_data.xlsx')
