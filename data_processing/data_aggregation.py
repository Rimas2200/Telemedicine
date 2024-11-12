import pandas as pd


def calculate_summary_statistics(input_file, output_file):
    """
    Вычисляет сводную статистику для каждого показателя, включая максимум, минимум, среднее, медиану и стандартное отклонение
    по каждому пользователю и дате, и сохраняет результаты в Excel-файл.

    :param input_file: str, путь к входному Excel-файлу с исходными данными, содержащими столбцы 'user_id', 'time', 'temperature',
                       'pulse', 'glucose', 'breath', 'oxygenation', 'blood_pressure_sys', 'blood_pressure_dia', 'avg_pulse'.
    :param output_file: str, путь для сохранения выходного Excel-файла с рассчитанной сводной статистикой.
    :return: None
    """

    df = pd.read_excel(input_file, sheet_name='Sheet1')
    df['time'] = pd.to_datetime(df['time'])
    df['date'] = df['time'].dt.date

    result = df.groupby(['user_id', 'date']).agg(
        max_temperature=('temperature', 'max'),
        min_temperature=('temperature', 'min'),
        mean_temperature=('temperature', 'mean'),
        median_temperature=('temperature', 'median'),
        std_temperature=('temperature', 'std'),
        max_pulse=('pulse', 'max'),
        min_pulse=('pulse', 'min'),
        mean_pulse=('pulse', 'mean'),
        median_pulse=('pulse', 'median'),
        std_pulse=('pulse', 'std'),
        max_glucose=('glucose', 'max'),
        min_glucose=('glucose', 'min'),
        mean_glucose=('glucose', 'mean'),
        median_glucose=('glucose', 'median'),
        std_glucose=('glucose', 'std'),
        max_breath=('breath', 'max'),
        min_breath=('breath', 'min'),
        mean_breath=('breath', 'mean'),
        median_breath=('breath', 'median'),
        std_breath=('breath', 'std'),
        max_oxygenation=('oxygenation', 'max'),
        min_oxygenation=('oxygenation', 'min'),
        mean_oxygenation=('oxygenation', 'mean'),
        median_oxygenation=('oxygenation', 'median'),
        std_oxygenation=('oxygenation', 'std'),
        max_blood_pressure_sys=('blood_pressure_sys', 'max'),
        min_blood_pressure_sys=('blood_pressure_sys', 'min'),
        mean_blood_pressure_sys=('blood_pressure_sys', 'mean'),
        median_blood_pressure_sys=('blood_pressure_sys', 'median'),
        std_blood_pressure_sys=('blood_pressure_sys', 'std'),
        max_blood_pressure_dia=('blood_pressure_dia', 'max'),
        min_blood_pressure_dia=('blood_pressure_dia', 'min'),
        mean_blood_pressure_dia=('blood_pressure_dia', 'mean'),
        median_blood_pressure_dia=('blood_pressure_dia', 'median'),
        std_blood_pressure_dia=('blood_pressure_dia', 'std'),
        max_avg_pulse=('avg_pulse', 'max'),
        min_avg_pulse=('avg_pulse', 'min'),
        mean_avg_pulse=('avg_pulse', 'mean'),
        median_avg_pulse=('avg_pulse', 'median'),
        std_avg_pulse=('avg_pulse', 'std'),
    ).reset_index()

    result.to_excel(output_file, index=False)


calculate_summary_statistics('rounded_file.xlsx', 'kmeans-data.xlsx')
