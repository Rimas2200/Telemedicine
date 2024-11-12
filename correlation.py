import pandas as pd


def correlation(file_path: str, cluster_value: int) -> pd.DataFrame:
    """
    Функция для расчета корреляции между уровнем глюкозы и другими показателями в заданном кластере.

    :param file_path: Путь к Excel файлу с данными.
    :param cluster_value: Значение кластера, для которого нужно вычислить корреляцию.
    :return: DataFrame с корреляцией между глюкозой и другими показателями.
    """
    data = pd.read_excel(file_path)

    filtered_data = data[data['cluster'] == cluster_value]

    if 'max_glucose' not in filtered_data.columns:
        print("Столбец 'Глюкоза' отсутствует в данных.")
        return None

    filtered_data = filtered_data.drop(columns=['user_id', 'cluster'], axis=1)

    # Вычисление корреляции
    correlation_matrix = filtered_data.corr()

    # Извлечение корреляции между глюкозой и другими показателями
    glucose_correlation = correlation_matrix['mean_glucose']

    # Сохраняем результат в Excel файл
    glucose_correlation.to_excel(f'glucose_correlation_cluster_{cluster_value}.xlsx', index=True)

    # Возвращаем DataFrame с корреляцией
    return glucose_correlation


file_path = 'data.xlsx'
cluster_value = 0
correlation_result = correlation(file_path, cluster_value)

if correlation_result is not None:
    print("Корреляция между Глюкозой и другими показателями:")
    print(correlation_result)
