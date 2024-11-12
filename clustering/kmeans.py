import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA


def load_data(file_path: str, sheet_name: str = 'Sheet1') -> pd.DataFrame:
    """
    Загружает данные из Excel файла.

    :param file_path: Путь к Excel файлу.
    :param sheet_name: Название листа в файле.
    :return: DataFrame с данными из файла.
    """
    data = pd.read_excel(file_path, sheet_name=sheet_name)
    return data


def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Обрабатывает данные, удаляя ненужные столбцы и извлекая признаки.

    :param data: DataFrame с исходными данными.
    :return: DataFrame с признаками, готовыми для кластеризации.
    """
    columns = data.columns.tolist()
    columns.remove('cluster_number')
    columns.remove('user_id')
    columns.remove('date')
    columns.remove('min_distance')

    features = data[columns]
    return features


def elbow_method(features: pd.DataFrame) -> list:
    """
    Применяет метод локтя для определения оптимального числа кластеров.

    :param features: Признаки для кластеризации.
    :return: Список значений SSE для каждого числа кластеров.
    """
    sse = []
    K_range = range(1, 11)
    for k in K_range:
        kmeans = KMeans(n_clusters=k, random_state=0, max_iter=1000)
        kmeans.fit(features)
        sse.append(kmeans.inertia_)  # inertia_ хранит значение SSE для k
    return sse


def plot_elbow_method(K_range: range, sse: list) -> None:
    """
    Строит график метода локтя для определения оптимального числа кластеров.

    :param K_range: Диапазон значений количества кластеров.
    :param sse: Список значений SSE для каждого числа кластеров.
    :return: None
    """
    plt.figure(figsize=(8, 4))
    plt.plot(K_range, sse, marker='o')
    plt.title('Метод локтя для определения оптимального числа кластеров')
    plt.xlabel('Количество кластеров (K)')
    plt.ylabel('Сумма квадратов ошибок (SSE)')
    plt.xticks(K_range)
    plt.grid()
    plt.show()


def perform_kmeans(features: pd.DataFrame, n_clusters: int) -> KMeans:
    """
    Выполняет кластеризацию с использованием алгоритма KMeans.

    :param features: Признаки для кластеризации.
    :param n_clusters: Количество кластеров.
    :return: Объект модели KMeans после обучения.
    """
    kmeans = KMeans(n_clusters=n_clusters, random_state=0, max_iter=1000)
    kmeans.fit(features)
    return kmeans


def add_cluster_info(data: pd.DataFrame, kmeans: KMeans, features: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет информацию о кластерах и расстоянии до центров в исходные данные.

    :param data: Исходный DataFrame с данными.
    :param kmeans: Обученная модель KMeans.
    :param features: Признаки для кластеризации.
    :return: DataFrame с добавленными метками кластеров и расстояниями.
    """
    labels = kmeans.labels_
    cluster_centers = kmeans.cluster_centers_
    distances = cdist(features, cluster_centers, 'euclidean')

    data['cluster'] = labels
    data['distance'] = np.min(distances, axis=1)
    return data


def save_to_excel(data: pd.DataFrame, file_name: str) -> None:
    """
    Сохраняет данные в Excel файл.

    :param data: DataFrame с данными.
    :param file_name: Имя файла для сохранения.
    :return: None
    """
    data.to_excel(file_name, index=False)


def apply_pca(features: pd.DataFrame, n_components: int = 2) -> np.ndarray:
    """
    Применяет метод главных компонент (PCA) для снижения размерности до 2D.

    :param features: Признаки для PCA.
    :param n_components: Количество компонентов после снижения размерности.
    :return: Результат применения PCA (матрица признаков).
    """
    pca = PCA(n_components=n_components)
    pca_result = pca.fit_transform(features)
    return pca_result


def visualize_clusters(pca_result: np.ndarray, labels: np.ndarray, n_clusters: int) -> None:
    """
    Визуализирует кластеры на 2D графике.

    :param pca_result: Результат применения PCA для 2D визуализации.
    :param labels: Метки кластеров.
    :param n_clusters: Количество кластеров.
    :return: None
    """
    visualization_df = pd.DataFrame(data=pca_result, columns=['PC1', 'PC2'])
    visualization_df['cluster'] = labels

    plt.figure(figsize=(10, 6))
    for cluster in range(n_clusters):
        cluster_data = visualization_df[visualization_df['cluster'] == cluster]
        plt.scatter(cluster_data['PC1'], cluster_data['PC2'], label=f'Cluster {cluster}')

    plt.title('Visualization of Clusters')
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.legend()
    plt.grid()
    plt.savefig('clusters.png', dpi=300)
    plt.show()


data = load_data('kmeans-new-data.xlsx')

features = preprocess_data(data)

sse = elbow_method(features)
plot_elbow_method(range(1, 11), sse)

n_clusters = 4
kmeans = perform_kmeans(features, n_clusters)

data = add_cluster_info(data, kmeans, features)

save_to_excel(data, 'clusters.xlsx')

pca_result = apply_pca(features)
visualize_clusters(pca_result, kmeans.labels_, n_clusters)
