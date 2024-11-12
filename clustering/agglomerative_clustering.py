import mysql.connector
import pandas as pd
from scipy.cluster.hierarchy import linkage, fcluster
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

mysql_connect = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='',
    database='Telemedicine'
)
cursor = mysql_connect.cursor()

# SQL запрос для получения статистики по данным
query = """
    SELECT user_id,
    
    -- Статистика по шагам
    AVG(steps) AS avg_steps,
    MAX(steps) AS max_steps,
    MIN(steps) AS min_steps,
    (SELECT steps FROM (
        SELECT steps, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY steps) AS row_num,
               COUNT(*) OVER(PARTITION BY user_id) AS total_count
        FROM Classification
    ) AS subquery
    WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))
    ORDER BY row_num
    LIMIT 1) AS median_steps,
    STDDEV(steps) AS stddev_steps,
    
    -- Статистика по глюкозе
    AVG(glucose) AS avg_glucose,
    MAX(glucose) AS max_glucose,
    MIN(glucose) AS min_glucose,
    (SELECT glucose FROM (
        SELECT glucose, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY glucose) AS row_num,
               COUNT(*) OVER(PARTITION BY user_id) AS total_count
        FROM Classification
    ) AS subquery
    WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))
    ORDER BY row_num
    LIMIT 1) AS median_glucose,
    STDDEV(glucose) AS stddev_glucose,
    
    -- Статистика по температуре
    AVG(temperature) AS avg_temperature,
    MAX(temperature) AS max_temperature,
    MIN(temperature) AS min_temperature,
    (SELECT temperature FROM (
        SELECT temperature, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY temperature) AS row_num,
               COUNT(*) OVER(PARTITION BY user_id) AS total_count
        FROM Classification
    ) AS subquery
    WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))
    ORDER BY row_num
    LIMIT 1) AS median_temperature,
    STDDEV(temperature) AS stddev_temperature,
    
    -- Статистика по пульсу
    AVG(pulse) AS avg_pulse,
    MAX(pulse) AS max_pulse,
    MIN(pulse) AS min_pulse,
    (SELECT pulse FROM (
        SELECT pulse, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY pulse) AS row_num,
               COUNT(*) OVER(PARTITION BY user_id) AS total_count
        FROM Classification
    ) AS subquery
    WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))
    ORDER BY row_num
    LIMIT 1) AS median_pulse,
    STDDEV(pulse) AS stddev_pulse,
    
    -- Статистика по дыханию
    AVG(breath) AS avg_breath,
    MAX(breath) AS max_breath,
    MIN(breath) AS min_breath,
    (SELECT breath FROM (
        SELECT breath, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY breath) AS row_num,
               COUNT(*) OVER(PARTITION BY user_id) AS total_count
        FROM Classification
    ) AS subquery
    WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))
    ORDER BY row_num
    LIMIT 1) AS median_breath,
    STDDEV(breath) AS stddev_breath,
    
    -- Статистика по сатурации
    AVG(oxygenation) AS avg_oxygenation,
    MAX(oxygenation) AS max_oxygenation,
    MIN(oxygenation) AS min_oxygenation,
    (SELECT oxygenation FROM (
        SELECT oxygenation, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY oxygenation) AS row_num,
               COUNT(*) OVER(PARTITION BY user_id) AS total_count
        FROM Classification
    ) AS subquery
    WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))
    ORDER BY row_num
    LIMIT 1) AS median_oxygenation,
    STDDEV(oxygenation) AS stddev_oxygenation,
    
    -- Статистика по систолическому давлению
    AVG(blood_pressure_sys) AS avg_blood_pressure_sys,
    MAX(blood_pressure_sys) AS max_blood_pressure_sys,
    MIN(blood_pressure_sys) AS min_blood_pressure_sys,
    (SELECT blood_pressure_sys FROM (
        SELECT blood_pressure_sys, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY blood_pressure_sys) AS row_num,
               COUNT(*) OVER(PARTITION BY user_id) AS total_count
        FROM Classification
    ) AS subquery
    WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))
    ORDER BY row_num
    LIMIT 1) AS median_blood_pressure_sys,
    STDDEV(blood_pressure_sys) AS stddev_blood_pressure_sys,
    
    -- Статистика по диастолическому давлению
    AVG(blood_pressure_dia) AS avg_blood_pressure_dia,
    MAX(blood_pressure_dia) AS max_blood_pressure_dia,
    MIN(blood_pressure_dia) AS min_blood_pressure_dia,
    (SELECT blood_pressure_dia FROM (
        SELECT blood_pressure_dia, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY blood_pressure_dia) AS row_num,
               COUNT(*) OVER(PARTITION BY user_id) AS total_count
        FROM Classification
    ) AS subquery
    WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))
    ORDER BY row_num
    LIMIT 1) AS median_blood_pressure_dia,
    STDDEV(blood_pressure_dia) AS stddev_blood_pressure_dia

FROM Classification
GROUP BY user_id;
"""

cursor.execute(query)
records = cursor.fetchall()

columns = [
    'user_id',
    'avg_steps', 'max_steps', 'min_steps', 'median_steps', 'stddev_steps',
    'avg_glucose', 'max_glucose', 'min_glucose', 'median_glucose', 'stddev_glucose',
    'avg_temperature', 'max_temperature', 'min_temperature', 'median_temperature', 'stddev_temperature',
    'avg_pulse', 'max_pulse', 'min_pulse', 'median_pulse', 'stddev_pulse',
    'avg_breath', 'max_breath', 'min_breath', 'median_breath', 'stddev_breath',
    'avg_oxygenation', 'max_oxygenation', 'min_oxygenation', 'median_oxygenation', 'stddev_oxygenation',
    'avg_blood_pressure_sys', 'max_blood_pressure_sys', 'min_blood_pressure_sys', 'median_blood_pressure_sys', 'stddev_blood_pressure_sys',
    'avg_blood_pressure_dia', 'max_blood_pressure_dia', 'min_blood_pressure_dia', 'median_blood_pressure_dia', 'stddev_blood_pressure_dia'
]

df = pd.DataFrame(records, columns=columns)

features = df[
    [
        'avg_steps', 'max_steps', 'min_steps', 'median_steps', 'stddev_steps',
        'avg_glucose', 'max_glucose', 'min_glucose', 'median_glucose', 'stddev_glucose',
        'avg_temperature', 'max_temperature', 'min_temperature', 'median_temperature', 'stddev_temperature',
        'avg_pulse', 'max_pulse', 'min_pulse', 'median_pulse', 'stddev_pulse',
        'avg_breath', 'max_breath', 'min_breath', 'median_breath', 'stddev_breath',
        'avg_oxygenation', 'max_oxygenation', 'min_oxygenation', 'median_oxygenation', 'stddev_oxygenation',
        'avg_blood_pressure_sys', 'max_blood_pressure_sys', 'min_blood_pressure_sys', 'median_blood_pressure_sys', 'stddev_blood_pressure_sys',
        'avg_blood_pressure_dia', 'max_blood_pressure_dia', 'min_blood_pressure_dia', 'median_blood_pressure_dia', 'stddev_blood_pressure_dia'
    ]
]

# Агломеративная иерархическая кластеризация с использованием метода Уорда
linked = linkage(features, method='ward')
clusters = fcluster(linked, t=4, criterion='maxclust')
df['cluster'] = clusters
cluster_counts = pd.Series(clusters).value_counts().sort_index()

# Сохранение результатов кластеризации в текстовый файл
with open("Agglomerative_clustering.txt", "w") as f:
    for cluster, count in cluster_counts.items():
        f.write(f"Кластер {cluster}: {count} пациентов\n")

    for cluster in sorted(df['cluster'].unique()):
        f.write(f"\nПациенты в кластере {cluster}:\n")
        cluster_users = df[df['cluster'] == cluster]['user_id'].tolist()
        f.write(", ".join(map(str, cluster_users)) + "\n")

# Обновлние базы данных, добавление метки кластеров для каждого пациента
for _, row in df.iterrows():
    user_id = row['user_id']
    cluster = row['cluster']
    cursor.execute("""UPDATE Classification SET cluster = %s WHERE user_id = %s""", (cluster, user_id))
mysql_connect.commit()

# Выполняется PCA и строится график кластеров в 2D пространстве
pca = PCA(n_components=2)
pca_features = pca.fit_transform(features)

plt.figure(figsize=(10, 7))
plt.scatter(pca_features[:, 0], pca_features[:, 1], c=clusters, cmap='viridis', marker='o')
plt.colorbar(label="Кластеры")
plt.title("Агломеративная кластеризация")
plt.xlabel("Главная компонента 1")
plt.ylabel("Главная компонента 2")

plt.savefig("Agglomerative_clustering.png")

plt.show()
