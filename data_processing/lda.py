import pandas as pd
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis

data = pd.read_excel('lda-data.xlsx', sheet_name = 'Sheet1')
data = data.drop('user_id', axis=1)
data.head()

# Разделение выборки на две части
X = data.drop('cluster', axis=1)
y = data['cluster']

# Создание модели LDA
lda = LinearDiscriminantAnalysis()

# Обучение модель на данных
lda.fit(X, y)

# Получение коэффициентов дискриминантной функции и свободного члена
coefficients = lda.coef_
intercept = lda.intercept_
print(coefficients)
print(intercept)

# Запись в excel-file
coefficients = pd.DataFrame(coefficients, columns=X.columns)
coefficients.to_excel('Коэффициенты дискриминантной функции.xlsx')