import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
import matplotlib.pyplot as plt

data = pd.read_excel('logreg-data.xlsx', sheet_name = 'Sheet1')
data = data.drop('user_id', axis=1)
data.head()

# Разделяем данные на набор признаков и целевую переменную
X = data.drop(['5<glucose<6', '6<glucose<7', '7<glucose<8', '8<glucose'], axis=1)
y = data['6<glucose<7']

# Создание модели логистической регрессии
logreg = LogisticRegression(max_iter=10000)

# Обучение модели на обучающей выборке
logreg.fit(X, y)
# Получим веса модели
weights = logreg.coef_[0]
print(weights)

# Получение свободного члена
bias = logreg.intercept_
bias

# Получим названия всех признаков (анализы)
columns = X.columns
values = []

# Ввод значения каждого признака и добавляем его в список values
for i in columns:
    value = float(input(f"{i} = "))
    values.append(value)

# Преобразуем в массив numpy
values = np.array(values)

# Предсказываем вероятности классов для тестовых данных
y_proba = logreg.predict_proba([values])

# Выводим вероятности отнесения объектов к классу 1
print('Вероятность 6<glucose<7 =', np.round(y_proba[:, 1], 10), '\n')

def sigmoid(x):
  return (1 / (1 + np.exp(-x)))

# Создадим массив значений
x = np.arange(-5, 5, 0.1)
# Вычислим значения сигмоидной функции для каждого значения x
y = sigmoid(x)

# Построение графика сигмоидной функции
plt.plot(x, y, color = '#4c72b0', linewidth=2.5)
plt.xlabel('Значение уравнения регрессии')
plt.ylabel('Вероятность')
plt.grid(True)
plt.savefig('Sigmoid Function.png', dpi=300, bbox_inches='tight')
plt.show()