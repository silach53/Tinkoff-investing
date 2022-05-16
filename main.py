# код из джупитер ноутбука - и он запускал сторонний код
# !python table_generation.py

import data_parser
import pandas as pd
import requests as req
import httplib2
import googleapiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

# Смотрим к таблице с каким именем обращаться
data = pd.read_excel('tinkoffReport_' + data_parser.account_data['now_date'].strftime('%Y.%b.%d') + '.xlsx')


# Переводим один процент в 0,01
def rep(s, car):
    s = s.replace("%", "")
    t = s.find(car)
    s = s.replace(car, "")
    return "0," + '0' * (2 - t) + s


# Функция для получения заглавное n-вой английской буквы
def ex_alpha(n):
    return chr(ord('@') + n)


links = ['https://www.tinkoff.ru/invest/indexes/TCBRUS/structure/details/',
         'https://www.tinkoff.ru/invest/etfs/TSST/structure/details/',
         'https://www.tinkoff.ru/invest/etfs/TEMS/structure/details/',
         'https://www.tinkoff.ru/invest/etfs/TECH/structure/details/',
         'https://www.slickcharts.com/sp500',
         'https://www.tinkoff.ru/invest/etfs/TMOS/structure/details/',
         ]

# Словарь сопоставляющие название по тикеру и наоборот
Tikers = dict()
Names = dict()

with open("dict.txt", 'r', encoding='utf-8') as file:
    text = file.read().split("\n")

for x in text:
    # Унифицируем строки, для удобно обращаться
    x = x.split("\t")
    string = ""
    for y in x[:-1]:
        string += y

    string = string.replace(" ", "")

    Tikers[string] = x[-1]
    Names[x[-1]] = string


# Чтобы записать процент чистой прибыли, находим индексы двух строк и записываем их частное
idx_1 = data['Unnamed: 1'].to_list().index('Profit')
idx_2 = data['Unnamed: 1'].to_list().index('PayIn - PayOut')

# Для красоты, обозначим что это процент дохода
data['Unnamed: 1'][idx_1 + 1] = 'Percent'
data['Unnamed: 2'][idx_1 + 1] = str(
    round(100 * float(data['Unnamed: 2'][idx_1]) / float(data['Unnamed: 2'][idx_2]), 9)) + '%'

# Убираем nan и переводим в лист
data.fillna('', inplace=True)
investing_table = data.values.tolist()


# Запомним количество денег в каждом индексе, заодно заменив '.' на ','
# потому что эксель понимает, что это число только при ','
money = list(data['Unnamed: 11'][6:12])
for i in range(len(money)):
    money[i] = str(money[i]).replace(".", ",")


# Создаем таблицу состава индексов
data = pd.DataFrame()
num_col = 4

for link in links:
    # Обрабатываем индекс S&P500 - отдельно
    if link == 'https://www.slickcharts.com/sp500':
        # Притворяемся мозилой, чтобы нас пустил сайт
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 '
                                 '(KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        result = req.get(link, headers=headers)
        frame = pd.read_html(result.content.decode())[0]
        col_name = frame.columns[3]

        # Создадим столбец зависящий от значений в основной таблице, через формулу
        prices = []
        for i in range(len(frame[col_name])):
            frame[col_name][i] = rep(str(frame[col_name][i]), '.')
            prices.append(f"={ex_alpha(num_col - 2)}{i + 2}*{ex_alpha(num_col)}1")

        # Чтобы при выводе отличать индесы в таблице - преиминуем столбы
        frame.rename(columns={
            frame.columns[1]: 'TSPX' + "_" + frame.columns[1],
            frame.columns[2]: 'TSPX' + "_" + frame.columns[2],
            frame.columns[3]: 'TSPX' + "_" + frame.columns[3]}, inplace=True)

        # Присоединяем к таблице столбцы
        data = pd.concat([data, frame[frame.columns[1]], frame[frame.columns[3]], frame[frame.columns[2]],
                          pd.Series(prices, name=money[num_col // 4 - 1])], axis=1)
        num_col += 4
        continue

    # Обрабатываем все остальные индексы
    frame = pd.read_html(req.get(link).text)[0]
    col_name = frame.columns[-1]

    for i in range(len(frame[col_name])):
        frame[col_name][i] = rep(frame[col_name][i], ',')

    col_name = frame.columns[0]
    tiker = []
    prices = []

    for i in range(len(frame[col_name])):

        # Чистим данные с сайта
        frame[col_name][i] = frame[col_name][i].replace('Акция', '')
        frame[col_name][i] = frame[col_name][i].replace('АДР', '')
        frame[col_name][i] = frame[col_name][i].replace(' ', '')

        if frame[col_name][i] in Tikers:
            tiker.append(Tikers[frame[col_name][i]])
        else:
            tiker.append('NaaN')
            print(frame[col_name][i])

        prices.append(f"={ex_alpha(num_col - 2)}{i + 2}*{ex_alpha(num_col)}1")

    frame.rename(columns={frame.columns[0]: link.split("/")[5] + "_" + frame.columns[0],
                          frame.columns[1]: link.split("/")[5] + "_" + frame.columns[1]},
                 inplace=True)

    data = pd.concat(
        [data, frame[frame.columns[0]], frame[frame.columns[1]], pd.Series(tiker, name=f"{link.split('/')[5]}_Tikers"),
         pd.Series(prices, name=money[num_col // 4 - 1])], axis=1)

    num_col += 4

# Убираем nan и переводим в лист
data.fillna('', inplace=True)
new_table = [data.columns.to_list()] + [x for x in data.values.tolist()]

values = new_table
# Словарь для подсчета - сколько денег принадлежит каждому тикеру
COUNTER = dict()

for k in range(len(links)):
    for i in range(1, len(values)):
        if len(values[i]) < 4*k+2:
            break
        if values[i][4 * k + 2] == '':
            break
        if values[i][4 * k + 2] in COUNTER:
            COUNTER[values[i][4*k+2]] += float(values[i][4*k+3].replace(',', '.'))
        else:
            COUNTER[values[i][4*k+2]] = float(values[i][4*k+3].replace(',', '.'))

PORTFEL = []
for x in COUNTER:
    PORTFEL.append([COUNTER[x], x])
PORTFEL.sort()
PORTFEL.reverse()
ANS_PORTFEL = [['', '', '']]
for x in PORTFEL:
    ANS_PORTFEL.append([Names[x[1]], x[1], x[0]])


# Иницилизируем googleapi
CREDENTIALS_FILE = 'creds.json'
spreadsheet_id = '1jBf4mwvzHqzvZRNWhUwRgm3ghF0M6_FzzLDfsfTkFGg'
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = googleapiclient.discovery.build('sheets', 'v4', http=httpAuth)

# Запишем в таблицу результат работы программы
service.spreadsheets().values().batchUpdate(
    spreadsheetId=spreadsheet_id,
    body={
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": "Investing!A1:" + chr(ord('@') + len(investing_table[0])) + str(len(investing_table)),
             "majorDimension": "ROWS",
             "values": investing_table},
            {"range": "Indexes!A1:" + chr(ord('@') + len(new_table[0])) + str(len(new_table)),
             "majorDimension": "ROWS",
             "values": new_table},
            {"range": "PORTFEL!A1:C"+str(len(ANS_PORTFEL)),
             "majorDimension": "ROWS",
             "values": ANS_PORTFEL}
        ]
    }
).execute()

'''
Код использующий таблицу xx.xlsx - где для каждого тике есть соответсвие "имя индустрия сектор рынок страна"
аналогично вышенаписанному коду,через словари просто ведеться подсчет количество денег в каждой
из категорий - был оставлен на отформатированном ssd компьютера (
'''
