# Версия рабочая от 16.09.2021 10:43
# Программа разбиения папки на папки нужного размера
# (например для дальнейшей записи на диски большого архива данных без компрессии)
# При разбиении каждая папка дозаписывается найденными файлами до нужного размера разделения
# Если попадаются файлы, которые больше размера деления, то они переносятся в отдельную
# директорию больших файлов
# Оконный интерфейс


import PySimpleGUI as sg
import time
import os.path
import os
from pathlib import Path
import glob
import psutil
import shutil
import threading
import re

# Переменные
starttime = 0  # Хранит время запуска потока разделения папки
flag_cut_process = False  # флаг остановки отсчета времени выполнения
stop_thread = False  # Флаг отсановки потока
lock = threading.Lock()
folder = ''  # выбранная папка (путь)
disk_name = ''  # выбранный диск
cut_size = 0  # Размер разделения
cut_size_b = 0  # Размер разделения в байтах


# Взятие текущего времени
def get_time_now():
    return time.strftime("%d.%m.%Y %H:%M:%S")


# Подсчет и вывод прогресса затраченного времени
def cur_use_time(starttime):
    curtime = time.time()
    progress_time = curtime - starttime
    window['-OUTPUT-'].update(
        'Прошло времени: {:02d}:{:02d}:{:02d}'.format(round(progress_time // 3600), round((progress_time // 60) % 60),
                                                      round(progress_time % 60)))


# Функция разделения папки (Передается в отдельный поток)
def func_cut_folder(path_folder_to_cut, cut_size_b, disk_name, iteration, window):
    # Переменная для сохранения размера наполняемой текущей папки деления
    summa = 0
    # Индекс папки деления
    index_folder = 1
    # выделяем название папки, которую будем делить из введеного пути
    name_folder_to_cut = path_folder_to_cut[path_folder_to_cut.rfind('/') + 1:]
    # Путь для рекурсивного прохода по всей папке деления и подпапкам и возврату всех путей
    path_s = path_folder_to_cut + "/**/*"
    # Длина пути папки для разделения
    len_path = len(path_folder_to_cut)
    # Словарь хранящий папки деления ключ = название + индекс папки деления, значение = размер папки
    dict_folder = {}
    # флаг хранит была ли запись в папку из словаря
    flag_w = False
    # Название папки с большими файлами
    big_files_folder = f"{disk_name}{name_folder_to_cut}_big_files"
    dst = ""
    tmp_dst = ""
    dst2 = ""
    dst3 = ""
    global stop_thread

    # Если искомая папка существует
    if (os.path.isdir(path_folder_to_cut)):
        # print(f"Разделение началось {get_time_now()}")
        window.write_event_value('-THREAD-',
                                 f"Разделение началось {get_time_now()}")  # put a message into queue for GUI
        # starttime = time.time()
        i = 0
        # Рекурсивно проходим по всем путям к подпапкам и файлам
        for path in glob.iglob(path_s, recursive=True):
            if stop_thread is False:
                # Если результирующей папки деления с индексом не существует, то создаем ее
                if not (os.path.isdir(disk_name + name_folder_to_cut + "_" + str(index_folder))):
                    os.makedirs(disk_name + name_folder_to_cut + "_" + str(index_folder))
                    # print(f"Папка {name_folder_to_cut}_{index_folder} создана")
                    # window.write_event_value('-THREAD-', f"Папка {name_folder_to_cut}_{index_folder} создана")  # put a message into queue for GUI
                    dict_folder[f"{name_folder_to_cut}_{str(index_folder)}"] = summa
                # else:
                # print(f"Папка {name_folder_to_cut}_{index_folder} существует")
                # window.write_event_value('-THREAD-', f"Папка {name_folder_to_cut}_{index_folder} существует")  # put a message into queue for GUI

                # Если путь это каталог, то в результирущей папке деления создаем этот каталог
                if (os.path.isdir(path)):
                    dst = disk_name + name_folder_to_cut + "_" + str(index_folder) + "\\" + str(path[(len_path + 1):])
                    # print(dst)
                    # window.write_event_value('-THREAD-', dst)  # put a message into queue for GUI
                    if not (os.path.isdir(dst)):
                        os.makedirs(dst)
                        # print("Папка назначения создана")
                        # window.write_event_value('-THREAD-', "Папка назначения создана")  # put a message into queue for GUI
                    # else:
                    # print("Папка назначения существует")
                    # window.write_event_value('-THREAD-', "Папка назначения существует")  # put a message into queue for GUI
                # если путь = файл
                else:
                    # print(path)
                    # window.write_event_value('-THREAD-', path)  # put a message into queue for GUI
                    # вычисляем размер файла
                    size = str(os.path.getsize(path))
                    # print(f'Размер файла вычислен: {int(size)/1048576} МБ')
                    # window.write_event_value('-THREAD-', f'Размер файла вычислен: {int(size)/1048576} МБ')  # put a message into queue for GUI

                    # Если размер файла больше размера деления
                    if (int(size) > cut_size_b):
                        # print("Файл больше размера деления")
                        # window.write_event_value('-THREAD-', "Файл больше размера деления")  # put a message into queue for GUI
                        # Создаем каталог больших файлов и копируем файл туда
                        # Если каталога больших фалов не существует, то создаем его
                        if not (os.path.isdir(big_files_folder)):
                            os.makedirs(big_files_folder)
                        # Получаем путь к большому файлу
                        tmp_dst = str(path[:path.rfind('\\')])
                        # Путь куда запишем большой файл
                        path_to_big = big_files_folder + "\\" + str(tmp_dst[(len_path + 1):])
                        # Если директории, куда пишем файл нет, то создаем ее
                        if not (os.path.isdir(path_to_big)):
                            os.makedirs(path_to_big)
                        shutil.copy2(path, path_to_big)
                        with open(f'{big_files_folder}\\bigfiles_in_folder.csv', 'a') as f1:
                            f1.write(f'{name_folder_to_cut}_big_files\\{path[(len_path + 1):]}\n')
                        # print("Большой файл скопирован")
                        # window.write_event_value('-THREAD-', "Большой файл скопирован")  # put a message into queue for GUI
                    else:
                        # Проходим по всем уже ранее созданным папкам деления для дозаписи в них
                        for key, summa in dict_folder.items():
                            # print(f"Размер папки {key} перед копированием файла: {summa/1048576} МБ")
                            # window.write_event_value('-THREAD-', f"Размер папки {key} перед копированием файла: {summa/1048576} МБ")  # put a message into queue for GUI
                            summa += int(size)

                            # Получаем путь к файлу полный без имени файла
                            tmp_dst = str(path[:path.rfind('\\')])
                            if (summa <= cut_size_b):
                                # print(f"Размер папки еще меньше {cut_size} Мб = {summa/1048576} МБ")
                                # window.write_event_value('-THREAD-', f"Размер папки еще меньше {cut_size} Мб = {summa/1048576} МБ")  # put a message into queue for GUI
                                # путь к папке с файлом для копирования
                                dst2 = disk_name + key + "\\" + str(tmp_dst[(len_path + 1):])
                                # print(f"dst2: {dst2}")
                                # window.write_event_value('-THREAD-', f"dst2: {dst2}")  # put a message into queue for GUI
                                if not (os.path.isdir(dst2)):
                                    os.makedirs(dst2)
                                    # print("Папка для копирования файла создана")
                                    # window.write_event_value('-THREAD-', "Папка для копирования файла создана")  # put a message into queue for GUI
                                # else:
                                # print("Папка для копирования файла существует")
                                # window.write_event_value('-THREAD-', "Папка для копирования файла существует")  # put a message into queue for GUI
                                with open(f'{disk_name}{key}\\files_in_folder.csv', 'a') as f1:
                                    f1.write(f'{key}\\{path[(len_path + 1):]}\n')
                                shutil.copy2(path, dst2)
                                # print("Файл скопирован")
                                # window.write_event_value('-THREAD-', "Файл скопирован")  # put a message into queue for GUI
                                dict_folder[key] = summa
                                # устанавливаем флаг true так как была запись файла в одну из папок в словаре
                                flag_w = True
                                # прерываем перебор словаря, так как записали файл
                                break
                            else:
                                # print(f"файл не помещается в папку {key}")
                                # window.write_event_value('-THREAD-', f"файл не помещается в папку {key}")  # put a message into queue for GUI
                                flag_w = False
                                continue
                        else:
                            if flag_w is False:
                                # print(f"Размер папки превысил {cut_size} Мб")
                                # window.write_event_value('-THREAD-', f"Размер папки превысил {cut_size} Мб")  # put a message into queue for GUI
                                index_folder += 1
                                if not (os.path.isdir(disk_name + name_folder_to_cut + "_" + str(index_folder))):
                                    os.makedirs(disk_name + name_folder_to_cut + "_" + str(index_folder))
                                    # print(f"Папка {name_folder_to_cut}_{index_folder} создана")
                                    # window.write_event_value('-THREAD-', f"Папка {name_folder_to_cut}_{index_folder} создана")  # put a message into queue for GUI
                                # else:
                                # print(f"Папка {name_folder_to_cut}_{index_folder} уже существует")
                                # window.write_event_value('-THREAD-', f"Папка {name_folder_to_cut}_{index_folder} уже существует")  # put a message into queue for GUI
                                dst3 = disk_name + name_folder_to_cut + "_" + str(index_folder) + "\\" + str(
                                    tmp_dst[(len_path + 1):])
                                # print(f"dst3: {dst3}")
                                # window.write_event_value('-THREAD-', f"dst3: {dst3}")  # put a message into queue for GUI
                                if not (os.path.isdir(dst3)):
                                    os.makedirs(dst3)
                                    # print("Папка для копирования файла создана")
                                    # window.write_event_value('-THREAD-', "Папка для копирования файла создана")  # put a message into queue for GUI
                                # else:
                                # print("Папка для копирования файла существует")
                                # window.write_event_value('-THREAD-', "Папка для копирования файла существует")  # put a message into queue for GUI
                                with open(f'{disk_name}{name_folder_to_cut}_{str(index_folder)}\\files_in_folder.csv',
                                          'a') as f1:
                                    f1.write(f"{name_folder_to_cut}_{str(index_folder)}\\{path[(len_path + 1):]}\n")
                                shutil.copy2(path, dst3)
                                # print("Файл скопирован")
                                # window.write_event_value('-THREAD-', "Файл скопирован")  # put a message into queue for GUI
                                summa = int(size)
                                dict_folder[f"{name_folder_to_cut}_{str(index_folder)}"] = summa
                i += 1
                ii = i / iteration * 100  # Размер текущего заполнения прогресс бара (от 0 до 100)
                # print(f"итерация: {i}")
                progress_bar.UpdateBar(ii)
            else:
                window.write_event_value('-THREAD-END-',
                                         f"Процесс остановлен: {get_time_now()}")  # put a message into queue for GUI
                break
        # создаем файл описание всех файлов описаний папок деления
        # Проходим по всем папкам деления, копируем пути из каждого файла описания папки деления в результирующий
        # удаляем файл описание папки деления
        # Копируем результирующий файл описания со всеми путями в каждую папку деления
        if stop_thread is False:  # Если поток не был остановлен в процессе
            # Создаем файл описи всех файлов по папкам
            with open(f'{disk_name}All_files_in_folders_{name_folder_to_cut}.csv', 'a') as f1:
                for key in dict_folder.keys():
                    with open(f'{disk_name}{key}\\files_in_folder.csv', 'r') as f2:
                        f1.write(f'{key}\n')
                        string_f2 = f2.read()
                        f1.write(string_f2 + '\n')
                    os.remove(f'{disk_name}{key}\\files_in_folder.csv')

            # Если существует папка с большими файлами, то в общий файл описи добавляем опись файлов большой папки
            if (os.path.isdir(big_files_folder)):
                with open(f'{disk_name}All_files_in_folders_{name_folder_to_cut}.csv', 'a') as f1:
                    with open(f'{big_files_folder}\\bigfiles_in_folder.csv', 'r') as f3:
                        f1.write(f'{name_folder_to_cut}_big_files\n')
                        string_f3 = f3.read()
                        f1.write(string_f3 + '\n')
                    os.remove(f'{big_files_folder}\\bigfiles_in_folder.csv')
                shutil.copy2(f'{disk_name}All_files_in_folders_{name_folder_to_cut}.csv', big_files_folder)

            # Раскидываем общий файл описи по всем папкам разделения
            for key in dict_folder.keys():
                shutil.copy2(f'{disk_name}All_files_in_folders_{name_folder_to_cut}.csv', disk_name + key)

            os.remove(
                f'{disk_name}All_files_in_folders_{name_folder_to_cut}.csv')  # Удалить итоговый файл из корня диска (он уже есть во всех папках)
            # print(f"Разделение завершено {get_time_now()}")
            window.write_event_value('-THREAD-END-',
                                     f"Разделение завершено {get_time_now()}")  # put a message into queue for GUI
        window.write_event_value('-THREAD-', f"Поток завершился {get_time_now()}")  # put a message into queue for GUI
        window.write_event_value('-THREAD-', f"Задача завершена {get_time_now()}")  # put a message into queue for GUI
        # Сбрасываем флаг завершения потока
        lock.acquire()
        stop_thread = False
        lock.release()
    else:
        print(f'Не доступен сетевой путь: {path_folder_to_cut}')


# Вычисляет размер папки, количество файлов и количество итераций функции
def folderSize(path):
    fsize = 0
    numfile = 0
    iteration = 0
    for file in Path(path).rglob('*'):
        if (os.path.isfile(file)):
            fsize += os.path.getsize(file)
            numfile += 1
        iteration += 1
    return fsize, numfile, iteration


# *******************************************************GUI*****************************************************
disk = []  # Список дисков для сохранения разделения
numfile = 100  # Размерность прогресс бара (100%)
list_sizes = [('CD', 700), ('DVD', 4400)]

progressbar = [
    [sg.ProgressBar(numfile, orientation='h', size=(54, 10), key='progressbar')]
]
outputwin = [
    [sg.Output(size=(82, 20))]
]

layout = [
    [sg.Text('Папка для разделения:'), sg.In(size=(50, 1), enable_events=True, key='T_FOLDER'),
     sg.FolderBrowse('Выбор папки', key='FB_FOLDER')],
    [sg.Text('Список дисков с достаточным местом для разделения:')],
    [sg.Combo(values=disk, size=(10, 1), key='LB_DISK', enable_events=True, readonly=True)],
    [sg.Text('Размер разделения:'), sg.Combo(values=list_sizes, key='INPUT_SIZE', size=(10, 1), enable_events=True),
     sg.Text('МБ (Введите или выберите размер деления)')],
    [sg.Frame('Консоль вывода', layout=outputwin)],
    [sg.Frame('Прогресс', layout=progressbar)],
    [sg.Submit('Запуск разделения', key='START'), sg.Submit('Стоп', key='CANCEL', disabled=True),
     sg.Text(size=(30, 1), key='-OUTPUT-', justification='right'), sg.Cancel('Закрыть', key='CLOSE')]
]

window = sg.Window('Разделение папки на части', layout)
progress_bar = window['progressbar']

while True:
    event, values = window.read(timeout=0.001)
    if event in (sg.WIN_CLOSED, 'CLOSE'):
        break
    elif event == 'T_FOLDER':
        # Очищаем список дисков после нового выбора папки
        disk.clear()
        folder = values['T_FOLDER']  # Путь к папке в поле выбора папки

        # ************Преобразуем адрес типа u:\ в u:/
        raw_s = r'{}'.format(folder)  # преобразуем в сырую строку
        match = re.search(r'[a-zA-Z]:\\', raw_s)  # ищем регулярное выражение (типа u:\)
        if match:  # если подстрока вида u:\ нашлась
            folder = folder[:2] + "/"  # меняем на вид u:/
        # *******************************************************

        # Если папка существует
        if (os.path.isdir(folder) and len(folder) > 2):
            print("\nВычисление размера выбранной папки...")
            size, numfile, iteration = folderSize(folder)
            print("\n***********************************************")
            print(f'Выбрана папка: {folder}')
            print(f'Найдено файлов: {numfile}')
            print("Размер папки:")
            print(f'{size} Bytes')
            print(f'{size / 1048576:.2f} Mb')
            print(f'{size / 1073741824:.2f} Gb')

            # Определяем список дисков логических с файловой системой NTFS
            drps = psutil.disk_partitions()
            disk_names = [dp.device for dp in drps if dp.fstype == 'NTFS']
            # print(disk_names)
            # Определяем размер найденных дисков
            disk_us = list(map(lambda x: psutil.disk_usage(x), disk_names))
            # print(disk_us)

            print("\nСписок дисков с достаточным местом для разделения:")
            for name, sized in zip(disk_names, disk_us):
                if sized.free > size:
                    # Исключаем диски без прав на запись файлов
                    try:
                        with open(f'{name}test.txt', 'a') as f:
                            print(f'{name} = {sized.free / 1024 / 1024:.2f} МБ')
                            disk.append(name)
                        os.remove(f'{name}test.txt')
                    except:
                        print(f'Диск {name} исключен. Нет прав на запись файлов')
            # Выводим список найденных дисков в интерфейсе
            window['LB_DISK'].update(values=disk)
            if len(disk) == 0:
                print("Нет ни одного диска с достаточным свободным местом для разделения выбранной папки!")
        else:
            print("Выбранной папки не существует или нет доступа")
            folder = ''
            disk_name = ''

    elif event == 'LB_DISK':
        if values['LB_DISK'] != []:
            disk_name = values['LB_DISK']
            print(f"Выбран диск: {disk_name}")

    elif event == 'INPUT_SIZE':
        cut_size = values['INPUT_SIZE'][1]
        cut_size_b = (cut_size - 1) * 1048576  # Отнимаем мегабайт для файлика с описью
        # print(f"Размер разделения: {cut_size} МБ")
        window['INPUT_SIZE'].update(values['INPUT_SIZE'][1])

    elif event == 'START':
        try:
            # Проверка введенного размера на целое число и присвоение переменной
            cut_size = int(values['INPUT_SIZE'])
            cut_size_b = (cut_size - 1) * 1048576

            # Если все поля заполнены и размер деления не равен 0, то запускаем процесс разделения в отдельном потоке
            if folder != '' and disk_name != '' and window['INPUT_SIZE'] != '' and cut_size > 0:
                progress_bar.UpdateBar(0)
                print(f"Размер разделения: {cut_size} МБ")
                # Запуск отдельного потока для обработки папки
                flag_cut_process = True  # Флаг запуска функции отсчета времени работы потока
                starttime = time.time()
                threading.Thread(target=func_cut_folder, args=(folder, cut_size_b, disk_name, iteration, window,),
                                 daemon=True).start()
                print(f"Поток запущен {get_time_now()}")
                # Обновдение элементов интерфейса
                window['START'].update(disabled=True)
                window['LB_DISK'].update(disabled=True)
                window['T_FOLDER'].update(disabled=True)
                window['INPUT_SIZE'].update(disabled=True)
                window['CANCEL'].update(disabled=False)
                window['FB_FOLDER'].update(disabled=True)
            else:
                print("Запуск не возможен. Выбраны не все поля или ошибки ввода")

        except:
            print(f'Некорректный ввод размера разделения!')
            cut_size = 0

    elif event == 'CANCEL':
        # Останавливаем поток по разделению папки
        lock.acquire()
        stop_thread = True
        lock.release()

    elif event == '-THREAD-':  # Выводим сообщения от потока
        print(values['-THREAD-'])

    # Если пришло событие от потока о завершении
    elif event == '-THREAD-END-':
        print(values['-THREAD-END-'])
        flag_cut_process = False  # Останавливаем таймер работы потока
        # Обновляем элементы интерфейса
        window['FB_FOLDER'].update(disabled=False)
        window['START'].update(disabled=False)
        window['LB_DISK'].update(readonly=True)
        window['LB_DISK'].update(values=[])
        disk_name = ''
        window['T_FOLDER'].update(disabled=False)
        window['T_FOLDER'].update('')
        folder = ''
        window['INPUT_SIZE'].update(disabled=False)
        window['INPUT_SIZE'].update('')
        cut_size = 0
        window['CANCEL'].update(disabled=True)

    # Если поток разделения запущен, то ведем отсчет времени его работы
    if flag_cut_process is True:
        cur_use_time(starttime)

window.close()
