# folder_cutter
This program divides a folder by size into subfolders with file paths description

Небольшая программка с интуитивно понятным оконным интерфейсом, которая производит деление каталога с файлами на подкаталоги заданного размера без компрессии (например для дальнейшей записи на оптические диски большого архива данных разделенного на части без компрессии). При разбиении каждая папка дозаписывается найденными файлами до нужного размера разделения. Если попадаются файлы, которые больше размера деления, то они переносятся в отдельную директорию больших файлов. В каждую папку-часть по итогу работы программы записывается файл с описанием файлов и путей к ним в каждой части деления. Если вручную произвести слияние частей, то на выходе получим исходный каталог со всей структурой что была до разделения.

Есть некоторые мысли по ее доработке. По все вопросам и предложениям пишите на vladimir.mva@gmail.com
