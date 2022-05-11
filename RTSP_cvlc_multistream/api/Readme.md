Мой шаблон API для запуска ML моделей в контейнере.

Построен на Fast Api и React
Имеет встроенную бд (живет пока работает unicorn или живет контейнер)

Требования:
> python 3.9
> и все что находится в requirements.txt

Запуск:
в UI или из консоли 
> uvicorn main:app --reload --host 0.0.0.0 --port 5089 

или как контейнер
> docker build $path
> docker run -p 5089:5089 $image_name


Доки в браузере после запуска: 
>http://127.0.0.1:5089/docs#/  
>или  
>http://127.0.0.1:5089/redoc