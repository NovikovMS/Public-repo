import numpy as np
import cv2

def read_image(img_path: str) -> np.array:
    """
    Читает картинку в np.array
    :param img_path: путь к картинке
    :return:
    """
    # cv2 читает в BGR
    image = cv2.imread(str(img_path))
    return image

def resize_image(img: np.array, max_dim_size: int) -> np.array:
    """
    Сжимает пропорционально картинку, чтобы большая сторона стала равна max_dim_size
    :param img: картинка в np.array
    :param max_dim_size: int
    :return:
    """
    image = np.array(img.copy())
    height, width, colour_layers = image.shape[:3]

    if width > height:
        scale_percent = max_dim_size / width
        new_width = max_dim_size
        new_height = int(height * scale_percent)
    if width <= height:
        scale_percent = max_dim_size / height
        new_width = int(width * scale_percent)
        new_height = max_dim_size

    return cv2.resize(image, (new_width, new_height))


def pad_image(img: np.array, background_color: tuple[int]) -> np.array:
    """
    Относительно центра картинки добавляет поля
    в высоту или ширину делая кадр квадратным
    """
    image = np.array(img.copy())
    height, width, colour_layers = image.shape[:3]

    max_dimm = max(height, width)
    new_height = int(max_dimm)
    new_width = int(max_dimm)

    # пустой кадр, заполненный фоном, куда потом вставим изображение
    empty_frame = np.full((new_height,
                           new_width,
                           colour_layers),
                          background_color,
                          dtype='uint8')

    pad_height = int((new_height - height) / 2)
    pad_width = int((new_width - width) / 2)

    # ищем координаты для вставки
    # в numpy нумерация с верхнего левого угла...
    # потому если мы добавляем сверху, координаты не меняются.
    if pad_height >= 0:
        y0 = 0 + pad_height
        y1 = y0 + height
    if pad_height < 0:
        y0 = 0
        y1 = y0 + height

    if pad_width >= 0:
        x0 = 0 + pad_width
        x1 = x0 + width
    if pad_width < 0:
        x0 = 0
        x1 = x0 + width

    # вставили картинку
    empty_frame[y0:y1, x0:x1] = image

    result = empty_frame

    return result

def process_image(img_path: str) -> np.array:
    """
    Пайплайн обработки зиображения
    :param img_path: путь к картинке
    :return: np.array
    """
    img = read_image(img_path)  # прочитали
    img = resize_image(img, 640)  # сжали/растянули
    img = pad_image(img, (114, 114, 114))  # сделали падинг фоном до квадрата
    img = img.astype(np.float32)  # привели к float32
    # img = cv2.normalize(img, None, 0, 1, cv2.NORM_MINMAX)  # предположение, цвета при обучении были нормализованы
    img = np.transpose(img, (2, 0, 1))  # приводим к виду [idx, colour, h, w]
    img = np.expand_dims(img, axis=0)
    return img
