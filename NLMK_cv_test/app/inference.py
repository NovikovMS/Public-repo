import json
import os
import onnx
import onnxruntime as ort
import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException
from app.functions import *

# Запускаем FastAPI приложение
app = FastAPI()

# Класс для хранения модели и запуска инференса
class Model:
    def __init__(self, model_path: str):
        """
        Для запуска модели
        model_path: str Путь к модели
        """
        # Load the model from Registry
        self.session = ort.InferenceSession(f"{model_path}")
        self.inname = [input.name for input in self.session.get_inputs()]
        self.outname = [output.name for output in self.session.get_outputs()]

    def predict(self, data: np.array) -> np.ndarray:
        """
        Используем загруженную модель для предсказаний по входящим данным
        data: картика в виде np.array
        """
        predictions = self.session.run(self.outname,
                                       {self.inname[0]: data})

        return predictions[0]


# Create model
model = Model('./models/end2end_best.onnx')


# Создаем POST endpoint с путьм '/invocations'
@app.post("/invocations")
async def create_upload_file(file: UploadFile = File(...)):
    # Расширение должно быть только .jpg
    if file.filename.endswith(".jpg"):
        # Создаем временный файл с темже именем что и загруженный
        # JPG file to load the data into a np.array
        with open(file.filename, "wb")as f:
            f.write(file.file.read())
        data = process_image(file.filename)
        os.remove(file.filename)

        # возвращает JSON object содержащий предсказание модели
        inference = np.array(model.predict(data)).tolist()
        return inference

    else:
        # Возвращаем HTTP 400 Exception
        raise HTTPException(status_code=400, detail="Не правильный формат файла. Только JPG файлы.")

