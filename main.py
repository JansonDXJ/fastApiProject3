import pyodbc as pyodbc
import pyodc
import uvicorn
from fastapi import FastAPI, Header, Body
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from models import StoredToDB
from datetime import datetime


"""set as a APP"""
app = FastAPI()

"""global constant"""
server = 'xiaojie.database.windows.net'
database = 'ml'
username = 'xiaojieding'
password = '*****'
driver = 'SQL Server Native Client 11.0'

@app.get("/metadata")
async def getMetaData():
    """
    The training model will provide API about metadata info, I will call this API to get current model modelName, version, startTrainingTime, endTrainingTime, trainingImageCount,
    Then store all info into DB: ModelMetaDataDB, exclude duplicate model info and return Jason
    """
    modelName, version, startTrainingTime, endTrainingTime, trainingImageCount = DummyAPI()
    dic = dict()
    exclude_duplicate = set()
    sql = "SELECT ModelName,Version, StartTrainingTime, EndTrainingTime, TrainingImageCount FROM dbo.ModelMetaDataDB"
    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
        with conn.cursor() as cursor:
            try:
                data = cursor.execute(sql)
                for d in data:
                    ModelName, Version, StartTrainingTime, EndTrainingTime, TrainingImageCount = d
                    if str(ModelName+Version) not in exclude_duplicate:
                        exclude_duplicate.add(str(ModelName+Version))
                        dic[str(ModelName+Version)].append([ModelName, Version, StartTrainingTime, EndTrainingTime, TrainingImageCount])
                return JSONResponse(content=dic, status_code=200, headers=None)
            except pyodbc.OperationalError as e:
                app.logger.err(f"{e.args[1]}")
                raise
            finally:
                cursor.close()

@app.post("/train")
async def trainModel():
    """
    Firstly, extracting all images from DB: TrainingPhotosDB,
    Then call training API to training.
    dummy API for CallTrainingAPI(ImageBinary, Label)
    """
    sql = "SELECT ImageBinary, label FROM dbo.TrainingPhotosDB"
    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
        with conn.cursor() as cursor:
            try:
                data = cursor.execute(sql)
                for d in data:
                    ImageBinary, Label = d
                    CallTrainingAPI(ImageBinary, Label)
                return JSONResponse(content={"Result": "Training Succeed"}, status_code=200, headers=None)
            except pyodbc.OperationalError as e:
                app.logger.err(f"{e.args[1]}")
                raise
            finally:
                cursor.close()

@app.post("/predict")
async def predictModel(ImageBinary):
    """
    Firstly, will call training API will a image and get a predict result with label,
    Then stored result at DB: PredictDB
    dummy API for CallPredictAPI(ImageBinary)
    """
    ImageBinary, Label, ModelName, Version = CallPredictAPI(ImageBinary)
    TrainingTime = datetime.now()
    sql = "INSERT ImageBinary, Label INTO dbo.PredictDB ([ImageBinary], [Label], [TrainingTime]) VALUE(?, ?, ?)"
    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
        with conn.cursor() as cursor:
            try:
                data = cursor.execute(sql, ImageBinary, Label, ModelName, Version, TrainingTime)
                return JSONResponse(content={"ImageBinary": Label}, status_code = 200, headers=None)
            except pyodbc.OperationalError as e:
                app.logger.err(f"{e.args[1]}")
                raise
            finally:
                cursor.close()
@app.get("/history")
async def getPredictHistory():
    """
    Extraing all predict result from DB: PredictDB
    """
    sql = "SELECT ImageBinary, Label, ModelName, Version, TrainingTime"
    dic = dict()
    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
        with conn.cursor() as cursor:
            try:
                data = cursor.execute(sql)
                for d in data:
                    ImageBinary, Label, ModelName, Version, TrainingTime = d
                    dic[ImageBinary].append([Label, ModelName, Version, TrainingTime])
                return JSONResponse(content=dic, status_code = 200, headers=None)
            except pyodbc.OperationalError as e:
                app.logger.err(f"{e.args[1]}")
                raise
            finally:
                cursor.close()

if __name__ == '__main__':
    obj = StoredToDB()
    obj.storePhotoDB()
    uvicorn.run(app)
