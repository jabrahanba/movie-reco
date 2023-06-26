import APIfunctions as f
from fastapi import FastAPI, HTTPException

#Definir la aplicación
app = FastAPI()

#http://127.0.0.1:8000 ruta raiz local

'''#Pruebas SI QUEDA TIEMPO PROBAR ESTO SINO BORRAR:

debo agregar:
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"mensaje": "No se puede acceder a la API en este momento o k ase."}
    )

@app.get("/mi-api")
def mi_api():
    raise HTTPException(status_code=500)'''


# --------------------------------------------

@app.get("/")
def index():
    return {"message":"Ola k ase"}

# --------------------------------------------

@app.get("/cantidad_filmaciones_mes/{mes}")
async def consulta1(mes: str):
    try:
        return await f.cantidad_filmaciones_mes(mes)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# --------------------------------------------

@app.get("/cantidad_filmaciones_dia/{dia}")
async def consulta2(dia: str):
    try:
        return await f.cantidad_filmaciones_dia(dia)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# --------------------------------------------

@app.get("/score_titulo/{titulo}")
async def consulta3(titulo: str):
    try:
        return await f.score_titulo(titulo)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# --------------------------------------------

@app.get("/votos_titulo/{titulo}")
async def consulta4(titulo: str):
    try:
        return await f.votos_titulo(titulo)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# --------------------------------------------

@app.get("/get_actor/{name_actor}")
async def consulta5_1(name_actor: str):
    try:
        return await f.get_actor(name_actor)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# --------------------------------------------

@app.get("/get_actor2/{name_actor}")
async def consulta5_2(name_actor: str):
    try:
        return await f.get_actor2(name_actor)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
# --------------------------------------------

@app.get("/get_director/{director}")
async def consulta6(director: str):
    try:
        return await f.get_director(director)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))