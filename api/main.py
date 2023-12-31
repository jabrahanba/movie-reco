import APIfunctions as f
from fastapi import FastAPI, HTTPException

#Definir la aplicación
app = FastAPI()

#http://127.0.0.1:8000 ruta raiz local


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
async def consulta5(name_actor: str):
    try:
        return await f.get_actor(name_actor)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# --------------------------------------------
#Esta la hice adicionalmente para devolver las peliculas por actor sin contar en las que el actor fue director también.
'''@app.get("/get_actor2/{name_actor}")
async def consulta5_2(name_actor: str):
    try:
        return await f.get_actor2(name_actor)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))'''
    
# --------------------------------------------

@app.get("/get_director/{director}")
async def consulta6(director: str):
    try:
        return await f.get_director(director)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
# --------------------------------------------

@app.get("/recomendacion/{title}")
async def consulta7(title: str):
    try:
        return await f.recomendacion(title)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))