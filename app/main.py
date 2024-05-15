import fastapi
from yoyo import read_migrations, get_backend
from .sensors.controller import router as sensorsRouter

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

migrations = read_migrations("migrations_ts")
backend = get_backend("postgres://timescale:timescale@timescale:5433/timescale")
backend.apply_migrations(backend.to_apply(migrations))
app.include_router(sensorsRouter)

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}