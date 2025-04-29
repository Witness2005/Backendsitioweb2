import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
from io import StringIO
import psycopg2
from psycopg2 import sql
import logging
from datetime import datetime
from typing import Optional, List, Dict
import uvicorn

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("birth_rate_api")

# Inicializar FastAPI
app = FastAPI(
    title="API de Tasas de Natalidad Mundial",
    description="Extrae y muestra datos de tasas de natalidad de Our World in Data",
    version="1.0.0"
)

# Configuración CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# URL del dataset
CSV_URL = "https://ourworldindata.org/grapher/crude-birth-rate.csv?v=1&csvType=full&useColumnShortNames=false"

class DatabaseManager:
    """Manejador de conexión y operaciones con PostgreSQL"""
    
    def __init__(self):
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establece conexión con la base de datos"""
        try:
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                port=os.getenv('DB_PORT', '5432')
            )
            logger.info("Conexión exitosa con PostgreSQL")
        except Exception as e:
            logger.error(f"Error al conectar con PostgreSQL: {str(e)}")
            self.conn = None
    
    def ensure_connection(self):
        """Verifica y restablece la conexión si es necesario"""
        if self.conn is None or self.conn.closed:
            self.connect()
    
    def create_table(self, table_name: str, columns: List[Dict[str, str]]):
        """
        Crea la tabla si no existe
        
        Args:
            table_name: Nombre de la tabla
            columns: Lista de diccionarios con 'name' y 'type' para cada columna
        """
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cursor:
                # Preparar definiciones de columnas
                columns_def = [
                    sql.SQL("{} {}").format(
                        sql.Identifier(col['name']),
                        sql.SQL(col['type'])
                    ) for col in columns
                ]
                
                # Crear tabla
                query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    {columns},
                    import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """).format(
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(', ').join(columns_def)
                )
                
                cursor.execute(query)
                self.conn.commit()
                logger.info(f"Tabla '{table_name}' creada/verificada")
                return True
                
        except Exception as e:
            logger.error(f"Error al crear tabla: {str(e)}")
            self.conn.rollback()
            return False
    
    def insert_data(self, table_name: str, data: List[Dict], columns: List[str]):
        """
        Inserta datos en la tabla
        
        Args:
            table_name: Nombre de la tabla
            data: Lista de diccionarios con los datos
            columns: Lista de nombres de columnas
        """
        if not self.conn or not data:
            return False
        
        try:
            with self.conn.cursor() as cursor:
                # Preparar consulta
                cols = sql.SQL(', ').join(map(sql.Identifier, columns))
                placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(columns))
                
                query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({values})
                """).format(
                    table=sql.Identifier(table_name),
                    columns=cols,
                    values=placeholders
                )
                
                # Preparar datos para inserción
                rows = [
                    tuple(row[col] for col in columns)
                    for row in data
                ]
                
                # Insertar por lotes
                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    cursor.executemany(query, batch)
                    self.conn.commit()
                    logger.info(f"Insertados {len(batch)} registros en '{table_name}'")
                
                return True
                
        except Exception as e:
            logger.error(f"Error al insertar datos: {str(e)}")
            self.conn.rollback()
            return False
    
    def close(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
            logger.info("Conexión con PostgreSQL cerrada")

# Instancia global del manejador de base de datos
db_manager = DatabaseManager()

def fetch_csv_data() -> Optional[pd.DataFrame]:
    """Descarga y procesa los datos CSV"""
    try:
        logger.info(f"Descargando datos desde: {CSV_URL}")
        response = requests.get(CSV_URL, timeout=10)
        response.raise_for_status()
        
        # Procesar CSV
        df = pd.read_csv(StringIO(response.text))
        
        # Limpieza de nombres de columnas
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        logger.info(f"Datos descargados: {df.shape[0]} filas, {df.shape[1]} columnas")
        return df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al descargar CSV: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado al procesar datos: {str(e)}")
        return None

def save_to_database(df: pd.DataFrame, table_name: str = "birth_rates") -> bool:
    """Guarda los datos en PostgreSQL"""
    if df is None or df.empty:
        return False
    
    try:
        # Convertir DataFrame a lista de diccionarios
        data = df.to_dict(orient='records')
        
        # Definir columnas y tipos
        columns = [
            {'name': 'entity', 'type': 'TEXT'},
            {'name': 'code', 'type': 'TEXT'},
            {'name': 'year', 'type': 'INTEGER'},
            {'name': 'crude_birth_rate', 'type': 'NUMERIC'}
        ]
        
        # Crear tabla si no existe
        if not db_manager.create_table(table_name, columns):
            return False
        
        # Insertar datos
        column_names = [col['name'] for col in columns]
        return db_manager.insert_data(table_name, data, column_names)
    
    except Exception as e:
        logger.error(f"Error al guardar en base de datos: {str(e)}")
        return False

@app.get("/")
def read_root():
    """Endpoint raíz con información básica"""
    return {
        "message": "API de Tasas de Natalidad Mundial",
        "endpoints": {
            "/data": "Obtener datos de tasas de natalidad",
            "/countries": "Lista de países disponibles",
            "/refresh": "Actualizar datos desde la fuente"
        },
        "source": CSV_URL
    }

@app.get("/data")
async def get_birth_rate_data(
    country: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None
):
    """
    Obtiene datos de tasas de natalidad
    
    Parámetros:
    - country: Filtrar por país
    - year_from: Año inicial
    - year_to: Año final
    """
    try:
        # Descargar datos directamente (o podrías leer de la base de datos)
        df = fetch_csv_data()
        if df is None:
            raise HTTPException(status_code=500, detail="Error al obtener datos")
        
        # Aplicar filtros
        if country:
            df = df[df['entity'].str.lower() == country.lower()]
        if year_from:
            df = df[df['year'] >= year_from]
        if year_to:
            df = df[df['year'] <= year_to]
        
        return df.to_dict(orient='records')
    
    except Exception as e:
        logger.error(f"Error en endpoint /data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/countries")
async def get_countries_list():
    """Obtiene la lista de países disponibles"""
    try:
        df = fetch_csv_data()
        if df is None:
            raise HTTPException(status_code=500, detail="Error al obtener datos")
        
        countries = sorted(df['entity'].unique().tolist())
        return {"countries": countries, "count": len(countries)}
    
    except Exception as e:
        logger.error(f"Error en endpoint /countries: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/refresh")
async def refresh_data(save_to_db: bool = False):
    """
    Actualiza los datos desde la fuente original
    
    Parámetro:
    - save_to_db: Si es True, guarda los datos en PostgreSQL
    """
    try:
        df = fetch_csv_data()
        if df is None:
            raise HTTPException(status_code=500, detail="Error al obtener datos")
        
        result = {"message": "Datos actualizados correctamente", "rows": len(df)}
        
        if save_to_db and os.getenv('DB_HOST'):
            if save_to_database(df):
                result["database"] = "Datos guardados en PostgreSQL"
            else:
                result["database"] = "Error al guardar en PostgreSQL"
        
        return result
    
    except Exception as e:
        logger.error(f"Error en endpoint /refresh: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("shutdown")
def shutdown_event():
    """Cerrar conexión a la base de datos al apagar la aplicación"""
    db_manager.close()

if __name__ == "__main__":
    # Configuración para desarrollo local
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)