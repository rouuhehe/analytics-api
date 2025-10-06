from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from .db import get_pg_db, get_mysql_db, histories_collection

router = APIRouter(prefix="/analytics")

# Verificar tablas existentes
@router.get("/check-tables")
def check_existing_tables(db: Session = Depends(get_pg_db)):
    """Verifica qué tablas existen en la base de datos"""
    try:
        query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        result = db.execute(query)
        tables = [row[0] for row in result]
        
        # También verificar vistas
        view_query = text("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        view_result = db.execute(view_query)
        views = [row[0] for row in view_result]
        
        return {
            "tables": tables,
            "views": views,
            "total_tables": len(tables),
            "total_views": len(views)
        }
    except Exception as e:
        return {"error": f"Error al verificar tablas: {str(e)}"}

# Verificar estructura de tablas
@router.get("/check-table-structure")
def check_table_structure(db: Session = Depends(get_pg_db)):
    """Verificar la estructura de las tablas"""
    try:
        # Verificar que existan las tablas principales
        tables_query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('pet', 'adoption_centers', 'adoption_status', 'vaccines')
            ORDER BY table_name;
        """)
        
        tables = db.execute(tables_query).fetchall()
        table_names = [row[0] for row in tables]
        
        # Verificar columnas de cada tabla
        table_info = {}
        for table_name in table_names:
            columns_query = text(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' 
                AND table_schema = 'public'
                ORDER BY ordinal_position;
            """)
            columns = db.execute(columns_query).fetchall()
            table_info[table_name] = [{"column": row[0], "type": row[1], "nullable": row[2]} for row in columns]
        
        return {
            "available_tables": table_names,
            "table_structures": table_info,
            "message": "Tablas listas para consultas directas con JOINs"
        }
        
    except Exception as e:
        return {"error": str(e)}


# Mascotas por especie
@router.get("/pets-by-species")
def pets_by_species(db: Session = Depends(get_pg_db)):
    query = text("SELECT species, COUNT(*) AS total FROM pet GROUP BY species ORDER BY total DESC")
    result = db.execute(query)
    return [{"species": row[0], "total": row[1]} for row in result]

# Adoptadas por centro
@router.get("/adopted-by-center")
def adopted_by_center(db: Session = Depends(get_pg_db)):
    query = text("""
        SELECT ac.name as center_name, COUNT(*) AS total_adopted 
        FROM pet p 
        JOIN adoption_centers ac ON p.adoption_center_id = ac.id 
        JOIN adoption_status ast ON p.id = ast.pet_id 
        WHERE ast.state = 'ADOPTED' 
        GROUP BY ac.name 
        ORDER BY total_adopted DESC
    """)
    result = db.execute(query)
    return [{"center_name": row[0], "total_adopted": row[1]} for row in result]

# Estado de solicitudes
@router.get("/requests-status")
def requests_status(db: Session = Depends(get_pg_db)):
    query = text("""
        SELECT ast.state as request_status, COUNT(*) AS total 
        FROM pet p 
        JOIN adoption_status ast ON p.id = ast.pet_id 
        GROUP BY ast.state 
        ORDER BY total DESC
    """)
    result = db.execute(query)
    return [{"status": row[0], "total": row[1]} for row in result]

# Porcentaje de vacunación
@router.get("/vaccination-status")
def vaccination_status(db: Session = Depends(get_pg_db)):
    # Obtener total de mascotas
    pets_query = text("SELECT COUNT(*) FROM pet")
    pets_result = db.execute(pets_query)
    total_pets = pets_result.scalar()
    
    # Obtener mascotas vacunadas (pet_id únicas en vaccines)
    vaccines_query = text("SELECT COUNT(DISTINCT pet_id) FROM vaccines")
    vaccines_result = db.execute(vaccines_query)
    vaccinated_pets = vaccines_result.scalar()
    
    percentage = round((vaccinated_pets / total_pets) * 100, 2) if total_pets else 0
    return {"total_pets": total_pets, "vaccinated": vaccinated_pets, "percentage_vaccinated": percentage}

# Verificar conexión MongoDB
@router.get("/mongodb-health")
def mongodb_health():
    """Verificar estado de conexión a MongoDB"""
    try:
        # Intentar contar documentos
        count = histories_collection.count_documents({})
        return {
            "status": "connected",
            "collection": "histories",
            "document_count": count,
            "message": "MongoDB connection successful"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"MongoDB connection failed: {str(e)}"
        }

# Historiales de mascotas (solo MongoDB)  
@router.get("/pet-histories")
def pet_histories(limit: int = 5):
    """Obtener historiales de mascotas desde MongoDB"""
    try:
        # Obtener historiales limitados de MongoDB
        histories = list(histories_collection.find().limit(limit))
        
        result = []
        for history in histories:
            result.append({
                "pet_id": history.get("pet_id"),
                "history": history.get("history", [])
            })
        
        return {
            "message": f"Showing {len(result)} pet histories from MongoDB",
            "total_found": len(result),
            "histories": result
        }
    except Exception as e:
        return {"error": f"MongoDB connection error: {str(e)}", "histories": []}
