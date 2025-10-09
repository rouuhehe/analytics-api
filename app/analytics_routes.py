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

@router.get("/check-full-structure")
def check_full_structure(
    pg_db: Session = Depends(get_pg_db),
    mysql_db: Session = Depends(get_mysql_db)
):
    """Verifica la estructura de PostgreSQL, MySQL y MongoDB"""
    result = {}

    # ---------------------------
    # 1️⃣ PostgreSQL
    # ---------------------------
    try:
        pg_tables_query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        pg_tables = [row[0] for row in pg_db.execute(pg_tables_query)]

        pg_table_info = {}
        for table in pg_tables:
            columns_query = text(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table}'
                ORDER BY ordinal_position;
            """)
            columns = pg_db.execute(columns_query).fetchall()
            pg_table_info[table] = [
                {"column": c[0], "type": c[1], "nullable": c[2]} for c in columns
            ]
        result["postgresql"] = {
            "tables": pg_tables,
            "structures": pg_table_info,
            "total_tables": len(pg_tables),
        }
    except Exception as e:
        result["postgresql_error"] = str(e)

    # ---------------------------
    # 2️⃣ MySQL
    # ---------------------------
    try:
        mysql_tables_query = text("SHOW TABLES;")
        mysql_tables = [row[0] for row in mysql_db.execute(mysql_tables_query)]

        mysql_table_info = {}
        for table in mysql_tables:
            columns_query = text(f"SHOW COLUMNS FROM {table};")
            columns = mysql_db.execute(columns_query).fetchall()
            mysql_table_info[table] = [
                {"column": c[0], "type": c[1], "nullable": c[2]} for c in columns
            ]
        result["mysql"] = {
            "tables": mysql_tables,
            "structures": mysql_table_info,
            "total_tables": len(mysql_tables),
        }
    except Exception as e:
        result["mysql_error"] = str(e)

    # ---------------------------
    # 3️⃣ MongoDB
    # ---------------------------
    try:
        # Obtener algunos documentos de ejemplo (solo 3)
        sample_docs = list(histories_collection.find().limit(3))
        formatted_docs = []
        for doc in sample_docs:
            doc.pop("_id", None)  # quitar ObjectId para que sea más limpio
            formatted_docs.append(doc)
        result["mongodb"] = {
            "collection": "histories",
            "sample_documents": formatted_docs,
            "document_count": histories_collection.count_documents({}),
        }
    except Exception as e:
        result["mongodb_error"] = str(e)

    return result

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

@router.get("/users-with-adoptions")
def users_with_adoptions(pg_db: Session = Depends(get_pg_db), mysql_db: Session = Depends(get_mysql_db)):
    """
    Devuelve la cantidad de usuarios que han adoptado mascotas (cruzando MySQL y PostgreSQL)
    """
    try:
        # 1️⃣ Obtener IDs de mascotas adoptadas (PostgreSQL)
        adopted_pets_query = text("""
            SELECT DISTINCT pet_id
            FROM adoption_status
            WHERE state = 'ADOPTED'
        """)
        adopted_pet_ids = [str(row[0]) for row in pg_db.execute(adopted_pets_query).fetchall()]

        if not adopted_pet_ids:
            return {"message": "No hay mascotas adoptadas registradas", "users_with_adoptions": 0}

        mysql_query = text("""
            SELECT COUNT(DISTINCT user_id) AS total_users
            FROM requests
            WHERE status = 'approved' 
            AND pet_id IN :adopted_pet_ids
        """)
        result = mysql_db.execute(mysql_query, {"adopted_pet_ids": tuple(adopted_pet_ids)}).fetchone()

        total_users = result[0] if result else 0

        return {
            "message": "Usuarios que han adoptado al menos una mascota",
            "total_users": total_users,
            "total_pets_adopted": len(adopted_pet_ids)
        }

    except Exception as e:
        return {"error": f"Error al ejecutar la consulta cruzada: {str(e)}"}

@router.get("/full-adoption-report")
def full_adoption_report(
    pg_db: Session = Depends(get_pg_db),
    mysql_db: Session = Depends(get_mysql_db)
):
    """
    Combina datos de PostgreSQL, MySQL y MongoDB:
    - PostgreSQL: adopciones confirmadas
    - MySQL: usuarios y solicitudes
    - MongoDB: historiales de las mascotas adoptadas
    """
    try:
        # 1️⃣ PostgreSQL → obtener mascotas adoptadas
        adopted_query = text("""
            SELECT pet_id, state, last_updated
            FROM adoption_status
            WHERE state = 'ADOPTED'
        """)
        adopted = [dict(row._mapping) for row in pg_db.execute(adopted_query).fetchall()]

        if not adopted:
            return {"message": "No hay adopciones registradas"}

        adopted_pet_ids = [str(a["pet_id"]) for a in adopted]

        # 2️⃣ MySQL → obtener usuarios que adoptaron esas mascotas
        mysql_query = text("""
            SELECT r.user_id, u.name AS user_name, r.pet_id, r.status
            FROM requests r
            JOIN users u ON r.user_id = u.id
            WHERE r.status = 'approved' AND r.pet_id IN :pet_ids
        """)
        mysql_result = mysql_db.execute(mysql_query, {"pet_ids": tuple(adopted_pet_ids)}).fetchall()
        user_adoptions = [dict(row._mapping) for row in mysql_result]

        # 3️⃣ MongoDB → obtener historiales de esas mascotas
        mongo_histories = list(histories_collection.find({"pet_id": {"$in": adopted_pet_ids}}))
        formatted_histories = {h["pet_id"]: h.get("history", []) for h in mongo_histories}

        # 4️⃣ Unir todo
        combined = []
        for record in user_adoptions:
            pet_id = record["pet_id"]
            matching_pg = next((a for a in adopted if str(a["pet_id"]) == str(pet_id)), {})
            combined.append({
                "user_id": record["user_id"],
                "user_name": record["user_name"],
                "pet_id": pet_id,
                "status_pg": matching_pg.get("state"),
                "last_updated": matching_pg.get("last_updated"),
                "history": formatted_histories.get(pet_id, [])
            })

        return {
            "message": "Reporte combinado de adopciones",
            "total_records": len(combined),
            "adoptions": combined
        }

    except Exception as e:
        return {"error": f"Error al generar el reporte combinado: {str(e)}"}
