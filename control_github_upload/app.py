from flask import Flask, render_template, request, redirect, url_for, flash, session, g, jsonify
from collections import defaultdict
from functools import wraps
from werkzeug.security import check_password_hash, generate_password_hash
import mysql.connector
from mysql.connector import Error
from config import Config
import webbrowser
import threading
import os
import datetime
import calendar
import json
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from werkzeug.utils import secure_filename
import time
import db as db_utils
import migrations as migration_utils
import permissions as permission_utils
import repositories as repo
from services.capa import (
    build_capa_insert_values,
    build_capa_step_one_values,
    expand_notification_names,
)

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = app.config.get("SECRET_KEY", "cambia-esta-clave")
app.config['SESSION_COOKIE_NAME'] = 'suite_session'

app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, 'static', 'uploads', 'documentos')
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s"
)
logger = logging.getLogger("control_app")


def open_browser():
    pass


def get_connection():
    return db_utils.get_connection(app)


def fetch_all(query, params=None):
    return db_utils.fetch_all(app, query, params)


def fetch_one(query, params=None):
    return db_utils.fetch_one(app, query, params)


def execute_query(query, params=None, many=False):
    return db_utils.execute_query(app, query, params, many)

def log_action(accion, detalles):
    """Registra una acción en el log de auditoría."""
    if hasattr(g, 'user') and g.user:
        try:
            execute_query(
                "INSERT INTO audit_log (usuario_id, accion, detalles) VALUES (%s, %s, %s)",
                (g.user['id'], accion, detalles)
            )
        except Exception as e:
            logger.exception("Error al registrar log de auditoría")

def enviar_correo(destinatario, asunto, cuerpo):
    remitente = app.config.get('MAIL_USERNAME', 'alertas@prodeman.com')
    password = app.config.get('MAIL_PASSWORD', '')
    servidor = app.config.get('MAIL_SERVER', 'smtp.gmail.com')
    puerto = app.config.get('MAIL_PORT', 587)
    
    msg = MIMEMultipart()
    msg['From'] = remitente
    msg['To'] = destinatario
    msg['Subject'] = asunto
    msg.attach(MIMEText(cuerpo, 'html'))
    
    try:
        if password:
            server = smtplib.SMTP(servidor, puerto)
            server.starttls()
            server.login(remitente, password)
            server.send_message(msg)
            server.quit()
        else:
            print(f"[DEBUG EMAIL] To: {destinatario} | Subject: {asunto} | Body: {cuerpo}")
    except Exception as e:
        logger.exception("Error al enviar correo a %s", destinatario)

def legacy_schema_sync():
    import os
    log_path = os.path.join(os.getcwd(), "migration_debug.log")
    with open(log_path, "a") as f:
        f.write(f"\n--- Migración Iniciada: {datetime.datetime.now()} ---\n")
    
    conn = get_connection()
    cursor = conn.cursor()
    try:
        def log_m(msg):
            with open(log_path, "a") as f:
                f.write(f"[MIGRATE] {msg}\n")
        
        log_m("Iniciando Paso 1...")
        # 1. Migrating to Controlador/Controlado (previous task)
        cursor.execute("SHOW COLUMNS FROM cronograma_semanal LIKE 'controlador_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE cronograma_semanal CHANGE COLUMN responsable_id controlador_id INT DEFAULT NULL")
        
        cursor.execute("SHOW COLUMNS FROM cronograma_semanal LIKE 'controlado_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN controlado_id INT DEFAULT NULL AFTER controlador_id")
            cursor.execute("ALTER TABLE cronograma_semanal ADD CONSTRAINT fk_controlado FOREIGN KEY (controlado_id) REFERENCES usuarios(id) ON DELETE SET NULL")

        cursor.execute("SHOW COLUMNS FROM controles LIKE 'controlador_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE controles ADD COLUMN controlador_id INT DEFAULT NULL AFTER responsable_id")
            cursor.execute("ALTER TABLE controles ADD CONSTRAINT fk_controles_controlador FOREIGN KEY (controlador_id) REFERENCES usuarios(id) ON DELETE SET NULL")
            cursor.execute("UPDATE controles SET controlador_id = responsable_id")

        cursor.execute("SHOW COLUMNS FROM controles LIKE 'controlado_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE controles ADD COLUMN controlado_id INT DEFAULT NULL AFTER controlador_id")
            cursor.execute("ALTER TABLE controles ADD CONSTRAINT fk_controles_controlado FOREIGN KEY (controlado_id) REFERENCES usuarios(id) ON DELETE SET NULL")
        
        # 2. CALENDAR MIGRATION (New)
        cursor.execute("SHOW COLUMNS FROM cronograma_semanal LIKE 'fecha_inicio'")
        if not cursor.fetchone():
            print("Applying Calendar Migration...")
            try:
                cursor.execute("ALTER TABLE cronograma_semanal DROP INDEX unique_cronograma")
            except Error: pass
            
            # Add new columns
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN fecha_inicio DATE NULL")
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN fecha_fin DATE NULL")
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN hora_inicio TIME DEFAULT NULL")
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN hora_fin TIME DEFAULT NULL")
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN titulo VARCHAR(255) DEFAULT NULL")
            
            # Migrate existing week data to dates (approximate)
            cursor.execute("SELECT id, anio, semana FROM cronograma_semanal WHERE anio IS NOT NULL")
            rows = cursor.fetchall()
            for r_id, anio, sem in rows:
                # Simple week to date calc
                d = datetime.date(anio, 1, 1) + datetime.timedelta(weeks=sem-1)
                d_end = d + datetime.timedelta(days=6)
                cursor.execute("UPDATE cronograma_semanal SET fecha_inicio = %s, fecha_fin = %s WHERE id = %s", (d, d_end, r_id))
            
            # Make columns NOT NULL after migration
            cursor.execute("ALTER TABLE cronograma_semanal MODIFY COLUMN fecha_inicio DATE NOT NULL")
            cursor.execute("ALTER TABLE cronograma_semanal MODIFY COLUMN fecha_fin DATE NOT NULL")
            
            # Drop old columns
            try:
                cursor.execute("ALTER TABLE cronograma_semanal DROP COLUMN anio")
                cursor.execute("ALTER TABLE cronograma_semanal DROP COLUMN semana")
            except Error: pass

        # 9. Modify documentos_control schema (Fotos and No Names)
        cursor.execute("SHOW COLUMNS FROM documentos_control LIKE 'imagen_path'")
        if not cursor.fetchone():
            try:
                cursor.execute("ALTER TABLE documentos_control MODIFY nombre_documento VARCHAR(255) NULL")
            except Exception as e:
                print(f"Warn: {e}")
            cursor.execute("ALTER TABLE documentos_control ADD COLUMN imagen_path VARCHAR(255) DEFAULT NULL")


        # 3. AUDIT LOG MIGRATION
        cursor.execute("SHOW TABLES LIKE 'audit_log'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE audit_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    usuario_id INT,
                    accion VARCHAR(255),
                    detalles TEXT,
                    fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
                )
            """)

        # 4. USER PLANTA_ID & SECTOR_ID MIGRATION
        cursor.execute("SHOW COLUMNS FROM usuarios LIKE 'planta_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE usuarios ADD COLUMN planta_id INT DEFAULT NULL")
            cursor.execute("ALTER TABLE usuarios ADD CONSTRAINT fk_usuarios_planta FOREIGN KEY (planta_id) REFERENCES plantas(id) ON DELETE SET NULL")
        
        cursor.execute("SHOW COLUMNS FROM usuarios LIKE 'sector_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE usuarios ADD COLUMN sector_id INT DEFAULT NULL")
            cursor.execute("ALTER TABLE usuarios ADD CONSTRAINT fk_usuarios_sector FOREIGN KEY (sector_id) REFERENCES sectores(id) ON DELETE SET NULL")

        # 5. RECURRENCE MIGRATION
        cursor.execute("SHOW COLUMNS FROM cronograma_semanal LIKE 'recurrencia'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN recurrencia VARCHAR(50) DEFAULT NULL")
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN recurrencia_fin DATE DEFAULT NULL")

        # 6. Sector NULL capability in schedule
        cursor.execute("ALTER TABLE cronograma_semanal MODIFY COLUMN sector_id INT NULL")
        cursor.execute("ALTER TABLE cronograma_semanal MODIFY COLUMN controlado_id INT NULL")
        
        # 7. Parent ID for recurring instances
        cursor.execute("SHOW COLUMNS FROM cronograma_semanal LIKE 'parent_id'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN parent_id INT DEFAULT NULL")

        # 8. Fix tipo_control column to allow 'E' (and others)
        cursor.execute("ALTER TABLE cronograma_semanal MODIFY COLUMN tipo_control VARCHAR(10) NOT NULL")

        # 10. Audit Plan field
        cursor.execute("SHOW COLUMNS FROM cronograma_semanal LIKE 'plan_auditoria'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE cronograma_semanal ADD COLUMN plan_auditoria TEXT DEFAULT NULL")

        # 11. Auditor Roles and Risk PDF
        for table in ['cronograma_semanal', 'controles']:
            for col in ['auditor_jefe_id', 'auditor_acompanante_id', 'auditor_formacion_id']:
                cursor.execute(f"SHOW COLUMNS FROM {table} LIKE '{col}'")
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} INT DEFAULT NULL")
                    cursor.execute(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_{col} FOREIGN KEY ({col}) REFERENCES usuarios(id) ON DELETE SET NULL")
            
            # Add string name columns for non-user auditors
            for col_name in ['auditor_jefe_nombre', 'auditor_acompanante_nombre', 'auditor_formacion_nombre']:
                cursor.execute(f"SHOW COLUMNS FROM {table} LIKE '{col_name}'")
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} VARCHAR(255) DEFAULT NULL")

        cursor.execute("SHOW COLUMNS FROM controles LIKE 'fecha_fin_control'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE controles ADD COLUMN fecha_fin_control DATE DEFAULT NULL")

        cursor.execute("SHOW COLUMNS FROM controles LIKE 'riesgos_pdf_path'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE controles ADD COLUMN riesgos_pdf_path VARCHAR(255) DEFAULT NULL")
        
        cursor.execute("SHOW COLUMNS FROM controles LIKE 'tipo_control'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE controles ADD COLUMN tipo_control VARCHAR(10) DEFAULT 'P'")

        # 12. Audit Workflow State
        cursor.execute("SHOW COLUMNS FROM controles LIKE 'estado_flujo'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE controles ADD COLUMN estado_flujo VARCHAR(50) DEFAULT 'Completado'")

        # 13. Audit Plan Workflow
        for col_name in ['sistema_gestion_auditoria', 'objetivo_auditoria', 'criterios_auditoria', 'descripcion_actividades_auditoria', 'recursos_auditoria', 'agenda_auditoria_path', 'agenda_auditoria']:
            cursor.execute(f"SHOW COLUMNS FROM controles LIKE '{col_name}'")
            if not cursor.fetchone():
                col_type = "TEXT" if col_name in ['objetivo_auditoria', 'criterios_auditoria', 'descripcion_actividades_auditoria', 'recursos_auditoria', 'agenda_auditoria'] else "VARCHAR(255)"
                cursor.execute(f"ALTER TABLE controles ADD COLUMN {col_name} {col_type} DEFAULT NULL")

        # 14. INFORME DE AUDITORIA DIGITAL
        cursor.execute("SHOW TABLES LIKE 'hallazgos_auditoria'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE hallazgos_auditoria (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    control_id INT NOT NULL,
                    requisito VARCHAR(255),
                    tipo_hallazgo VARCHAR(100),
                    descripcion TEXT,
                    FOREIGN KEY (control_id) REFERENCES controles(id) ON DELETE CASCADE
                )
            """)
        
        for col_name in ['fortalezas_auditoria', 'conclusiones_auditoria']:
            cursor.execute(f"SHOW COLUMNS FROM controles LIKE '{col_name}'")
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE controles ADD COLUMN {col_name} TEXT DEFAULT NULL")

        # 15. ACCIONES CORRECTIVAS (CAPA)
        cursor.execute("SHOW TABLES LIKE 'acciones_correctivas'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE acciones_correctivas (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    hallazgo_id INT NOT NULL,
                    control_id INT DEFAULT NULL,
                    estado_flujo VARCHAR(50) DEFAULT 'PASO_1',
                    tipo_auditoria VARCHAR(100),
                    fecha_auditoria DATE,
                    tipo_hallazgo VARCHAR(100),
                    requisito_normativo VARCHAR(255),
                    auditor_lider VARCHAR(255),
                    auditor_acompanante VARCHAR(255),
                    auditor_formacion VARCHAR(255),
                    area_auditada VARCHAR(255),
                    responsable_area VARCHAR(255),
                    proceso_auditado VARCHAR(255),
                    responsable_proceso VARCHAR(255),
                    fecha_cierre_programado DATE,
                    responsable_verificacion VARCHAR(255),
                    responsable_ejecucion VARCHAR(255),
                    evidencia_descripcion TEXT,
                    accion_inmediata_consecuencias TEXT,
                    accion_inmediata_requiere TINYINT(1) DEFAULT 0,
                    accion_inmediata_desc TEXT,
                    ishikawa_metodo TEXT,
                    ishikawa_mano_obra TEXT,
                    ishikawa_maquina TEXT,
                    ishikawa_material TEXT,
                    ishikawa_medicion TEXT,
                    ishikawa_medioambiente TEXT,
                    porque_1 TEXT,
                    porque_2 TEXT,
                    porque_3 TEXT,
                    porque_4 TEXT,
                    porque_5 TEXT,
                    porque_6 TEXT,
                    plan_tipo_accion VARCHAR(100),
                    plan_descripcion TEXT,
                    prorroga_requiere TINYINT(1) DEFAULT 0,
                    prorroga_fecha DATE,
                    prorroga_motivo TEXT,
                    prorroga_avances TEXT,
                    aprueba_causas TINYINT(1) DEFAULT 0,
                    aprueba_plan TINYINT(1) DEFAULT 0,
                    justificacion_resolucion TEXT,
                    verificacion_implementacion TEXT,
                    evidencia_path VARCHAR(255),
                    FOREIGN KEY (hallazgo_id) REFERENCES hallazgos_auditoria(id) ON DELETE CASCADE
                )
            """)
        else:
            # Fallback for incomplete table creation
            cursor.execute("SHOW COLUMNS FROM acciones_correctivas LIKE 'descripcion'")
            if cursor.fetchone():
                log_m("Columna 'descripcion' detectada en acciones_correctivas, haciéndola opcional.")
                cursor.execute("ALTER TABLE acciones_correctivas MODIFY COLUMN descripcion TEXT DEFAULT NULL")

            cursor.execute("SHOW COLUMNS FROM acciones_correctivas LIKE 'estado'")
            if cursor.fetchone():
                log_m("Columna 'estado' detectada en acciones_correctivas, haciéndola opcional.")
                cursor.execute("ALTER TABLE acciones_correctivas MODIFY COLUMN estado VARCHAR(50) DEFAULT NULL")

            cursor.execute("SHOW COLUMNS FROM acciones_correctivas LIKE 'hallazgo_id'")
            if not cursor.fetchone():
                log_m("Agregando columna base hallazgo_id")
                cursor.execute("ALTER TABLE acciones_correctivas ADD COLUMN hallazgo_id INT NOT NULL AFTER id")
                try:
                    cursor.execute("ALTER TABLE acciones_correctivas ADD CONSTRAINT fk_ac_hallazgo FOREIGN KEY (hallazgo_id) REFERENCES hallazgos_auditoria(id) ON DELETE CASCADE")
                except: pass

            cursor.execute("SHOW COLUMNS FROM acciones_correctivas LIKE 'estado_flujo'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE acciones_correctivas ADD COLUMN estado_flujo VARCHAR(50) DEFAULT 'PASO_1' AFTER hallazgo_id")
            
            columns_to_check = [
                ('tipo_auditoria', 'VARCHAR(100)'), ('fecha_auditoria', 'DATE'), ('tipo_hallazgo', 'VARCHAR(100)'),
                ('control_id', 'INT DEFAULT NULL'),
                ('requisito_normativo', 'VARCHAR(255)'), ('auditor_lider', 'VARCHAR(255)'), ('auditor_acompanante', 'VARCHAR(255)'),
                ('auditor_formacion', 'VARCHAR(255)'), ('area_auditada', 'VARCHAR(255)'), ('responsable_area', 'VARCHAR(255)'),
                ('proceso_auditado', 'VARCHAR(255)'), ('responsable_proceso', 'VARCHAR(255)'), ('fecha_cierre_programado', 'DATE'),
                ('responsable_verificacion', 'VARCHAR(255)'), ('responsable_ejecucion', 'VARCHAR(255)'), ('evidencia_descripcion', 'TEXT'),
                ('accion_inmediata_consecuencias', 'TEXT'), ('accion_inmediata_requiere', 'TINYINT(1) DEFAULT 0'), ('accion_inmediata_desc', 'TEXT'),
                ('ishikawa_metodo', 'TEXT'), ('ishikawa_mano_obra', 'TEXT'), ('ishikawa_maquina', 'TEXT'),
                ('ishikawa_material', 'TEXT'), ('ishikawa_medicion', 'TEXT'), ('ishikawa_medioambiente', 'TEXT'),
                ('porque_1', 'TEXT'), ('porque_2', 'TEXT'), ('porque_3', 'TEXT'), ('porque_4', 'TEXT'), ('porque_5', 'TEXT'), ('porque_6', 'TEXT'),
                ('plan_tipo_accion', 'VARCHAR(100)'), ('plan_descripcion', 'TEXT'), ('prorroga_requiere', 'TINYINT(1) DEFAULT 0'),
                ('prorroga_fecha', 'DATE'), ('prorroga_motivo', 'TEXT'), ('prorroga_avances', 'TEXT'),
                ('aprueba_causas', 'TINYINT(1) DEFAULT 0'), ('aprueba_plan', 'TINYINT(1) DEFAULT 0'), ('justificacion_resolucion', 'TEXT'),
                ('verificacion_implementacion', 'TEXT'), ('evidencia_path', 'VARCHAR(255)')
            ]
            for col, col_type in columns_to_check:
                cursor.execute(f"SHOW COLUMNS FROM acciones_correctivas LIKE '{col}'")
                if not cursor.fetchone():
                    log_m(f"Agregando columna {col} a acciones_correctivas")
                    cursor.execute(f"ALTER TABLE acciones_correctivas ADD COLUMN {col} {col_type}")

        conn.commit()
        log_m("Migraciones finalizadas exitosamente.")
    except Exception as e:
        with open(log_path, "a") as f:
            f.write(f"[ERROR] {str(e)}\n")
        print(f"Error during migration: {e}")
    finally:
        cursor.close()
        conn.close()


def run_migrations():
    migration_utils.run_versioned_migrations(get_connection, legacy_schema_sync)

def reset_control_data():
    print("Resetting all control data...")
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        tables = [
            "evidencias", "acciones_correctivas", "documentos_no_controlados", 
            "documentos_control", "personal_control", "productos_quimicos", 
            "cronograma_semanal", "controles", "cronogramas"
        ]
        for table in tables:
            cursor.execute(f"DELETE FROM {table}")
            cursor.execute(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        print("Reset complete.")
    except Exception as e:
        conn.rollback()
        print(f"Error during reset: {e}")
    finally:
        cursor.close()
        conn.close()


@app.before_request
def load_logged_in_user():
    user_id = session.get('user_id')
    if user_id is None:
        g.user = None
    else:
        g.user = fetch_one("SELECT *, rol_control AS rol FROM usuarios WHERE id = %s AND activo = 1", (user_id,))

def get_sidebar_pending_audits():
    if not getattr(g, 'user', None):
        return []
    if g.user['rol'] not in ['admin', 'superadmin']:
        return []
    return fetch_all("""
        SELECT id, fecha_control, fecha_fin_control, auditor_jefe_nombre
        FROM controles
        WHERE tipo_control = 'A' AND estado_flujo = 'A Confirmar'
        ORDER BY fecha_control ASC, id ASC
    """)


def get_sidebar_pending_capas():
    if not getattr(g, 'user', None):
        return []

    rows = fetch_all("""
        SELECT
            ac.id,
            ac.estado_flujo,
            ac.tipo_hallazgo,
            ac.area_auditada,
            ac.responsable_area,
            ac.auditor_lider,
            ac.fecha_cierre_programado
        FROM acciones_correctivas ac
        WHERE ac.estado_flujo IN ('PASO_1', 'PASO_2', 'PASO_3', 'PASO_4', 'PASO_5')
        ORDER BY ac.id DESC
    """)

    pendientes = []
    for row in rows:
        if permission_utils.can_edit_capa_step(g.user, row, row.get('estado_flujo')):
            pendientes.append(row)
    return pendientes


def infer_document_flags(document_state, raw_copia=None, raw_no_cargado=None):
    estado = (document_state or "correcto").strip()
    copia_controlada = (raw_copia == "si")
    no_cargado_portal = (raw_no_cargado == "si")

    if estado == "sin_copia_controlada":
        copia_controlada = False
    elif estado == "correcto":
        copia_controlada = True

    if estado == "documento_no_controlado":
        no_cargado_portal = True
    elif estado in ("correcto", "sin_copia_controlada", "documento_obsoleto"):
        no_cargado_portal = False

    return copia_controlada, no_cargado_portal


def get_all_pending_items():
    auditorias = get_sidebar_pending_audits()
    capas = get_sidebar_pending_capas()

    items = []
    for audit in auditorias:
        items.append({
            "kind": "auditoria",
            "id": audit["id"],
            "title": f"Auditoria #{audit['id']}",
            "subtitle": f"Confirmacion pendiente - {audit.get('fecha_control') or 'Sin fecha'}",
            "status": "A confirmar",
            "url": url_for("detalle_control", control_id=audit["id"]),
        })

    for capa in capas:
        items.append({
            "kind": "capa",
            "id": capa["id"],
            "title": f"CAPA #{capa['id']}",
            "subtitle": f"{capa.get('tipo_hallazgo') or 'Hallazgo'} - {capa.get('area_auditada') or 'Sin area'}",
            "status": (capa.get("estado_flujo") or "").replace("_", " "),
            "url": url_for("accion_correctiva", ac_id=capa["id"]),
        })

    return {
        "auditorias": auditorias,
        "capas": capas,
        "items": items,
        "count": len(items),
    }

@app.context_processor
def inject_sidebar_notifications():
    if not getattr(g, 'user', None):
        return {
            'sidebar_pending_items': [],
            'sidebar_pending_count': 0,
            'sidebar_pending_has_items': False,
        }
    pending = get_all_pending_items()
    return {
        'sidebar_pending_items': pending['items'],
        'sidebar_pending_count': pending['count'],
        'sidebar_pending_has_items': pending['count'] > 0,
    }

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if g.user is None:
            # Redirigir al Launcher (puerto 5000) si no hay sesión
            return redirect("http://127.0.0.1:5000/")
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if g.user is None:
                return redirect(url_for('login'))
            if g.user['rol'] not in roles:
                return redirect(url_for('mi_cronograma'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@app.route("/login")
def login():
    return redirect("http://127.0.0.1:5000/")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("http://127.0.0.1:5000/")

@app.route("/")
def home():
    if g.user and g.user['rol'] == 'visor':
        return redirect(url_for("historial"))
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
@login_required
@role_required('superadmin', 'admin', 'plant_manager')
def dashboard():
    p_id = g.user['planta_id'] if g.user['rol'] == 'plant_manager' else None
    params = {"planta_id": p_id}

    documentos_por_planta = fetch_all("""
    SELECT
        s.nombre AS sector,
        SUM(CASE WHEN dc.estado = 'correcto' THEN 1 ELSE 0 END) AS correcto,
        SUM(CASE WHEN dc.estado = 'documento_obsoleto' THEN 1 ELSE 0 END) AS documento_obsoleto,
        SUM(CASE WHEN dc.estado = 'sin_copia_controlada' THEN 1 ELSE 0 END) AS sin_copia_controlada,
        SUM(CASE WHEN dc.estado = 'documento_no_controlado' THEN 1 ELSE 0 END) AS documento_no_controlado,
        SUM(CASE WHEN dc.estado = 'otro' THEN 1 ELSE 0 END) AS otro
    FROM sectores s
    JOIN plantas p ON s.planta_id = p.id
    LEFT JOIN controles c ON c.sector_id = s.id
    LEFT JOIN documentos_control dc ON dc.control_id = c.id
    WHERE (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
    GROUP BY s.id, s.nombre
    ORDER BY s.nombre ASC
""", params)

    personal_por_planta = fetch_all("""
    SELECT
        s.nombre AS sector,
        COUNT(pc.id) AS personas_total,
        SUM(CASE WHEN pc.requiere_capacitacion = 1 THEN 1 ELSE 0 END) AS personas_requieren
    FROM sectores s
    JOIN plantas p ON s.planta_id = p.id
    LEFT JOIN controles c ON c.sector_id = s.id
    LEFT JOIN personal_control pc ON pc.control_id = c.id
    WHERE (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
    GROUP BY s.id, s.nombre
    ORDER BY s.nombre ASC
""", params)

    quimicos_por_planta = fetch_all("""
    SELECT
        s.nombre AS sector,
        COUNT(pq.id) AS quimicos_total,
        SUM(
            CASE
                WHEN pq.bajo_llave = 0
                  OR pq.envase_original = 0
                  OR pq.etiquetado_correcto = 0
                  OR pq.hoja_seguridad = 0
                THEN 1 ELSE 0
            END
        ) AS quimicos_incorrectos
    FROM sectores s
    JOIN plantas p ON s.planta_id = p.id
    LEFT JOIN controles c ON c.sector_id = s.id
    LEFT JOIN productos_quimicos pq ON pq.control_id = c.id
    WHERE (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
    GROUP BY s.id, s.nombre
    ORDER BY s.nombre ASC
""", params)

    stats = fetch_one("""
        SELECT
            COALESCE(SUM(total_documentos_correctos), 0) AS documentos_correctos,
            COALESCE(SUM(total_documentos_incorrectos), 0) AS documentos_incorrectos,
            COALESCE(SUM(total_personas_entrevistadas), 0) AS personas_total,
            COALESCE(SUM(total_requieren_capacitacion), 0) AS personas_requieren,
            COALESCE(COUNT(pq.id), 0) AS quimicos_total,
            COALESCE(SUM(
                CASE
                    WHEN pq.bajo_llave = 0
                      OR pq.envase_original = 0
                      OR pq.etiquetado_correcto = 0
                      OR pq.hoja_seguridad = 0
                    THEN 1 ELSE 0
                END
            ), 0) AS quimicos_incorrectos
        FROM controles c
        LEFT JOIN productos_quimicos pq ON pq.control_id = c.id
        WHERE (%(planta_id)s IS NULL OR c.planta_id = %(planta_id)s)
    """, params)

    ultimos_controles = fetch_all("""
        SELECT
            c.id,
            c.fecha_control,
            c.tipo_control,
            s.nombre AS sector,
            c.total_documentos_controlados,
            c.total_documentos_incorrectos,
            c.total_personas_entrevistadas,
            c.total_requieren_capacitacion
        FROM controles c
        INNER JOIN plantas p ON p.id = c.planta_id
        INNER JOIN sectores s ON s.id = c.sector_id
        WHERE (%(planta_id)s IS NULL OR c.planta_id = %(planta_id)s)
        ORDER BY c.fecha_control DESC, c.id DESC
        LIMIT 10
    """, params)

    # CAPAs pendientes para el usuario actual
    full_name = f"{g.user['nombre']} {g.user['apellido']}"
    capas_pendientes = fetch_all("""
        SELECT ac.*, h.descripcion as hallazgo_desc, p.nombre as planta_nombre
        FROM acciones_correctivas ac
        JOIN hallazgos_auditoria h ON ac.hallazgo_id = h.id
        JOIN controles c ON h.control_id = c.id
        JOIN plantas p ON c.planta_id = p.id
        WHERE (
            (ac.auditor_lider = %s AND ac.estado_flujo IN ('PASO_1', 'PASO_3', 'PASO_5'))
            OR
            (ac.responsable_area = %s AND ac.estado_flujo IN ('PASO_2', 'PASO_4'))
        )
        ORDER BY ac.id DESC
    """, (full_name, full_name))

    tendencias = fetch_all("""
        SELECT 
            DATE_FORMAT(fecha_control, '%Y-%m') as mes,
            COUNT(id) as total_controles,
            SUM(total_documentos_controlados) as docs_total,
            SUM(total_documentos_correctos) as docs_ok,
            SUM(total_personas_entrevistadas) as personas_total
        FROM controles
        WHERE fecha_control >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
          AND (%(planta_id)s IS NULL OR planta_id = %(planta_id)s)
        GROUP BY mes
        ORDER BY mes ASC
    """, params)

    auditorias_pendientes = get_sidebar_pending_audits()

    return render_template(
        "dashboard.html",
        stats=stats,
        ultimos_controles=ultimos_controles,
        documentos_por_planta=documentos_por_planta,
        personal_por_planta=personal_por_planta,
        quimicos_por_planta=quimicos_por_planta,
        tendencias=tendencias,
        auditorias_pendientes=auditorias_pendientes,
        capas_pendientes=capas_pendientes
    )


@app.route("/pendientes")
@login_required
def pendientes():
    pending = get_all_pending_items()
    return render_template(
        "pendientes.html",
        pending_items=pending["items"],
        pending_auditorias=pending["auditorias"],
        pending_capas=pending["capas"],
        pending_count=pending["count"],
    )

@app.route("/dashboard_auditorias")
@login_required
@role_required('superadmin', 'admin', 'plant_manager', 'auditor_jefe', 'auditor_acompanante')
def dashboard_auditorias():
    def aggregate_multi_value_rows(rows, group_key, value_key):
        grouped = defaultdict(lambda: defaultdict(int))
        for row in rows:
            group_value = (row.get(group_key) or "Sin sector").strip()
            raw_value = row.get(value_key) or ""
            values = [part.strip() for part in str(raw_value).split(",") if part and part.strip()]
            for value in values:
                grouped[group_value][value] += 1

        aggregated = []
        for group_value in sorted(grouped):
            for value in sorted(grouped[group_value]):
                aggregated.append({
                    group_key: group_value,
                    value_key: value,
                    "cantidad": grouped[group_value][value]
                })
        return aggregated

    s_id = request.args.get('sector_id', type=int)
    requested_planta_id = request.args.get('planta_id', type=int)
    meses = request.args.get('meses', type=int) or 12
    if meses not in [3, 6, 12, 24]:
        meses = 12
    tipo_hallazgo = (request.args.get('tipo_hallazgo') or '').strip() or None
    fecha_desde = datetime.date.today() - datetime.timedelta(days=30 * meses)

    if g.user['rol'] == 'plant_manager':
        planta_id = g.user['planta_id']
        plantas = fetch_all("SELECT id, nombre FROM plantas WHERE id = %s AND activa = 1", (planta_id,))
        sectores = fetch_all(
            "SELECT id, nombre FROM sectores WHERE planta_id = %s AND activo = 1 ORDER BY nombre",
            (planta_id,)
        )
    else:
        planta_id = requested_planta_id
        plantas = fetch_all("SELECT id, nombre FROM plantas WHERE activa = 1 ORDER BY nombre")
        sectores = fetch_all(
            "SELECT id, nombre FROM sectores WHERE activo = 1 AND (%s IS NULL OR planta_id = %s) ORDER BY nombre",
            (planta_id, planta_id)
        )

    allowed_sector_ids = {sector["id"] for sector in sectores}
    if s_id and s_id not in allowed_sector_ids:
        s_id = None

    params = {
        "sector_id": s_id,
        "planta_id": planta_id,
        "tipo_hallazgo": tipo_hallazgo,
        "fecha_desde": fecha_desde,
    }

    stats_auditoria = fetch_one("""
        SELECT
            COUNT(DISTINCT c.id) AS auditorias_total,
            COUNT(DISTINCT h.id) AS hallazgos_total,
            COUNT(DISTINCT CASE WHEN ac.estado_flujo != 'CERRADO' THEN ac.id END) AS acciones_abiertas,
            COUNT(DISTINCT CASE WHEN ac.estado_flujo = 'CERRADO' THEN ac.id END) AS acciones_cerradas
        FROM controles c
        LEFT JOIN hallazgos_auditoria h ON h.control_id = c.id
        LEFT JOIN acciones_correctivas ac ON ac.hallazgo_id = h.id
        WHERE c.tipo_control = 'A'
          AND (%(sector_id)s IS NULL OR c.sector_id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR c.planta_id = %(planta_id)s)
          AND (%(tipo_hallazgo)s IS NULL OR h.tipo_hallazgo = %(tipo_hallazgo)s)
    """, params) or {}

    hallazgos_por_sector = fetch_all("""
        SELECT
            s.nombre AS sector,
            COALESCE(h.tipo_hallazgo, 'Sin clasificar') AS tipo_hallazgo,
            COUNT(h.id) AS cantidad
        FROM sectores s
        JOIN plantas p ON s.planta_id = p.id
        LEFT JOIN controles c ON c.sector_id = s.id AND c.tipo_control = 'A'
        LEFT JOIN hallazgos_auditoria h ON h.control_id = c.id
        WHERE s.activo = 1
          AND (%(sector_id)s IS NULL OR s.id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
          AND (%(tipo_hallazgo)s IS NULL OR h.tipo_hallazgo = %(tipo_hallazgo)s)
          AND h.id IS NOT NULL
        GROUP BY s.id, s.nombre, COALESCE(h.tipo_hallazgo, 'Sin clasificar')
        ORDER BY s.nombre ASC, tipo_hallazgo ASC
    """, params)

    hallazgos_tipos_map = defaultdict(int)
    for row in hallazgos_por_sector:
        hallazgos_tipos_map[row['tipo_hallazgo']] += int(row['cantidad'] or 0)
    hallazgos_tipos = [
        {"tipo_hallazgo": tipo, "cantidad": cantidad}
        for tipo, cantidad in sorted(hallazgos_tipos_map.items(), key=lambda item: (-item[1], item[0]))
    ]

    acciones_por_sector = fetch_all("""
        SELECT
            s.nombre AS sector,
            ac.estado_flujo,
            COUNT(DISTINCT ac.id) AS cantidad
        FROM sectores s
        JOIN plantas p ON s.planta_id = p.id
        LEFT JOIN controles c ON c.sector_id = s.id AND c.tipo_control = 'A'
        LEFT JOIN hallazgos_auditoria h ON h.control_id = c.id
        LEFT JOIN acciones_correctivas ac ON ac.hallazgo_id = h.id
        WHERE s.activo = 1
          AND (%(sector_id)s IS NULL OR s.id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
          AND (%(tipo_hallazgo)s IS NULL OR h.tipo_hallazgo = %(tipo_hallazgo)s)
          AND ac.id IS NOT NULL
        GROUP BY s.id, s.nombre, ac.estado_flujo
        ORDER BY s.nombre ASC, ac.estado_flujo ASC
    """, params)

    acciones_estado_map = defaultdict(int)
    for row in acciones_por_sector:
        acciones_estado_map[row['estado_flujo']] += int(row['cantidad'] or 0)
    acciones_estado = [
        {"estado_flujo": estado, "cantidad": cantidad}
        for estado, cantidad in sorted(acciones_estado_map.items(), key=lambda item: item[0])
    ]

    acciones_tipos_rows = fetch_all("""
        SELECT
            COALESCE(s.nombre, 'Sin sector') AS sector,
            ac.plan_tipo_accion
        FROM acciones_correctivas ac
        JOIN hallazgos_auditoria h ON ac.hallazgo_id = h.id
        JOIN controles c ON h.control_id = c.id
        LEFT JOIN sectores s ON c.sector_id = s.id
        WHERE ac.plan_tipo_accion IS NOT NULL
          AND ac.plan_tipo_accion != ''
          AND c.tipo_control = 'A'
          AND (%(sector_id)s IS NULL OR c.sector_id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR c.planta_id = %(planta_id)s)
          AND (%(tipo_hallazgo)s IS NULL OR ac.tipo_hallazgo = %(tipo_hallazgo)s)
    """, params)

    acciones_tipos_sector = aggregate_multi_value_rows(acciones_tipos_rows, "sector", "plan_tipo_accion")
    acciones_tipos_map = defaultdict(int)
    for row in acciones_tipos_sector:
        acciones_tipos_map[row['plan_tipo_accion']] += int(row['cantidad'] or 0)
    acciones_tipos = [
        {"plan_tipo_accion": tipo, "cantidad": cantidad}
        for tipo, cantidad in sorted(acciones_tipos_map.items(), key=lambda item: (-item[1], item[0]))
    ]

    # CAPAs pendientes (Dashboard Auditorías)
    full_name = f"{g.user['nombre']} {g.user['apellido']}"
    capas_propias = fetch_all("""
        SELECT ac.*, h.descripcion as hallazgo_desc
        FROM acciones_correctivas ac
        JOIN hallazgos_auditoria h ON ac.hallazgo_id = h.id
        WHERE (
            (ac.auditor_lider = %s AND ac.estado_flujo IN ('PASO_1', 'PASO_3', 'PASO_5'))
            OR
            (ac.responsable_area = %s AND ac.estado_flujo IN ('PASO_2', 'PASO_4'))
        )
        ORDER BY ac.id DESC
    """, (full_name, full_name))

    tendencias_mensuales = fetch_all("""
        SELECT
            DATE_FORMAT(c.fecha_control, '%%Y-%%m') AS mes,
            COUNT(DISTINCT h.id) AS hallazgos_total,
            COUNT(DISTINCT ac.id) AS capas_total,
            COUNT(DISTINCT CASE WHEN ac.estado_flujo = 'CERRADO' THEN ac.id END) AS capas_cerradas
        FROM controles c
        LEFT JOIN hallazgos_auditoria h ON h.control_id = c.id
        LEFT JOIN acciones_correctivas ac ON ac.hallazgo_id = h.id
        WHERE c.tipo_control = 'A'
          AND c.fecha_control >= %(fecha_desde)s
          AND (%(sector_id)s IS NULL OR c.sector_id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR c.planta_id = %(planta_id)s)
          AND (%(tipo_hallazgo)s IS NULL OR h.tipo_hallazgo = %(tipo_hallazgo)s)
        GROUP BY DATE_FORMAT(c.fecha_control, '%%Y-%%m')
        ORDER BY mes ASC
    """, params)

    tiempos_ciclo = fetch_one("""
        SELECT
            ROUND(AVG(CASE
                WHEN c.plan_completado_at IS NOT NULL THEN TIMESTAMPDIFF(DAY, c.fecha_control, c.plan_completado_at)
                ELSE NULL
            END), 1) AS dias_a_plan,
            ROUND(AVG(CASE
                WHEN c.informe_emitido_at IS NOT NULL THEN TIMESTAMPDIFF(DAY, c.fecha_control, c.informe_emitido_at)
                ELSE NULL
            END), 1) AS dias_a_informe,
            ROUND(AVG(CASE
                WHEN ac.capa_creada_at IS NOT NULL THEN TIMESTAMPDIFF(DAY, c.fecha_control, ac.capa_creada_at)
                ELSE NULL
            END), 1) AS dias_a_capa,
            ROUND(AVG(CASE
                WHEN ac.capa_closed_at IS NOT NULL AND ac.capa_creada_at IS NOT NULL THEN TIMESTAMPDIFF(DAY, ac.capa_creada_at, ac.capa_closed_at)
                ELSE NULL
            END), 1) AS dias_cierre_capa
        FROM controles c
        LEFT JOIN hallazgos_auditoria h ON h.control_id = c.id
        LEFT JOIN acciones_correctivas ac ON ac.hallazgo_id = h.id
        WHERE c.tipo_control = 'A'
          AND (%(sector_id)s IS NULL OR c.sector_id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR c.planta_id = %(planta_id)s)
          AND (%(tipo_hallazgo)s IS NULL OR h.tipo_hallazgo = %(tipo_hallazgo)s)
    """, params) or {}

    sectores_recurrentes = fetch_all("""
        SELECT
            s.nombre AS sector,
            COUNT(DISTINCT CASE WHEN h.tipo_hallazgo LIKE 'No Conformidad%%' THEN h.id END) AS no_conformidades,
            COUNT(DISTINCT CASE WHEN ac.prorroga_requiere = 1 THEN ac.id END) AS prorrogas
        FROM sectores s
        JOIN plantas p ON s.planta_id = p.id
        LEFT JOIN controles c ON c.sector_id = s.id AND c.tipo_control = 'A'
        LEFT JOIN hallazgos_auditoria h ON h.control_id = c.id
        LEFT JOIN acciones_correctivas ac ON ac.hallazgo_id = h.id
        WHERE s.activo = 1
          AND (%(sector_id)s IS NULL OR s.id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
        GROUP BY s.id, s.nombre
        HAVING no_conformidades > 0 OR prorrogas > 0
        ORDER BY no_conformidades DESC, prorrogas DESC, s.nombre ASC
        LIMIT 10
    """, params)

    workflow_sector = fetch_all("""
        SELECT
            s.nombre AS sector,
            COUNT(DISTINCT CASE WHEN ac.estado_flujo = 'CERRADO' THEN ac.id END) AS cerradas,
            COUNT(DISTINCT CASE WHEN ac.estado_flujo IN ('PASO_2', 'PASO_4') THEN ac.id END) AS en_revision_sector,
            COUNT(DISTINCT CASE WHEN ac.estado_flujo IN ('PASO_1', 'PASO_3', 'PASO_5') THEN ac.id END) AS en_revision_auditor,
            COUNT(DISTINCT CASE WHEN ac.prorroga_requiere = 1 AND ac.estado_flujo = 'PASO_3' THEN ac.id END) AS bloqueadas_prorroga,
            COUNT(DISTINCT CASE WHEN ac.fecha_cierre_programado IS NOT NULL AND ac.estado_flujo != 'CERRADO' AND ac.fecha_cierre_programado < CURDATE() THEN ac.id END) AS vencidas
        FROM sectores s
        JOIN plantas p ON s.planta_id = p.id
        LEFT JOIN controles c ON c.sector_id = s.id AND c.tipo_control = 'A'
        LEFT JOIN hallazgos_auditoria h ON h.control_id = c.id
        LEFT JOIN acciones_correctivas ac ON ac.hallazgo_id = h.id
        WHERE s.activo = 1
          AND (%(sector_id)s IS NULL OR s.id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
          AND ac.id IS NOT NULL
        GROUP BY s.id, s.nombre
        ORDER BY s.nombre ASC
    """, params)

    ultimas_acciones = fetch_all("""
        SELECT ac.id, ac.tipo_hallazgo, ac.estado_flujo, ac.area_auditada, ac.auditor_lider, ac.responsable_area, ac.fecha_cierre_programado,
               ac.prorroga_requiere,
               CASE
                   WHEN ac.estado_flujo = 'CERRADO' THEN 'Cerrado'
                   WHEN ac.fecha_cierre_programado IS NOT NULL AND ac.estado_flujo != 'CERRADO' AND ac.fecha_cierre_programado < CURDATE() THEN 'Vencido'
                   WHEN ac.prorroga_requiere = 1 AND ac.estado_flujo = 'PASO_3' THEN 'Bloqueado por prórroga'
                   WHEN ac.estado_flujo IN ('PASO_2', 'PASO_4') THEN 'En revisión sector'
                   WHEN ac.estado_flujo IN ('PASO_1', 'PASO_3', 'PASO_5') THEN 'En revisión auditor'
                   ELSE 'Pendiente'
               END AS workflow_estado_visual
        FROM acciones_correctivas ac
        JOIN hallazgos_auditoria h ON ac.hallazgo_id = h.id
        JOIN controles c ON h.control_id = c.id
        WHERE (%(sector_id)s IS NULL OR c.sector_id = %(sector_id)s)
          AND (%(planta_id)s IS NULL OR c.planta_id = %(planta_id)s)
          AND (%(tipo_hallazgo)s IS NULL OR ac.tipo_hallazgo = %(tipo_hallazgo)s)
        ORDER BY ac.id DESC LIMIT 10
    """, params)

    hallazgos_disponibles = fetch_all("""
        SELECT DISTINCT tipo_hallazgo
        FROM hallazgos_auditoria
        WHERE tipo_hallazgo IS NOT NULL AND tipo_hallazgo != ''
        ORDER BY tipo_hallazgo
    """)

    return render_template(
        "dashboard_auditorias.html",
        plantas=plantas,
        planta_id=planta_id,
        meses=meses,
        tipo_hallazgo=tipo_hallazgo,
        hallazgos_disponibles=hallazgos_disponibles,
        stats_auditoria=stats_auditoria,
        hallazgos_tipos=hallazgos_tipos,
        hallazgos_por_sector=hallazgos_por_sector,
        acciones_estado=acciones_estado,
        acciones_por_sector=acciones_por_sector,
        acciones_tipos=acciones_tipos,
        acciones_tipos_sector=acciones_tipos_sector,
        tendencias_mensuales=tendencias_mensuales,
        tiempos_ciclo=tiempos_ciclo,
        sectores_recurrentes=sectores_recurrentes,
        workflow_sector=workflow_sector,
        ultimas_acciones=ultimas_acciones,
        capas_pendientes=capas_propias,
        sectores=sectores,
        s_id=s_id
    )

@app.route("/usuarios")
@login_required
@role_required('superadmin')
def usuarios():
    lista_usuarios = fetch_all("""
        SELECT u.id, u.nombre, u.apellido, u.usuario, u.email, u.rol_control AS rol, u.activo, u.created_at, 
               COALESCE(u.planta_id, s.planta_id) AS planta_id, u.sector_id, p.nombre as planta_nombre, s.nombre as sector_nombre
        FROM usuarios u
        LEFT JOIN sectores s ON u.sector_id = s.id
        LEFT JOIN plantas p ON COALESCE(u.planta_id, s.planta_id) = p.id
        ORDER BY u.id DESC
    """)
    plantas = fetch_all("SELECT id, nombre FROM plantas WHERE activa = 1 ORDER BY nombre")
    sectores = fetch_all("SELECT id, planta_id, nombre FROM sectores WHERE activo = 1 ORDER BY nombre")
    return render_template("usuarios.html", usuarios=lista_usuarios, plantas=plantas, sectores=sectores)

@app.route("/usuarios/nuevo", methods=["POST"])
@login_required
@role_required('superadmin')
def nuevo_usuario():
    nombre = request.form["nombre"]
    apellido = request.form["apellido"]
    email = request.form["email"]
    password = request.form["password"]
    rol = request.form["rol"]
    usuario = request.form.get("usuario")
    
    planta_id = request.form.get("planta_id") or None
    sector_id = request.form.get("sector_id") or None
    if sector_id and not planta_id:
        planta_id = infer_planta_id_from_sector(sector_id)
    
    hash_pass = generate_password_hash(password)
    try:
        execute_query(
            "INSERT INTO usuarios (nombre, apellido, usuario, email, password_hash, rol_control, activo, planta_id, sector_id) VALUES (%s, %s, %s, %s, %s, %s, 1, %s, %s)",
            (nombre, apellido, usuario, email, hash_pass, rol, planta_id, sector_id)
        )
        flash("Usuario creado exitosamente.", "success")
    except Exception as e:
        flash(f"Error al crear usuario: {e}", "danger")
    return redirect(url_for("usuarios"))

@app.route("/usuarios/toggle/<int:user_id>", methods=["POST"])
@login_required
@role_required('superadmin')
def toggle_usuario(user_id):
    user = fetch_one("SELECT activo FROM usuarios WHERE id = %s", (user_id,))
    if user:
        nuevo_estado = 0 if user['activo'] else 1
        execute_query("UPDATE usuarios SET activo = %s WHERE id = %s", (nuevo_estado, user_id))
        flash("Estado del usuario actualizado.", "success")
    return redirect(url_for("usuarios"))

@app.route("/usuarios/cambiar_rol/<int:user_id>", methods=["POST"])
@login_required
@role_required('superadmin')
def cambiar_rol_usuario(user_id):
    if user_id == g.user['id']:
        flash("No puedes cambiar tu propio rol por seguridad.", "warning")
        return redirect(url_for("usuarios"))
        
    nuevo_rol = request.form.get("rol")
    if nuevo_rol in ['superadmin', 'admin', 'visor', 'plant_manager', 'auditor_jefe']:
        execute_query("UPDATE usuarios SET rol_control = %s WHERE id = %s", (nuevo_rol, user_id))
        flash("Rol de Control actualizado exitosamente.", "success")
    else:
        flash("Rol inválido.", "danger")
    return redirect(url_for("usuarios"))

@app.route("/usuarios/editar/<int:user_id>", methods=["POST"])
@login_required
@role_required('superadmin')
def editar_usuario(user_id):
    nombre = request.form["nombre"]
    apellido = request.form["apellido"]
    email = request.form["email"]
    rol = request.form["rol"]
    usuario = request.form.get("usuario")
    password = request.form.get("password")
    
    planta_id = request.form.get("planta_id") or None
    sector_id = request.form.get("sector_id") or None
    if sector_id and not planta_id:
        planta_id = infer_planta_id_from_sector(sector_id)
    
    try:
        if password and password.strip():
            hash_pass = generate_password_hash(password)
            execute_query(
                "UPDATE usuarios SET nombre = %s, apellido = %s, usuario = %s, email = %s, rol_control = %s, password_hash = %s, planta_id = %s, sector_id = %s WHERE id = %s",
                (nombre, apellido, usuario, email, rol, hash_pass, planta_id, sector_id, user_id)
            )
        else:
            execute_query(
                "UPDATE usuarios SET nombre = %s, apellido = %s, usuario = %s, email = %s, rol_control = %s, planta_id = %s, sector_id = %s WHERE id = %s",
                (nombre, apellido, usuario, email, rol, planta_id, sector_id, user_id)
            )
        flash("Usuario actualizado exitosamente.", "success")
    except Exception as e:
        flash(f"Error al actualizar usuario: {e}", "danger")
    return redirect(url_for("usuarios"))

@app.route("/nuevo-control", methods=["GET", "POST"])
@login_required
@role_required('superadmin', 'admin')
def nuevo_control():
    plantas = fetch_all("""
        SELECT id, nombre
        FROM plantas
        WHERE activa = 1
        ORDER BY nombre
    """)

    sectores = fetch_all("""
        SELECT id, planta_id, nombre
        FROM sectores
        WHERE activo = 1
        ORDER BY nombre
    """)

    usuarios = fetch_all("""
        SELECT id, nombre, apellido, sector_id
        FROM usuarios
        WHERE activo = 1
        ORDER BY nombre, apellido
    """)

    if request.method == "POST":
        try:
            # DATOS GENERALES
            planta_id = request.form["planta_id"]
            sector_id = request.form["sector_id"]
            fecha_control = request.form["fecha_control"]
            controlador_id = request.form.get("controlador_id") or None
            controlado_id = request.form.get("controlado_id") or None
            responsable_id = controlador_id # Fallback for legacy
            sector_tiene_quimicos = 1 if request.form.get("sector_tiene_quimicos") == "si" else 0
            observaciones_generales = request.form.get("observaciones_generales") or None

            # PERSONAL
            personal_nombres = request.form.getlist("personal_nombre[]")
            personal_conoce = request.form.getlist("personal_conoce[]")
            personal_cap = request.form.getlist("personal_capacitacion[]")
            personal_req = request.form.getlist("personal_requiere[]")
            personal_obs = request.form.getlist("personal_observacion[]")

            # DOCUMENTOS
            documentos_codigos = request.form.getlist("documento_codigo[]")
            documentos_revision = request.form.getlist("documento_revision[]")
            documentos_copia = request.form.getlist("documento_copia[]")
            documentos_copia_numero = request.form.getlist("documento_copia_numero[]")
            documentos_no_cargado_portal = request.form.getlist("documento_no_cargado_portal[]")
            documentos_motivo_no_cargado = request.form.getlist("documento_motivo_no_cargado[]")
            documentos_estado = request.form.getlist("documento_estado[]")
            documentos_obs = request.form.getlist("documento_observacion[]")
            documentos_fotos = request.files.getlist("documento_foto[]")

            # QUIMICOS
            quimicos_nombres = request.form.getlist("quimico_nombre[]")
            quimicos_llave = request.form.getlist("quimico_llave[]")
            quimicos_envase = request.form.getlist("quimico_envase[]")
            quimicos_etiqueta = request.form.getlist("quimico_etiqueta[]")
            quimicos_hoja = request.form.getlist("quimico_hoja[]")
            quimicos_obs = request.form.getlist("quimico_observacion[]")
            quimicos_medida = request.form.getlist("quimico_medida[]")

            # TOTALES
            total_personas = 0
            total_requieren = 0
            total_docs = 0
            total_correctos = 0
            total_incorrectos = 0

            for i, nombre in enumerate(personal_nombres):
                if nombre.strip():
                    total_personas += 1
                    if i < len(personal_req) and personal_req[i] == "si":
                        total_requieren += 1

            for i, codigo in enumerate(documentos_codigos):
                if codigo.strip():
                    total_docs += 1
                    estado = documentos_estado[i] if i < len(documentos_estado) else "correcto"
                    if estado == "correcto":
                        total_correctos += 1
                    else:
                        total_incorrectos += 1

            # AUDITORIA INFO
            tipo_control = request.form.get("tipo_control", "P")
            auditor_jefe_id = request.form.get("auditor_jefe_id") or None
            auditor_acompanante_id = request.form.get("auditor_acompanante_id") or None
            auditor_formacion_id = request.form.get("auditor_formacion_id") or None
            riesgos_pdf_path = None

            # Handle PDF Upload
            if tipo_control == 'A' and 'riesgos_pdf' in request.files:
                file = request.files['riesgos_pdf']
                if file and file.filename != '':
                    filename = secure_filename(f"riesgos_{int(time.time())}_{file.filename}")
                    file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                    riesgos_pdf_path = filename

            # GUARDAR CONTROL
            control_id = execute_query("""
                INSERT INTO controles (
                    planta_id, sector_id, fecha_control, responsable_id, controlador_id, controlado_id,
                    sector_tiene_quimicos, observaciones_generales,
                    total_personas_entrevistadas, total_requieren_capacitacion,
                    total_documentos_controlados, total_documentos_correctos, total_documentos_incorrectos,
                    tipo_control, auditor_jefe_id, auditor_acompanante_id, auditor_formacion_id, riesgos_pdf_path
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                planta_id, sector_id, fecha_control, responsable_id, controlador_id, controlado_id,
                sector_tiene_quimicos, observaciones_generales,
                total_personas, total_requieren,
                total_docs, total_correctos, total_incorrectos,
                tipo_control, auditor_jefe_id, auditor_acompanante_id, auditor_formacion_id, riesgos_pdf_path
            ))

            # GUARDAR PERSONAL
            personal_rows = []
            for i, nombre in enumerate(personal_nombres):
                if nombre.strip():
                    personal_rows.append((
                        control_id,
                        nombre.strip(),
                        1 if i < len(personal_conoce) and personal_conoce[i] == "si" else 0,
                        1 if i < len(personal_cap) and personal_cap[i] == "si" else 0,
                        1 if i < len(personal_req) and personal_req[i] == "si" else 0,
                        personal_obs[i] if i < len(personal_obs) else None
                    ))

            if personal_rows:
                execute_query("""
                    INSERT INTO personal_control (
                        control_id,
                        nombre_apellido,
                        conoce_gestion_documental,
                        realizo_capacitacion,
                        requiere_capacitacion,
                        observaciones
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, personal_rows, many=True)

            # GUARDAR DOCUMENTOS
            documentos_rows = []
            for i, codigo in enumerate(documentos_codigos):
                if codigo.strip():
                    estado_documento = documentos_estado[i] if i < len(documentos_estado) and documentos_estado[i] else "correcto"
                    copia_controlada, no_cargado_portal = infer_document_flags(
                        estado_documento,
                        documentos_copia[i] if i < len(documentos_copia) else None,
                        documentos_no_cargado_portal[i] if i < len(documentos_no_cargado_portal) else None,
                    )
                    imagen_path = None
                    if i < len(documentos_fotos) and documentos_fotos[i].filename:
                        foto = documentos_fotos[i]
                        filename = f"{int(time.time())}_{secure_filename(foto.filename)}"
                        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                        foto.save(upload_path)
                        imagen_path = filename

                    documentos_rows.append((
                        control_id,
                        None,
                        codigo.strip(),
                        documentos_revision[i].strip() if i < len(documentos_revision) and documentos_revision[i].strip() else None,
                        1 if copia_controlada else 0,
                        documentos_copia_numero[i].strip() if i < len(documentos_copia_numero) and documentos_copia_numero[i].strip() else None,
                        1 if no_cargado_portal else 0,
                        documentos_motivo_no_cargado[i].strip() if i < len(documentos_motivo_no_cargado) and documentos_motivo_no_cargado[i].strip() else None,
                        estado_documento,
                        documentos_obs[i].strip() if i < len(documentos_obs) and documentos_obs[i].strip() else None,
                        imagen_path
                    ))

            if documentos_rows:
                execute_query("""
                    INSERT INTO documentos_control (
                        control_id,
                        nombre_documento,
                        codigo_documento,
                        revision,
                        copia_controlada,
                        copia_controlada_numero,
                        no_cargado_portal,
                        motivo_no_cargado,
                        estado,
                        observaciones,
                        imagen_path
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, documentos_rows, many=True)

            # GUARDAR QUIMICOS
            quimicos_rows = []
            if sector_tiene_quimicos == 1:
                for i, nombre in enumerate(quimicos_nombres):
                    if nombre.strip():
                        quimicos_rows.append((
                            control_id,
                            nombre.strip(),
                            1 if i < len(quimicos_llave) and quimicos_llave[i] == "si" else 0,
                            1 if i < len(quimicos_envase) and quimicos_envase[i] == "si" else 0,
                            1 if i < len(quimicos_etiqueta) and quimicos_etiqueta[i] == "si" else 0,
                            1 if i < len(quimicos_hoja) and quimicos_hoja[i] == "si" else 0,
                            quimicos_obs[i] if i < len(quimicos_obs) else None,
                            quimicos_medida[i] if i < len(quimicos_medida) else None
                        ))

            if quimicos_rows:
                execute_query("""
                    INSERT INTO productos_quimicos (
                        control_id,
                        nombre_producto,
                        bajo_llave,
                        envase_original,
                        etiquetado_correcto,
                        hoja_seguridad,
                        observaciones,
                        medida
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, quimicos_rows, many=True)

            # VINCULAR CON CRONOGRAMA SI EXISTE
            event_id = request.form.get("event_id")
            if event_id and event_id.strip():
                # Check if it's a recurring template
                template = fetch_one("SELECT * FROM cronograma_semanal WHERE id = %s", (event_id,))
                if template and template['recurrencia']:
                    # Calculate duration to preserve it in the clone
                    f_control = datetime.date.fromisoformat(fecha_control)
                    duration = template['fecha_fin'] - template['fecha_inicio']
                    f_fin = f_control + duration

                    # Clone to new realized record for this specific date
                    execute_query("""
                        INSERT INTO cronograma_semanal (sector_id, fecha_inicio, fecha_fin, hora_inicio, hora_fin, titulo, tipo_control, controlador_id, controlado_id, control_id, parent_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (sector_id, f_control, f_fin, template['hora_inicio'], template['hora_fin'], 
                          template['titulo'], 'R', controlador_id, controlado_id, control_id, event_id))
                else:
                    # Single event, just update
                    execute_query("UPDATE cronograma_semanal SET tipo_control = 'R', control_id = %s WHERE id = %s", (control_id, event_id))

            # SEND EMAIL TO RESPONSIBLE
            if controlador_id:
                controlador_data = fetch_one("SELECT email, nombre, apellido FROM usuarios WHERE id = %s", (controlador_id,))
                if controlador_data and controlador_data['email']:
                    enlace = url_for('detalle_control_publico', control_id=control_id, _external=True)
                    asunto = f"Control Documental Completado - #{control_id}"
                    cuerpo = f"""
                    <div style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto;">
                        <h2 style="color: #0056b3;">Se ha completado un nuevo control</h2>
                        <p>Hola <strong>{controlador_data['nombre']} {controlador_data['apellido']}</strong>,</p>
                        <p>Se ha registrado un nuevo control documental.</p>
                        <p>Puedes revisar el resumen detallado del control en el siguiente enlace, de forma pública y sin necesidad de iniciar sesión:</p>
                        <div style="margin: 25px 0;">
                            <a href="{enlace}" style="background-color: #0d6efd; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold;">Ver Resumen del Control</a>
                        </div>
                        <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                        <small style="color: #888;">Este es un mensaje automático del Sistema de Control Documental.</small>
                    </div>
                    """
                    threading.Thread(target=enviar_correo, args=(controlador_data['email'], asunto, cuerpo)).start()

            log_action("NUEVO_CONTROL", f"Control ID: {control_id}")
            flash("Control guardado correctamente.", "success")
            return redirect(url_for("historial"))

        except Exception as exc:
            flash(f"Error al guardar el control: {exc}", "danger")

    return render_template(
        "nuevo_control.html",
        plantas=plantas,
        sectores=sectores,
        usuarios=usuarios
    )

@app.route("/historial")
@login_required
@role_required('superadmin', 'admin', 'plant_manager')
def historial():
    planta_id = request.args.get("planta_id", type=int)
    sector_id = request.args.get("sector_id", type=int)
    responsable_id = request.args.get("responsable_id", type=int)
    fecha = request.args.get("fecha")
    tipo_control = request.args.get("tipo_control")

    plantas = fetch_all("""
        SELECT id, nombre
        FROM plantas
        WHERE activa = 1
        ORDER BY nombre
    """)

    sectores = fetch_all("""
        SELECT id, planta_id, nombre
        FROM sectores
        WHERE activo = 1
        ORDER BY nombre
    """)

    query = """
        SELECT
            c.id,
            c.fecha_control,
            c.tipo_control,
            p.nombre AS planta,
            s.nombre AS sector,
            c.total_documentos_controlados,
            c.total_documentos_correctos,
            c.total_documentos_incorrectos,
            c.total_personas_entrevistadas,
            c.total_requieren_capacitacion,
            CONCAT(COALESCE(u.nombre, ''), ' ', COALESCE(u.apellido, '')) AS responsable,
            (SELECT COUNT(*) FROM documentos_control dc WHERE dc.control_id = c.id AND dc.imagen_path IS NOT NULL) > 0 AS tiene_fotos
        FROM controles c
        INNER JOIN plantas p ON p.id = c.planta_id
        INNER JOIN sectores s ON s.id = c.sector_id
        LEFT JOIN usuarios u ON u.id = c.controlador_id
        WHERE 1 = 1
    """
    params = []

    if planta_id:
        query += " AND c.planta_id = %s"
        params.append(planta_id)

    if sector_id:
        query += " AND c.sector_id = %s"
        params.append(sector_id)

    if responsable_id:
        query += " AND (c.controlador_id = %s OR c.controlado_id = %s)"
        params.append(responsable_id)
        params.append(responsable_id)

    if fecha:
        query += " AND c.fecha_control = %s"
        params.append(fecha)

    if tipo_control:
        query += " AND c.tipo_control = %s"
        params.append(tipo_control)

    query += " ORDER BY c.fecha_control DESC, c.id DESC"

    controles = fetch_all(query, tuple(params))

    # Fetch sectors for the filter
    plantas = fetch_all("SELECT id, nombre FROM plantas WHERE activa = 1 ORDER BY nombre")
    sectores = fetch_all("SELECT id, planta_id, nombre FROM sectores WHERE activo = 1 ORDER BY nombre")
    usuarios_activos = fetch_all("""
        SELECT id, nombre, apellido, email, sector_id
        FROM usuarios
        WHERE activo = 1
        ORDER BY nombre, apellido
    """)
    current_year = datetime.datetime.now().year

    return render_template(
        "historial.html",
        controles=controles,
        plantas=plantas,
        sectores=sectores,
        current_year=current_year,
        usuarios_activos=usuarios_activos,
        es_mi_cronograma=False
    )

@app.route("/api/cronograma/eventos")
@login_required
def get_eventos():
    start = request.args.get('start') # ISO string from FullCalendar
    end = request.args.get('end')
    es_mi_cronograma = request.args.get('mi_cronograma') == 'true'
    
    query = """
        SELECT 
            cs.id, 
            cs.tipo_control, 
            cs.fecha_inicio, 
            cs.fecha_fin, 
            cs.hora_inicio, 
            cs.hora_fin, 
            cs.titulo,
            cs.controlador_id, 
            cs.controlado_id,
            cs.sector_id,
            cs.control_id,
            cs.recurrencia,
            cs.recurrencia_fin,
            cs.parent_id,
            s.nombre as sector_nombre,
            u1.nombre as controlador_nombre,
            u2.nombre as controlado_nombre,
            cs.plan_auditoria,
            cs.auditor_jefe_id,
            cs.auditor_acompanante_id,
            cs.auditor_formacion_id,
            cs.auditor_jefe_nombre,
            cs.auditor_acompanante_nombre,
            cs.auditor_formacion_nombre,
            aj.nombre as jefe_nombre,
            aa.nombre as acompanante_nombre,
            af.nombre as formacion_nombre,
            c.estado_flujo AS control_estado_flujo,
            c.tipo_control AS control_tipo_real
        FROM cronograma_semanal cs
        LEFT JOIN sectores s ON s.id = cs.sector_id
        LEFT JOIN plantas p ON p.id = s.planta_id
        LEFT JOIN usuarios u1 ON u1.id = cs.controlador_id
        LEFT JOIN usuarios u2 ON u2.id = cs.controlado_id
        LEFT JOIN usuarios aj ON aj.id = cs.auditor_jefe_id
        LEFT JOIN usuarios aa ON aa.id = cs.auditor_acompanante_id
        LEFT JOIN usuarios af ON af.id = cs.auditor_formacion_id
        LEFT JOIN controles c ON c.id = cs.control_id
        WHERE (
            (cs.fecha_inicio <= %(end)s AND cs.fecha_fin >= %(start)s)
            OR (cs.recurrencia IS NOT NULL AND cs.fecha_inicio <= %(end)s AND (cs.recurrencia_fin IS NULL OR cs.recurrencia_fin >= %(start)s))
        )
        AND (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
    """
    params_dict = {
        "end": (end.split('T')[0] if end else datetime.date.today().isoformat()), 
        "start": (start.split('T')[0] if start else datetime.date.today().isoformat()),
        "planta_id": g.user['planta_id'] if g.user['rol'] == 'plant_manager' else None
    }
    
    if es_mi_cronograma:
        query += f"""
        AND (
            cs.controlador_id = %(u_id)s
            OR cs.controlado_id = %(u_id)s
            OR {sql_multi_name_match('cs.auditor_jefe_nombre')}
            OR {sql_multi_name_match('cs.auditor_acompanante_nombre')}
            OR {sql_multi_name_match('cs.auditor_formacion_nombre')}
        )
        """
        params_dict['u_id'] = g.user['id']
        params_dict['full_name'] = f"{g.user['nombre']} {g.user['apellido']}".strip()
        
    rows = fetch_all(query, params_dict)
    
    # Track exceptions (Realized 'R' or Excluded 'E') to skip them in recurrence
    realized_exceptions = {} # { int(parent_id): [date_strs...] }
    for r in rows:
        if r['tipo_control'] in ['R', 'E'] and r['parent_id']:
            pid = int(r['parent_id'])
            if pid not in realized_exceptions: realized_exceptions[pid] = []
            if hasattr(r['fecha_inicio'], 'strftime'):
                realized_exceptions[pid].append(r['fecha_inicio'].strftime('%Y-%m-%d'))
            else:
                realized_exceptions[pid].append(str(r['fecha_inicio']))

    view_start = datetime.date.fromisoformat(params_dict['start'])
    view_end = datetime.date.fromisoformat(params_dict['end'])

    def resolve_calendar_status(r):
        if r.get('control_tipo_real') == 'A' or r['tipo_control'] == 'A':
            if r.get('control_estado_flujo'):
                return r['control_estado_flujo']
            return 'Programada'
        if r['tipo_control'] == 'R':
            return 'Realizado'
        return 'Programado'

    def status_class_from_label(label, item_kind='cronograma'):
        normalized = (label or '').strip().lower()
        if item_kind == 'capa':
            mapping = {
                'cerrado': 'event-capa-cerrado',
                'vencido': 'event-capa-vencido',
                'bloqueado por prórroga': 'event-capa-prorroga',
                'bloqueado por prorroga': 'event-capa-prorroga',
                'en revisión sector': 'event-capa-sector',
                'en revision sector': 'event-capa-sector',
                'en revisión auditor': 'event-capa-auditor',
                'en revision auditor': 'event-capa-auditor',
            }
            return mapping.get(normalized, 'event-capa')

        mapping = {
            'a confirmar': 'event-audit-confirm',
            'confirmada': 'event-audit-confirmed',
            'reprogramada': 'event-audit-rescheduled',
            'completado': 'event-realized',
            'completada': 'event-realized',
            'realizado': 'event-realized',
            'programada': 'event-programmed',
            'programado': 'event-programmed',
        }
        return mapping.get(normalized, 'event-programmed')

    def process_r(r, is_virtual=False):
        s_date = r['fecha_inicio'].isoformat()
        if r['hora_inicio']: s_date += f"T{r['hora_inicio']}"
        e_date = r['fecha_fin'].isoformat()
        if r['hora_fin']: e_date += f"T{r['hora_fin']}"
        else: e_date = (r['fecha_fin'] + datetime.timedelta(days=1)).isoformat()

        calendar_status = resolve_calendar_status(r)
        color = '#ffc107' if r['tipo_control'] == 'P' else ('#198754' if r['tipo_control'] == 'R' else '#0d6efd')
        # Unique ID for virtual is essential for FullCalendar
        ev_id = f"{r['id']}_{r['fecha_inicio'].isoformat()}" if is_virtual else str(r['id'])
        
        return {
            'id': ev_id,
            'title': r['titulo'] or (f"Control {r['sector_nombre']}" if r['sector_nombre'] else "Control General"),
            'start': s_date, 'end': e_date,
            'backgroundColor': color, 'borderColor': color,
            'extendedProps': {
                'original_id': r['id'],
                'tipo_control': r['tipo_control'],
                'sector_id': r['sector_id'],
                'sector_nombre': r['sector_nombre'],
                'controlador_id': r['controlador_id'],
                'controlado_id': r['controlado_id'],
                'controlador_nombre': r['controlador_nombre'],
                'controlado_nombre': r['controlado_nombre'],
                'controlado_nombre': r['controlado_nombre'],
                'control_id': r['control_id'],
                'item_kind': 'cronograma',
                'calendar_status': calendar_status,
                'status_class': status_class_from_label(calendar_status),
                'is_audit': (r.get('control_tipo_real') == 'A' or r['tipo_control'] == 'A'),
                'recurrencia': r['recurrencia'],
                'recurrencia_fin': r['recurrencia_fin'].isoformat() if r['recurrencia_fin'] else None,
                'plan_auditoria': r['plan_auditoria'],
                'auditor_jefe_id': r['auditor_jefe_id'],
                'auditor_acompanante_id': r['auditor_acompanante_id'],
                'auditor_formacion_id': r['auditor_formacion_id'],
                'auditor_jefe_nombre': r['auditor_jefe_nombre'],
                'auditor_acompanante_nombre': r['auditor_acompanante_nombre'],
                'auditor_formacion_nombre': r['auditor_formacion_nombre'],
                'jefe_nombre': r['jefe_nombre'] or r['auditor_jefe_nombre'],
                'acompanante_nombre': r['acompanante_nombre'] or r['auditor_acompanante_nombre'],
                'formacion_nombre': r['formacion_nombre'] or r['auditor_formacion_nombre']
            }
        }

    events = []
    for r in rows:
        if r['tipo_control'] == 'E': continue # SKIP exclusion/cancelation markers
        
        rid = int(r['id'])
        # Check if this specific instance (base event) is excluded
        is_excluded = False
        if rid in realized_exceptions:
            base_date_str = r['fecha_inicio'].strftime('%Y-%m-%d') if hasattr(r['fecha_inicio'], 'strftime') else str(r['fecha_inicio'])
            if base_date_str in realized_exceptions[rid] and r['recurrencia']:
                is_excluded = True

        # Base event (if in range AND not excluded)
        if r['fecha_inicio'] <= view_end and r['fecha_fin'] >= view_start and not is_excluded:
            events.append(process_r(r))
        
        # Recurrence virtual expansion
        if r['recurrencia']:
            curr_start, curr_end = r['fecha_inicio'], r['fecha_fin']
            rec_limit = r['recurrencia_fin'] or view_end # Limit by user choice OR view range
            
            while True:
                if r['recurrencia'] == 'semanal':
                    curr_start += datetime.timedelta(days=7)
                    curr_end += datetime.timedelta(days=7)
                elif r['recurrencia'] == 'mensual':
                    # Month increment
                    m = curr_start.month % 12 + 1
                    y = curr_start.year + (curr_start.month // 12)
                    max_day = calendar.monthrange(y, m)[1]
                    curr_start = curr_start.replace(year=y, month=m, day=min(curr_start.day, max_day))
                    
                    me, ye = curr_end.month % 12 + 1, curr_end.year + (curr_end.month // 12)
                    max_day_end = calendar.monthrange(ye, me)[1]
                    curr_end = curr_end.replace(year=ye, month=me, day=min(curr_end.day, max_day_end))

                if curr_start > rec_limit or curr_start > view_end: break
                
                # SKIP if already realized or excluded (exception)
                curr_date_str = curr_start.strftime('%Y-%m-%d') if hasattr(curr_start, 'strftime') else str(curr_start)
                if rid in realized_exceptions and curr_date_str in realized_exceptions[rid]:
                    continue

                if curr_start <= view_end and curr_end >= view_start:
                    events.append(process_r({**r, 'fecha_inicio': curr_start, 'fecha_fin': curr_end}, is_virtual=True))

    capa_query = """
        SELECT
            ac.id,
            ac.control_id,
            ac.estado_flujo,
            ac.fecha_auditoria,
            ac.fecha_cierre_programado,
            ac.tipo_hallazgo,
            ac.area_auditada,
            ac.auditor_lider,
            ac.responsable_area,
            ac.prorroga_requiere,
            s.nombre AS sector_nombre,
            p.id AS planta_id,
            CASE
                WHEN ac.estado_flujo = 'CERRADO' THEN 'Cerrado'
                WHEN ac.fecha_cierre_programado IS NOT NULL AND ac.estado_flujo != 'CERRADO' AND ac.fecha_cierre_programado < CURDATE() THEN 'Vencido'
                WHEN ac.prorroga_requiere = 1 AND ac.estado_flujo = 'PASO_3' THEN 'Bloqueado por prórroga'
                WHEN ac.estado_flujo IN ('PASO_2', 'PASO_4') THEN 'En revisión sector'
                WHEN ac.estado_flujo IN ('PASO_1', 'PASO_3', 'PASO_5') THEN 'En revisión auditor'
                ELSE 'En gestión'
            END AS calendar_status
        FROM acciones_correctivas ac
        LEFT JOIN sectores s ON s.nombre = ac.area_auditada
        LEFT JOIN plantas p ON p.id = s.planta_id
        WHERE COALESCE(ac.fecha_cierre_programado, ac.fecha_auditoria) BETWEEN %(start)s AND %(end)s
        AND (%(planta_id)s IS NULL OR p.id = %(planta_id)s)
    """
    capa_params = {
        "start": params_dict["start"],
        "end": params_dict["end"],
        "planta_id": params_dict["planta_id"],
    }
    if es_mi_cronograma:
        capa_query += """
        AND (
            %(full_name)s IS NOT NULL
            AND (
                %(full_name)s = ac.responsable_area
                OR %(full_name)s = ac.responsable_verificacion
                OR %(full_name)s = ac.responsable_ejecucion
                OR %(full_name)s = ac.auditor_lider
                OR FIND_IN_SET(%(full_name)s, REPLACE(COALESCE(ac.auditor_lider, ''), ', ', ',')) > 0
            )
        )
        """
        capa_params["full_name"] = params_dict["full_name"]

    capas = fetch_all(capa_query, capa_params)
    for capa in capas:
        capa_date = capa.get('fecha_cierre_programado') or capa.get('fecha_auditoria')
        if not capa_date:
            continue
        start_date = capa_date.isoformat() if hasattr(capa_date, 'isoformat') else str(capa_date)
        end_date = ((capa_date + datetime.timedelta(days=1)).isoformat() if hasattr(capa_date, 'isoformat') else start_date)
        status_label = capa.get('calendar_status') or 'En gestión'
        status_class = status_class_from_label(status_label, 'capa')
        events.append({
            'id': f"capa_{capa['id']}",
            'title': f"CAPA #{capa['id']}",
            'start': start_date,
            'end': end_date,
            'allDay': True,
            'backgroundColor': '#8b5cf6',
            'borderColor': '#8b5cf6',
            'extendedProps': {
                'item_kind': 'capa',
                'ac_id': capa['id'],
                'control_id': capa.get('control_id'),
                'tipo_control': 'CAPA',
                'calendar_status': status_label,
                'status_class': status_class,
                'tipo_hallazgo': capa.get('tipo_hallazgo'),
                'sector_nombre': capa.get('sector_nombre') or capa.get('area_auditada'),
                'controlador_nombre': capa.get('auditor_lider'),
                'controlado_nombre': capa.get('responsable_area'),
            }
        })

    return jsonify(events)

@app.route("/nueva-auditoria", methods=["GET", "POST"])
@login_required
@role_required('superadmin', 'admin', 'plant_manager')
def nueva_auditoria():
    if request.method == "POST":
        planta_id = request.form.get("planta_id")
        sector_id = request.form.get("sector_id")
        fecha_control = request.form.get("fecha_control")
        fecha_fin_control = request.form.get("fecha_fin_control") or None
        controlado_id = request.form.get("controlado_id")
        controlador_id = g.user['id']
        observaciones_generales = request.form.get("observaciones_generales")
        event_id = request.form.get("event_id")

        # AUDITORIA INFO
        tipo_control = "A"
        auditor_jefe_nombre = merge_person_names(request.form.get("auditor_jefe_nombre"), request.form.getlist("auditor_jefe_ids"))
        auditor_acompanante_nombre = merge_person_names(request.form.get("auditor_acompanante_nombre"), request.form.getlist("auditor_acompanante_ids"))
        auditor_formacion_nombre = merge_person_names(request.form.get("auditor_formacion_nombre"), request.form.getlist("auditor_formacion_ids"))
        riesgos_pdf_path = None

        # Handle PDF Upload
        if 'riesgos_pdf' in request.files:
            file = request.files['riesgos_pdf']
            if file and file.filename != '':
                filename = secure_filename(f"riesgos_{int(time.time())}_{file.filename}")
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                riesgos_pdf_path = filename

        # SAVING AUDIT
        control_id = execute_query("""
            INSERT INTO controles (
                planta_id, sector_id, fecha_control, fecha_fin_control,
                responsable_id, controlador_id, controlado_id,
                observaciones_generales, tipo_control, riesgos_pdf_path,
                auditor_jefe_nombre, auditor_acompanante_nombre, auditor_formacion_nombre,
                estado_flujo
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            planta_id, sector_id, fecha_control, fecha_fin_control,
            controlador_id, controlador_id, controlado_id,
            observaciones_generales, tipo_control, riesgos_pdf_path,
            auditor_jefe_nombre, auditor_acompanante_nombre, auditor_formacion_nombre,
            'A Confirmar'
        ))

        # SENDING EMAILS
        try:
            emails = set()
            # Controlado
            controlado_data = fetch_one("SELECT email FROM usuarios WHERE id = %s", (controlado_id,))
            if controlado_data and controlado_data['email']:
                emails.add(controlado_data['email'])
                
            # Auditores
            nombres_auditores = []
            for raw_group in [auditor_jefe_nombre, auditor_acompanante_nombre, auditor_formacion_nombre]:
                nombres_auditores.extend(split_multi_names(raw_group))
            if nombres_auditores:
                query_aus = "SELECT email FROM usuarios WHERE CONCAT(nombre, ' ', apellido) IN (%s)" % ','.join(['%s']*len(nombres_auditores))
                aus_data = fetch_all(query_aus, tuple(nombres_auditores))
                for ad in aus_data:
                    if ad['email']: emails.add(ad['email'])
                    
            # Calidad (Admins)
            admins = fetch_all("SELECT email FROM usuarios WHERE rol_control IN ('admin', 'superadmin') AND activo = 1")
            for admin in admins:
                if admin['email']: emails.add(admin['email'])
                
            if emails:
                enlace = url_for('detalle_control', control_id=control_id, _external=True)
                asunto = "Auditoría Pendiente de Confirmación"
                cuerpo = f"""
                <div style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #0b5ed7;">Nueva Auditoría a Confirmar</h2>
                    <p>Se ha planificado una nueva auditoría y requiere confirmación por parte de Calidad.</p>
                    <p><strong>Fecha Planificada:</strong> {fecha_control} al {fecha_fin_control or fecha_control}</p>
                    <p>Por favor, ingrese al sistema para confirmar o reprogramar la auditoría:</p>
                    <div style="margin: 25px 0;">
                        <a href="{enlace}" style="background-color: #0d6efd; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold;">Ver Auditoría</a>
                    </div>
                </div>
                """
                for email in emails:
                    threading.Thread(target=enviar_correo, args=(email, asunto, cuerpo)).start()
        except Exception as e:
            print(f"Error enviando correos auditoria: {e}")

        # UPDATE CRONOGRAMA if from event
        if event_id:
            execute_query("""
                UPDATE cronograma_semanal 
                SET tipo_control = 'R', control_id = %s 
                WHERE id = %s OR parent_id = %s AND fecha_inicio = %s
            """, (control_id, event_id, event_id, fecha_control))

        flash("Auditoría guardada correctamente.", "success")
        return redirect(url_for("historial"))

    sectores = fetch_all("SELECT id, nombre FROM sectores WHERE activo = 1 ORDER BY nombre")
    usuarios = fetch_all("SELECT id, nombre, apellido, sector_id FROM usuarios WHERE activo = 1 ORDER BY nombre")
    return render_template("nueva_auditoria.html", sectores=sectores, usuarios=usuarios)

@app.route("/control/publico/<int:control_id>")
def detalle_control_publico(control_id):
    control = repo.get_control_detail_public(app, control_id)

    if not control:
        return "Control no encontrado.", 404

    personal = repo.get_control_personal(app, control_id)
    documentos = repo.get_control_documentos(app, control_id)
    quimicos = repo.get_control_quimicos(app, control_id)
    agenda_existente = repo.parse_audit_agenda(control)
    can_manage_audit_plan = can_edit_audit_plan(control)

    return render_template(
        "detalle_control_publico.html",
        control=control,
        personal=personal,
        documentos=documentos,
        quimicos=quimicos,
        agenda_auditoria=agenda_existente
    )

@app.route("/control/<int:control_id>")
@login_required
def detalle_control(control_id):
    control = repo.get_control_detail(app, control_id)

    if not control:
        flash("El control no existe.", "danger")
        return redirect(url_for("historial"))

    personal = repo.get_control_personal(app, control_id)
    documentos = repo.get_control_documentos(app, control_id)
    quimicos = repo.get_control_quimicos(app, control_id)
    agenda_existente = repo.parse_audit_agenda(control)
    can_manage_audit_plan = can_edit_audit_plan(control)
    can_create_capa = can_create_audit_capa(control)
    hallazgos = repo.get_hallazgos_with_capa(app, control_id)

    return render_template(
        "detalle_control.html",
        control=control,
        personal=personal,
        documentos=documentos,
        quimicos=quimicos,
        agenda_auditoria=agenda_existente,
        hallazgos=hallazgos,
        can_manage_audit_plan=can_manage_audit_plan,
        can_create_capa=can_create_capa
    )

@app.route("/auditoria/<int:control_id>/confirmar", methods=["POST"])
@login_required
@role_required('superadmin')
def confirmar_auditoria(control_id):
    control = fetch_one("SELECT * FROM controles WHERE id = %s AND tipo_control = 'A'", (control_id,))
    if not control:
        flash("Auditoría no encontrada.", "danger")
        return redirect(url_for("historial"))
        
    execute_query("UPDATE controles SET estado_flujo = 'Confirmada' WHERE id = %s", (control_id,))
    
    try:
        emails = set()
        controlado_data = fetch_one("SELECT email FROM usuarios WHERE id = %s", (control['controlado_id'],))
        if controlado_data and controlado_data['email']: emails.add(controlado_data['email'])
            
        nombres_crudos = [n for n in [control['auditor_jefe_nombre'], control['auditor_acompanante_nombre'], control['auditor_formacion_nombre']] if n]
        nombres_auditores = []
        for crudo in nombres_crudos:
            nombres_auditores.extend(split_multi_names(crudo))
                
        if nombres_auditores:
            query_aus = "SELECT email FROM usuarios WHERE CONCAT(nombre, ' ', apellido) IN (%s)" % ','.join(['%s']*len(nombres_auditores))
            aus_data = fetch_all(query_aus, tuple(nombres_auditores))
            for ad in aus_data:
                if ad['email']: emails.add(ad['email'])
        
        if emails:
            enlace = url_for('detalle_control_publico', control_id=control_id, _external=True)
            asunto = "Auditoría Confirmada"
            cuerpo = f"""
            <div style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto;">
                <h2 style="color: #10b981;">Auditoría Confirmada</h2>
                <p>La auditoría planificada ha sido revisada y <strong>confirmada</strong> por Calidad.</p>
                <p><strong>Fecha Planificada:</strong> {control['fecha_control']} al {control['fecha_fin_control'] or control['fecha_control']}</p>
                <div style="margin: 25px 0;">
                    <a href="{enlace}" style="background-color: #10b981; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold;">Ver Auditoría</a>
                </div>
            </div>
            """
            for email in emails:
                threading.Thread(target=enviar_correo, args=(email, asunto, cuerpo)).start()
    except Exception as e:
        print(f"Error correos confirmar: {e}")

    flash("Auditoría confirmada correctamente.", "success")
    return redirect(url_for("detalle_control", control_id=control_id))

@app.route("/auditoria/<int:control_id>/reprogramar", methods=["POST"])
@login_required
@role_required('superadmin')
def reprogramar_auditoria(control_id):
    control = fetch_one("SELECT * FROM controles WHERE id = %s AND tipo_control = 'A'", (control_id,))
    if not control:
        flash("Auditoría no encontrada.", "danger")
        return redirect(url_for("historial"))
        
    nueva_fecha_inicio = request.form.get("nueva_fecha_inicio")
    nueva_fecha_fin = request.form.get("nueva_fecha_fin") or nueva_fecha_inicio
    
    if not nueva_fecha_inicio:
        flash("La fecha de inicio es requerida.", "danger")
        return redirect(url_for("detalle_control", control_id=control_id))

    execute_query("""
        UPDATE controles 
        SET fecha_control = %s, fecha_fin_control = %s, estado_flujo = 'Reprogramada' 
        WHERE id = %s
    """, (nueva_fecha_inicio, nueva_fecha_fin, control_id))
    
    # Update current schedule if it exists
    execute_query("""
        UPDATE cronograma_semanal
        SET fecha_inicio = %s, fecha_fin = %s
        WHERE control_id = %s
    """, (nueva_fecha_inicio, nueva_fecha_fin, control_id))
    
    try:
        emails = set()
        controlado_data = fetch_one("SELECT email FROM usuarios WHERE id = %s", (control['controlado_id'],))
        if controlado_data and controlado_data['email']: emails.add(controlado_data['email'])
            
        nombres_crudos = [n for n in [control['auditor_jefe_nombre'], control['auditor_acompanante_nombre'], control['auditor_formacion_nombre']] if n]
        nombres_auditores = []
        for crudo in nombres_crudos:
            nombres_auditores.extend(split_multi_names(crudo))
                
        if nombres_auditores:
            query_aus = "SELECT email FROM usuarios WHERE CONCAT(nombre, ' ', apellido) IN (%s)" % ','.join(['%s']*len(nombres_auditores))
            aus_data = fetch_all(query_aus, tuple(nombres_auditores))
            for ad in aus_data:
                if ad['email']: emails.add(ad['email'])
        
        if emails:
            enlace = url_for('detalle_control_publico', control_id=control_id, _external=True)
            asunto = "Auditoría Reprogramada"
            cuerpo = f"""
            <div style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto;">
                <h2 style="color: #f59e0b;">Auditoría Reprogramada</h2>
                <p>La auditoría originalmente planificada para el {control['fecha_control']} ha sido <strong>reprogramada</strong>.</p>
                <p><strong>Nuevas Fechas:</strong> {nueva_fecha_inicio} al {nueva_fecha_fin}</p>
                <div style="margin: 25px 0;">
                    <a href="{enlace}" style="background-color: #f59e0b; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; font-weight: bold;">Ver Auditoría</a>
                </div>
            </div>
            """
            for email in emails:
                threading.Thread(target=enviar_correo, args=(email, asunto, cuerpo)).start()
    except Exception as e:
        print(f"Error correos reprogramar: {e}")

    flash("Auditoría reprogramada correctamente.", "success")
    return redirect(url_for("detalle_control", control_id=control_id))

@app.route("/auditoria/editar/<int:control_id>", methods=["GET", "POST"])
@login_required
@role_required('superadmin', 'admin')
def editar_auditoria(control_id):
    control = fetch_one("""
        SELECT c.*
        FROM controles c
        WHERE c.id = %s AND c.tipo_control = 'A'
    """, (control_id,))

    if not control:
        flash("La auditoría no existe o no es de tipo A.", "danger")
        return redirect(url_for("historial"))

    plantas = fetch_all("SELECT id, nombre FROM plantas WHERE activa = 1 ORDER BY nombre")
    sectores = fetch_all("SELECT id, planta_id, nombre FROM sectores WHERE activo = 1 ORDER BY nombre")
    usuarios = fetch_all("SELECT id, nombre, apellido, sector_id FROM usuarios WHERE activo = 1 ORDER BY nombre, apellido")

    if request.method == "POST":
        try:
            sector_id = request.form["sector_id"]
            fecha_control = request.form["fecha_control"]
            fecha_fin_control = request.form.get("fecha_fin_control") or None
            
            controlador_id = request.form.get("controlador_id") or None
            auditor_jefe_nombre = merge_person_names(request.form.get("auditor_jefe_nombre"), request.form.getlist("auditor_jefe_ids"))

            auditor_acompanante_id = None
            auditor_acompanante_nombre = merge_person_names(request.form.get("auditor_acompanante_nombre"), request.form.getlist("auditor_acompanante_ids"))

            auditor_formacion_id = None
            auditor_formacion_nombre = merge_person_names(request.form.get("auditor_formacion_nombre"), request.form.getlist("auditor_formacion_ids"))

            observaciones_generales = request.form.get("observaciones_generales") or None

            # Handle PDF
            riesgos_pdf_path = control['riesgos_pdf_path']
            if 'riesgos_pdf' in request.files:
                file = request.files['riesgos_pdf']
                if file.filename != '':
                    import os
                    from werkzeug.utils import secure_filename
                    filename = secure_filename(f"riesgos_auditoria_{control_id}_{file.filename}")
                    upload_folder = os.path.join(app.root_path, 'static', 'uploads', 'riesgos')
                    os.makedirs(upload_folder, exist_ok=True)
                    file_path = os.path.join(upload_folder, filename)
                    file.save(file_path)
                    riesgos_pdf_path = f"uploads/riesgos/{filename}"

            execute_query("""
                UPDATE controles
                SET sector_id = %s, fecha_control = %s, fecha_fin_control = %s,
                    controlador_id = %s, auditor_jefe_nombre = %s,
                    auditor_acompanante_id = %s, auditor_acompanante_nombre = %s,
                    auditor_formacion_id = %s, auditor_formacion_nombre = %s,
                    observaciones_generales = %s, riesgos_pdf_path = %s
                WHERE id = %s
            """, (
                sector_id, fecha_control, fecha_fin_control,
                controlador_id, auditor_jefe_nombre,
                auditor_acompanante_id, auditor_acompanante_nombre,
                auditor_formacion_id, auditor_formacion_nombre,
                observaciones_generales, riesgos_pdf_path,
                control_id
            ))

            log_action("EDITAR_AUDITORIA", f"Auditoría ID: {control_id}")
            flash("Auditoría editada correctamente.", "success")
            return redirect(url_for("detalle_control", control_id=control_id))

        except Exception as exc:
            flash(f"Error al editar la auditoría: {exc}", "danger")

    return render_template(
        "editar_auditoria.html",
        control=control,
        plantas=plantas,
        sectores=sectores,
        usuarios=usuarios
    )

@app.route("/auditoria/<int:control_id>/plan", methods=["GET", "POST"])
@login_required
def plan_auditoria(control_id):
    control = fetch_one("""
        SELECT c.*, p.nombre AS planta_nombre, s.nombre AS sector_nombre
        FROM controles c
        JOIN plantas p ON c.planta_id = p.id
        JOIN sectores s ON c.sector_id = s.id
        WHERE c.id = %s AND c.tipo_control = 'A'
    """, (control_id,))

    if not control:
        flash("La auditoría no existe o no es de tipo A.", "danger")
        return redirect(url_for("historial"))

    if not can_edit_audit_plan(control):
        flash("Solo el auditor jefe designado puede completar o editar el plan de auditoria.", "danger")
        return redirect(url_for("detalle_control", control_id=control_id))

    if request.method == "POST":
        try:
            sistema_gestion = request.form.get("sistema_gestion_auditoria")
            objetivo = request.form.get("objetivo_auditoria")
            criterios = request.form.get("criterios_auditoria")
            descripcion = request.form.get("descripcion_actividades_auditoria")
            recursos = request.form.get("recursos_auditoria")

            import json
            agenda_dias = request.form.getlist("agenda_dia[]")
            agenda_horas = request.form.getlist("agenda_hora[]")
            agenda_actividades = request.form.getlist("agenda_actividad[]")
            agenda_lugares = request.form.getlist("agenda_lugar[]")
            agenda_auditores = request.form.getlist("agenda_auditor[]")

            agenda_list = []
            for d, h, a, l, au in zip(agenda_dias, agenda_horas, agenda_actividades, agenda_lugares, agenda_auditores):
                if d.strip() or a.strip():
                    agenda_list.append({
                        "dia": d.strip(),
                        "hora": h.strip(),
                        "actividad": a.strip(),
                        "lugar": l.strip(),
                        "auditor": au.strip()
                    })
            
            agenda_json = json.dumps(agenda_list, ensure_ascii=False) if agenda_list else None

            execute_query("""
                UPDATE controles
                SET sistema_gestion_auditoria = %s,
                    objetivo_auditoria = %s,
                    criterios_auditoria = %s,
                    descripcion_actividades_auditoria = %s,
                    recursos_auditoria = %s,
                    agenda_auditoria = %s,
                    plan_completado_at = NOW()
                WHERE id = %s
            """, (sistema_gestion, objetivo, criterios, descripcion, recursos, agenda_json, control_id))

            flash("Plan de auditoría guardado correctamente.", "success")
            return redirect(url_for("detalle_control", control_id=control_id))

        except Exception as exc:
            flash(f"Error al guardar el plan de auditoría: {exc}", "danger")

    import json
    agenda_existente = []
    if control.get('agenda_auditoria'):
        try:
            agenda_existente = json.loads(control['agenda_auditoria'])
        except Exception:
            pass

    return render_template(
        "plan_auditoria.html",
        control=control,
        agenda_existente=agenda_existente
    )

@app.route("/auditoria/<int:control_id>/informe", methods=["GET", "POST"])
@login_required
def informe_auditoria(control_id):
    control = fetch_one("""
        SELECT c.*, p.nombre AS planta_nombre, s.nombre AS sector_nombre
        FROM controles c
        JOIN plantas p ON c.planta_id = p.id
        JOIN sectores s ON c.sector_id = s.id
        WHERE c.id = %s AND c.tipo_control = 'A'
    """, (control_id,))

    if not control:
        flash("La auditoría no existe o no es de tipo A.", "danger")
        return redirect(url_for("historial"))

    if not can_edit_audit_report(control):
        flash("Solo el auditor jefe designado puede crear o editar el informe de auditoria.", "danger")
        return redirect(url_for("detalle_control", control_id=control_id))

    hallazgos = fetch_all("""
        SELECT h.*, ac.id AS accion_correctiva_id, ac.estado_flujo AS ac_estado
        FROM hallazgos_auditoria h
        LEFT JOIN acciones_correctivas ac ON h.id = ac.hallazgo_id
        WHERE h.control_id = %s ORDER BY h.id ASC
    """, (control_id,))

    if request.method == "POST":
        try:
            conclusiones = (request.form.get("conclusiones_auditoria") or "").strip() or None
            fortalezas = (request.form.get("fortalezas_auditoria") or "").strip() or None

            hallazgo_ids = request.form.getlist("hallazgo_id[]")
            requisitos = request.form.getlist("hallazgo_requisito[]")
            tipos = request.form.getlist("hallazgo_tipo[]")
            descripciones = request.form.getlist("hallazgo_descripcion[]")

            hallazgos_normalizados = []
            for h_id, requisito, tipo, descripcion in zip(hallazgo_ids, requisitos, tipos, descripciones):
                hallazgo = {
                    "id": (h_id or "").strip(),
                    "requisito": (requisito or "").strip(),
                    "tipo": (tipo or "").strip(),
                    "descripcion": (descripcion or "").strip(),
                }
                if not any([hallazgo["requisito"], hallazgo["tipo"], hallazgo["descripcion"]]):
                    continue
                if not all([hallazgo["requisito"], hallazgo["tipo"], hallazgo["descripcion"]]):
                    flash("Cada hallazgo debe tener requisito, tipo y desvio/evidencia completos.", "danger")
                    return redirect(url_for("informe_auditoria", control_id=control_id))
                hallazgos_normalizados.append(hallazgo)

            if not hallazgos_normalizados:
                flash("El informe debe contener al menos un hallazgo completo.", "danger")
                return redirect(url_for("informe_auditoria", control_id=control_id))

            execute_query("""
                UPDATE controles
                SET conclusiones_auditoria = %s, fortalezas_auditoria = %s, informe_emitido_at = NOW()
                WHERE id = %s
            """, (conclusiones, fortalezas, control_id))

            previous_rows = fetch_all("""
                SELECT h.id, ac.id AS accion_correctiva_id
                FROM hallazgos_auditoria h
                LEFT JOIN acciones_correctivas ac ON ac.hallazgo_id = h.id
                WHERE h.control_id = %s
            """, (control_id,))
            previous_ids = {str(r['id']) for r in previous_rows}
            protected_ids = {str(r['id']) for r in previous_rows if r.get('accion_correctiva_id')}
            retained_ids = set()

            for hallazgo in hallazgos_normalizados:
                h_id = hallazgo["id"]
                if h_id and h_id in previous_ids:
                    execute_query(
                        "UPDATE hallazgos_auditoria SET requisito=%s, tipo_hallazgo=%s, descripcion=%s WHERE id=%s",
                        (hallazgo["requisito"], hallazgo["tipo"], hallazgo["descripcion"], h_id)
                    )
                    retained_ids.add(h_id)
                else:
                    execute_query(
                        "INSERT INTO hallazgos_auditoria (control_id, requisito, tipo_hallazgo, descripcion) VALUES (%s, %s, %s, %s)",
                        (control_id, hallazgo["requisito"], hallazgo["tipo"], hallazgo["descripcion"])
                    )

            skipped_deletes = []
            for remaining_id in previous_ids - retained_ids:
                if remaining_id in protected_ids:
                    skipped_deletes.append(remaining_id)
                    continue
                execute_query("DELETE FROM hallazgos_auditoria WHERE id=%s", (remaining_id,))

            log_action("ACTUALIZAR_INFORME_AUDITORIA", f"Auditoria ID: {control_id}")
            if skipped_deletes:
                flash("Informe guardado. Algunos hallazgos no se eliminaron porque ya tienen una CAPA asociada.", "warning")
            else:
                flash("Informe de auditoria actualizado correctamente.", "success")
            return redirect(url_for("detalle_control", control_id=control_id))

        except Exception as exc:
            flash(f"Error al guardar el informe: {exc}", "danger")

    return render_template(
        "informe_auditoria.html",
        control=control,
        hallazgos=hallazgos
    )

@app.route("/control/editar/<int:control_id>", methods=["GET", "POST"])
@login_required
@role_required('superadmin', 'admin')
def editar_control(control_id):
    control = fetch_one("""
        SELECT c.id, c.fecha_control, c.observaciones_generales, c.sector_tiene_quimicos,
               c.planta_id, c.sector_id, c.controlador_id, c.controlado_id, c.tipo_control
        FROM controles c
        WHERE c.id = %s
    """, (control_id,))

    if not control:
        flash("El control no existe.", "danger")
        return redirect(url_for("historial"))

    if control['tipo_control'] == 'A':
        return redirect(url_for("editar_auditoria", control_id=control_id))

    plantas = fetch_all("SELECT id, nombre FROM plantas WHERE activa = 1 ORDER BY nombre")
    sectores = fetch_all("SELECT id, planta_id, nombre FROM sectores WHERE activo = 1 ORDER BY nombre")
    usuarios = fetch_all("SELECT id, nombre, apellido FROM usuarios WHERE activo = 1 ORDER BY nombre, apellido")
    personal = fetch_all("SELECT nombre_apellido, conoce_gestion_documental, realizo_capacitacion, requiere_capacitacion, observaciones FROM personal_control WHERE control_id = %s ORDER BY id ASC", (control_id,))
    documentos = fetch_all("SELECT nombre_documento, codigo_documento, revision, copia_controlada, no_cargado_portal, estado, observaciones, imagen_path FROM documentos_control WHERE control_id = %s ORDER BY id ASC", (control_id,))
    quimicos = fetch_all("SELECT nombre_producto, bajo_llave, envase_original, etiquetado_correcto, hoja_seguridad, observaciones, medida FROM productos_quimicos WHERE control_id = %s ORDER BY id ASC", (control_id,))

    if request.method == "POST":
        try:
            sector_id = request.form["sector_id"]
            fecha_control = request.form["fecha_control"]
            controlador_id = request.form.get("controlador_id") or None
            controlado_id = request.form.get("controlado_id") or None
            sector_tiene_quimicos = 1 if request.form.get("sector_tiene_quimicos") == "si" else 0
            observaciones_generales = request.form.get("observaciones_generales") or None

            personal_nombres = request.form.getlist("personal_nombre[]")
            personal_conoce = request.form.getlist("personal_conoce[]")
            personal_cap = request.form.getlist("personal_capacitacion[]")
            personal_req = request.form.getlist("personal_requiere[]")
            personal_obs = request.form.getlist("personal_observacion[]")

            documentos_codigos = request.form.getlist("documento_codigo[]")
            documentos_revision = request.form.getlist("documento_revision[]")
            documentos_copia = request.form.getlist("documento_copia[]")
            documentos_no_cargado_portal = request.form.getlist("documento_no_cargado_portal[]")
            documentos_estado = request.form.getlist("documento_estado[]")
            documentos_obs = request.form.getlist("documento_observacion[]")
            documentos_fotos = request.files.getlist("documento_foto[]")

            quimicos_nombres = request.form.getlist("quimico_nombre[]")
            quimicos_llave = request.form.getlist("quimico_llave[]")
            quimicos_envase = request.form.getlist("quimico_envase[]")
            quimicos_etiqueta = request.form.getlist("quimico_etiqueta[]")
            quimicos_hoja = request.form.getlist("quimico_hoja[]")
            quimicos_obs = request.form.getlist("quimico_observacion[]")
            quimicos_medida = request.form.getlist("quimico_medida[]")

            total_personas = 0
            total_requieren = 0
            total_docs = 0
            total_correctos = 0
            total_incorrectos = 0

            for i, nombre in enumerate(personal_nombres):
                if nombre.strip():
                    total_personas += 1
                    if i < len(personal_req) and personal_req[i] == "si":
                        total_requieren += 1

            for i, codigo in enumerate(documentos_codigos):
                if codigo.strip():
                    total_docs += 1
                    estado = documentos_estado[i] if i < len(documentos_estado) else "correcto"
                    if estado == "correcto":
                        total_correctos += 1
                    else:
                        total_incorrectos += 1

            execute_query("""
                UPDATE controles SET
                    sector_id = %s, fecha_control = %s, controlador_id = %s, controlado_id = %s,
                    responsable_id = %s, sector_tiene_quimicos = %s, observaciones_generales = %s,
                    total_personas_entrevistadas = %s, total_requieren_capacitacion = %s,
                    total_documentos_controlados = %s, total_documentos_correctos = %s, total_documentos_incorrectos = %s
                WHERE id = %s
            """, (sector_id, fecha_control, controlador_id, controlado_id, controlador_id,
                  sector_tiene_quimicos, observaciones_generales,
                  total_personas, total_requieren, total_docs, total_correctos, total_incorrectos,
                  control_id))

            execute_query("DELETE FROM personal_control WHERE control_id = %s", (control_id,))
            personal_rows = []
            for i, nombre in enumerate(personal_nombres):
                if nombre.strip():
                    personal_rows.append((control_id, nombre.strip(),
                        1 if i < len(personal_conoce) and personal_conoce[i] == "si" else 0,
                        1 if i < len(personal_cap) and personal_cap[i] == "si" else 0,
                        1 if i < len(personal_req) and personal_req[i] == "si" else 0,
                        personal_obs[i] if i < len(personal_obs) else None))
            if personal_rows:
                execute_query("INSERT INTO personal_control (control_id, nombre_apellido, conoce_gestion_documental, realizo_capacitacion, requiere_capacitacion, observaciones) VALUES (%s, %s, %s, %s, %s, %s)", personal_rows, many=True)

            viejos_docs = fetch_all("SELECT id, codigo_documento, imagen_path FROM documentos_control WHERE control_id = %s", (control_id,))
            mapa_imagenes = { str(d['codigo_documento']): d['imagen_path'] for d in viejos_docs if d['codigo_documento'] and d['imagen_path'] }
            # Add mapping by index too as backup if code changes
            for i, d in enumerate(viejos_docs):
                mapa_imagenes[f"idx_{i}"] = d['imagen_path']

            execute_query("DELETE FROM documentos_control WHERE control_id = %s", (control_id,))
            documentos_rows = []
            for i, codigo in enumerate(documentos_codigos):
                codigo_str = codigo.strip()
                if codigo_str:
                    estado_documento = documentos_estado[i] if i < len(documentos_estado) and documentos_estado[i] else "correcto"
                    copia_controlada, no_cargado_portal = infer_document_flags(
                        estado_documento,
                        documentos_copia[i] if i < len(documentos_copia) else None,
                        documentos_no_cargado_portal[i] if i < len(documentos_no_cargado_portal) else None,
                    )
                    # Try to get image by code, then by index if same count
                    imagen_path = mapa_imagenes.get(codigo_str) or mapa_imagenes.get(f"idx_{i}")
                    if i < len(documentos_fotos) and documentos_fotos[i].filename:
                        foto = documentos_fotos[i]
                        filename = f"{int(time.time())}_{secure_filename(foto.filename)}"
                        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                        foto.save(upload_path)
                        imagen_path = filename

                    documentos_rows.append((control_id, None,
                        codigo_str,
                        documentos_revision[i].strip() if i < len(documentos_revision) and documentos_revision[i].strip() else None,
                        1 if copia_controlada else 0,
                        None, # copia_controlada_numero
                        1 if no_cargado_portal else 0,
                        None, # motivo_no_cargado
                        estado_documento,
                        documentos_obs[i].strip() if i < len(documentos_obs) and documentos_obs[i].strip() else None,
                        imagen_path))
            if documentos_rows:
                execute_query("INSERT INTO documentos_control (control_id, nombre_documento, codigo_documento, revision, copia_controlada, copia_controlada_numero, no_cargado_portal, motivo_no_cargado, estado, observaciones, imagen_path) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", documentos_rows, many=True)

            execute_query("DELETE FROM productos_quimicos WHERE control_id = %s", (control_id,))
            quimicos_rows = []
            if sector_tiene_quimicos == 1:
                for i, nombre in enumerate(quimicos_nombres):
                    if nombre.strip():
                        quimicos_rows.append((control_id, nombre.strip(),
                            1 if i < len(quimicos_llave) and quimicos_llave[i] == "si" else 0,
                            1 if i < len(quimicos_envase) and quimicos_envase[i] == "si" else 0,
                            1 if i < len(quimicos_etiqueta) and quimicos_etiqueta[i] == "si" else 0,
                            1 if i < len(quimicos_hoja) and quimicos_hoja[i] == "si" else 0,
                            quimicos_obs[i] if i < len(quimicos_obs) else None,
                            quimicos_medida[i] if i < len(quimicos_medida) else None))
            if quimicos_rows:
                execute_query("INSERT INTO productos_quimicos (control_id, nombre_producto, bajo_llave, envase_original, etiquetado_correcto, hoja_seguridad, observaciones, medida) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", quimicos_rows, many=True)

            log_action("EDITAR_CONTROL", f"Control ID: {control_id}")
            flash("Control actualizado correctamente.", "success")
            return redirect(url_for("detalle_control", control_id=control_id))

        except Exception as exc:
            flash(f"Error al guardar los cambios: {exc}", "danger")

    return render_template(
        "editar_control.html",
        control=control,
        plantas=plantas,
        sectores=sectores,
        usuarios=usuarios,
        personal=personal,
        documentos=documentos,
        quimicos=quimicos
    )

@app.route("/control/eliminar/<int:control_id>", methods=["POST"])
@login_required
@role_required('superadmin')
def eliminar_control(control_id):

    password = request.form.get("password")
    
    # Verify password
    if not check_password_hash(g.user['password_hash'], password):
        flash("Contraseña incorrecta. No se pudo eliminar el control.", "danger")
        return redirect(url_for('detalle_control', control_id=control_id))
    
    try:
        # 1. Revert linked calendar event (if any)
        execute_query("""
            UPDATE cronograma_semanal 
            SET tipo_control = 'P', control_id = NULL 
            WHERE control_id = %s
        """, (control_id,))
        
        # 2. Delete child records
        execute_query("DELETE FROM personal_control WHERE control_id = %s", (control_id,))
        execute_query("DELETE FROM documentos_control WHERE control_id = %s", (control_id,))
        execute_query("DELETE FROM productos_quimicos WHERE control_id = %s", (control_id,))
        
        # 3. Delete the control itself
        execute_query("DELETE FROM controles WHERE id = %s", (control_id,))
        
        log_action("ELIMINAR_CONTROL", f"Control ID: {control_id}")
        flash("Control eliminado correctamente y cronograma actualizado.", "success")
        return redirect(url_for('historial'))
    except Exception as e:
        flash(f"Error al eliminar el control: {e}", "danger")
        return redirect(url_for('detalle_control', control_id=control_id))

def safe_int_id(v):
    try:
        if v is None:
            return None
        s_v = str(v).strip()
        if s_v == "" or s_v == "null" or s_v == "undefined":
            return None
        return int(v)
    except (ValueError, Error):
        return None

def upsert_cronograma_event(
    sector_id,
    fecha_inicio,
    fecha_fin,
    hora_inicio,
    hora_fin,
    titulo,
    controlador_id,
    controlado_id,
    recurrencia,
    recurrencia_fin,
    tipo_control,
    plan_auditoria=None,
    auditor_jefe_nombre=None,
    auditor_acompanante_nombre=None,
    auditor_formacion_nombre=None,
    event_id=None
):
    if event_id:
        execute_query("""
            UPDATE cronograma_semanal
            SET sector_id=%s, fecha_inicio=%s, fecha_fin=%s, hora_inicio=%s, hora_fin=%s, titulo=%s, controlador_id=%s, controlado_id=%s, recurrencia=%s, recurrencia_fin=%s, tipo_control=%s, plan_auditoria=%s,
                auditor_jefe_nombre=%s, auditor_acompanante_nombre=%s, auditor_formacion_nombre=%s
            WHERE id=%s
        """, (
            sector_id, fecha_inicio, fecha_fin, hora_inicio, hora_fin, titulo, controlador_id, controlado_id,
            recurrencia, recurrencia_fin, tipo_control, plan_auditoria,
            auditor_jefe_nombre, auditor_acompanante_nombre, auditor_formacion_nombre, event_id
        ))
        return event_id

    return execute_query("""
        INSERT INTO cronograma_semanal (
            sector_id, fecha_inicio, fecha_fin, hora_inicio, hora_fin, titulo, controlador_id, controlado_id,
            recurrencia, recurrencia_fin, tipo_control, plan_auditoria,
            auditor_jefe_nombre, auditor_acompanante_nombre, auditor_formacion_nombre
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        sector_id, fecha_inicio, fecha_fin, hora_inicio, hora_fin, titulo, controlador_id, controlado_id,
        recurrencia, recurrencia_fin, tipo_control, plan_auditoria,
        auditor_jefe_nombre, auditor_acompanante_nombre, auditor_formacion_nombre
    ))

def notify_programacion(controlado_id, controlador_id, sector_id, fecha_inicio, fecha_fin, tipo_control):
    if not controlado_id:
        return

    resp = fetch_one("SELECT nombre, email FROM usuarios WHERE id = %s", (controlado_id,))
    controlador = fetch_one("SELECT nombre FROM usuarios WHERE id = %s", (controlador_id,))
    c_name = controlador['nombre'] if controlador else "Alguien"

    if resp and resp['email']:
        try:
            s_name = "un sector (pendiente de asignar)"
            if sector_id:
                sector = fetch_one("SELECT nombre FROM sectores WHERE id = %s", (sector_id,))
                if sector:
                    s_name = f"el sector <strong>{sector['nombre']}</strong>"

            tipo_label = "auditoría" if tipo_control == 'A' else "inspección"
            cuerpo = f"<h3>{tipo_label.capitalize()} Programada</h3><p>Hola {resp['nombre']}, se ha programado una {tipo_label} en {s_name} desde el {fecha_inicio} hasta el {fecha_fin}.</p><p>El controlador asignado es: {c_name}.</p>"
            enviar_correo(resp['email'], f"Aviso: Nueva {tipo_label.capitalize()} Programada", cuerpo)
        except Exception as email_err:
            print(f"Error al enviar correo de programaciÃ³n: {email_err}")

def infer_planta_id_from_sector(sector_id):
    sector_id = safe_int_id(sector_id)
    if not sector_id:
        return None
    sector = fetch_one("SELECT planta_id FROM sectores WHERE id = %s", (sector_id,))
    return sector['planta_id'] if sector else None

def split_multi_names(raw_value):
    return permission_utils.split_multi_names(raw_value)

def user_name_matches(raw_value, full_name):
    return permission_utils.user_name_matches(raw_value, full_name)

def merge_person_names(raw_value, selected_ids):
    names = split_multi_names(raw_value)
    ids = []
    if isinstance(selected_ids, (list, tuple)):
        ids = [safe_int_id(v) for v in selected_ids]
    else:
        maybe_id = safe_int_id(selected_ids)
        ids = [maybe_id] if maybe_id else []

    ids = [v for v in ids if v]
    if ids:
        placeholders = ", ".join(["%s"] * len(ids))
        linked_users = fetch_all(
            f"SELECT CONCAT(nombre, ' ', apellido) AS full_name FROM usuarios WHERE id IN ({placeholders})",
            tuple(ids)
        )
        for user in linked_users:
            full_name = (user.get('full_name') or '').strip()
            if full_name:
                names.append(full_name)

    unique_names = []
    seen = set()
    for name in names:
        key = name.lower()
        if key not in seen:
            seen.add(key)
            unique_names.append(name)

    return ", ".join(unique_names) if unique_names else None

def sql_multi_name_match(column_name, param_name="full_name"):
    normalized_column = (
        f"REPLACE(REPLACE(REPLACE(REPLACE(LOWER(COALESCE({column_name}, '')), CHAR(13), ','), "
        f"CHAR(10), ','), ', ', ','), ',,', ',')"
    )
    return f"FIND_IN_SET(LOWER(TRIM(%({param_name})s)), {normalized_column}) > 0"

def current_user_full_name():
    return permission_utils.current_user_full_name(g.user)

def user_is_audit_lead(control):
    return permission_utils.is_audit_lead(g.user, control)


def can_edit_audit_plan(control):
    return permission_utils.can_edit_audit_plan(g.user, control)


def can_edit_audit_report(control):
    return permission_utils.can_edit_audit_report(g.user, control)


def can_create_audit_capa(control):
    return permission_utils.can_create_capa(g.user, control)


def parse_capa_evidence_photos(raw_value):
    if not raw_value:
        return []
    try:
        loaded = json.loads(raw_value)
        if isinstance(loaded, list):
            return [str(item).strip() for item in loaded if str(item).strip()]
    except Exception:
        pass
    return []

@app.route("/api/cronograma/toggle", methods=["POST"])
@login_required
@role_required('superadmin', 'admin')
def toggle_cronograma():
    try:
        data = request.json

        sector_id = safe_int_id(data.get('sector_id'))
        fecha_inicio = data.get('fecha_inicio')
        fecha_fin = data.get('fecha_fin')
        hora_inicio = data.get('hora_inicio') or None
        hora_fin = data.get('hora_fin') or None
        titulo = data.get('titulo')
        
        new_state = data.get('new_state')
        controlador_id = safe_int_id(data.get('controlador_id'))
        controlado_id = safe_int_id(data.get('controlado_id'))
        tipo_control = data.get('tipo_control') or data.get('new_state') or 'A'
        plan_auditoria = data.get('plan_auditoria')
        auditor_jefe_nombre = data.get('auditor_jefe_nombre')
        auditor_acompanante_nombre = data.get('auditor_acompanante_nombre')
        auditor_formacion_nombre = data.get('auditor_formacion_nombre')
        control_id = safe_int_id(data.get('control_id'))
        event_id = safe_int_id(data.get('event_id'))

        recurrencia = data.get('recurrencia') or None
        recurrencia_fin = data.get('recurrencia_fin') or None

        if new_state == 'E':
            instance_date = data.get('instance_date')
            
            if event_id and instance_date:
                # INSTANCE DELETION: Create an exception record
                template = fetch_one("SELECT * FROM cronograma_semanal WHERE id = %s", (event_id,))
                if template and template['recurrencia']:
                    inst_date = datetime.date.fromisoformat(instance_date)
                    execute_query("""
                        INSERT INTO cronograma_semanal (sector_id, fecha_inicio, fecha_fin, tipo_control, parent_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (template['sector_id'], inst_date, inst_date, 'E', event_id))
                    return jsonify({"status": "success"})

            # FULL DELETION
            if event_id:
                execute_query("DELETE FROM cronograma_semanal WHERE id = %s", (event_id,))
            return jsonify({"status": "success"})

        if new_state == 'P' or new_state == 'A':
            event_id = upsert_cronograma_event(
                sector_id=sector_id,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                hora_inicio=hora_inicio,
                hora_fin=hora_fin,
                titulo=titulo,
                controlador_id=controlador_id,
                controlado_id=controlado_id,
                recurrencia=recurrencia,
                recurrencia_fin=recurrencia_fin,
                tipo_control=new_state,
                plan_auditoria=data.get('plan_auditoria'),
                auditor_jefe_nombre=data.get('auditor_jefe_nombre'),
                auditor_acompanante_nombre=data.get('auditor_acompanante_nombre'),
                auditor_formacion_nombre=data.get('auditor_formacion_nombre'),
                event_id=event_id
            )
            
            # Send Email to CONTROLADO
            if controlado_id:
                resp = fetch_one("SELECT nombre, email FROM usuarios WHERE id = %s", (controlado_id,))
                controlador = fetch_one("SELECT nombre FROM usuarios WHERE id = %s", (controlador_id,))
                c_name = controlador['nombre'] if controlador else "Alguien"
                
                if resp and resp['email']:
                    try:
                        s_name = "un sector (pendiente de asignar)"
                        if sector_id:
                            sector = fetch_one("SELECT nombre FROM sectores WHERE id = %s", (sector_id,))
                            if sector: s_name = f"el sector <strong>{sector['nombre']}</strong>"
                            
                        cuerpo = f"<h3>Control Programado</h3><p>Hola {resp['nombre']}, se ha programado una inspección en {s_name} desde el {fecha_inicio} hasta el {fecha_fin}.</p><p>El controlador asignado es: {c_name}.</p>"
                        enviar_correo(resp['email'], "Aviso: Nuevo Control Programado", cuerpo)
                    except Exception as email_err:
                        print(f"Error al enviar correo de programación: {email_err}")
                    
        elif new_state == 'R':
            # Mark as realized
            instance_date = data.get('instance_date')
            
            if event_id and instance_date:
                # RECURRING INSTANCE CASE: Clone template to new realized record
                template = fetch_one("SELECT * FROM cronograma_semanal WHERE id = %s", (event_id,))
                if template and template['recurrencia']:
                    # Calculate duration to preserve it in the clone
                    inst_start = datetime.date.fromisoformat(instance_date)
                    duration = template['fecha_fin'] - template['fecha_inicio']
                    inst_end = inst_start + duration
                    
                    # Create exception record
                    execute_query("""
                        INSERT INTO cronograma_semanal (sector_id, fecha_inicio, fecha_fin, hora_inicio, hora_fin, titulo, tipo_control, controlador_id, controlado_id, control_id, parent_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (template['sector_id'], inst_start, inst_end, template['hora_inicio'], template['hora_fin'], 
                          template['titulo'], 'R', template['controlador_id'], template['controlado_id'], control_id, event_id))
                    
                    # Update curr for email purposes
                    curr = {'controlado_id': template['controlado_id'], 'fecha_inicio': instance_date}
                else:
                    # Not recurring or not found, fallback to standard update
                    execute_query("UPDATE cronograma_semanal SET tipo_control = 'R', control_id = %s WHERE id = %s", (control_id, event_id))
                    curr = fetch_one("SELECT controlado_id, fecha_inicio FROM cronograma_semanal WHERE id = %s", (event_id,))
            elif event_id:
                # SINGLE EVENT CASE
                execute_query("UPDATE cronograma_semanal SET tipo_control = 'R', control_id = %s WHERE id = %s", (control_id, event_id))
                curr = fetch_one("SELECT controlado_id, fecha_inicio FROM cronograma_semanal WHERE id = %s", (event_id,))
            else:
                curr = None

            # Send validation email to CONTROLADO
            if curr and curr['controlado_id']:
                    resp = fetch_one("SELECT nombre, email FROM usuarios WHERE id = %s", (curr['controlado_id'],))
                    if resp and resp['email']:
                        try:
                            if control_id:
                                link = url_for('detalle_control', control_id=control_id, _external=True)
                            else:
                                link = url_for('historial', _external=True)
                            cuerpo = f"<h3>Inspección Completada</h3><p>Hola {resp['nombre']}, se ha dado por completado el control iniciado el {curr['fecha_inicio']}. Puedes revisar la información <a href='{link}'>ingresando aquí</a>.</p>"
                            enviar_correo(resp['email'], "Inspección Completada", cuerpo)
                        except Exception as email_err:
                            print(f"Error al enviar correo de completado: {email_err}")
                    
        log_action("MODIFICAR_CRONOGRAMA", f"ID: {event_id}, Estado: {new_state}")
        return jsonify({"status": "success", "event_id": event_id})
    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        print(f"CRITICAL ERROR in toggle_cronograma: {err_msg}")
        return jsonify({"status": "error", "message": str(e), "trace": err_msg}), 500
        
@app.route("/api/auditoria/bulk_create", methods=["POST"])
@app.route("/api/cronograma/bulk_create", methods=["POST"])
@login_required
@role_required('superadmin', 'admin')
def bulk_create_auditorias():
    try:
        data = request.json
        assignments = data.get('assignments', []) # Expected: [{'sector_id': X, 'fecha': 'YYYY-MM-DD'}, ...]
        controlador_id = safe_int_id(data.get('controlador_id'))
        controlado_id = safe_int_id(data.get('controlado_id'))
        titulo = data.get('titulo', 'Auditoría Programada')

        tipo_control = data.get('tipo_control') or data.get('new_state') or 'A'
        plan_auditoria = data.get('plan_auditoria')
        auditor_jefe_nombre = data.get('auditor_jefe_nombre')
        auditor_acompanante_nombre = data.get('auditor_acompanante_nombre')
        auditor_formacion_nombre = data.get('auditor_formacion_nombre')

        if not assignments:
            return jsonify({"status": "error", "message": "No se han proporcionado asignaciones."}), 400

        created_events = []
        for asgn in assignments:
            s_id = safe_int_id(asgn.get('sector_id'))
            fecha_str = asgn.get('fecha')
            fecha_fin_str = asgn.get('fecha_fin') or fecha_str
            hora_inicio = asgn.get('hora_inicio') or data.get('hora_inicio')
            hora_fin = asgn.get('hora_fin') or data.get('hora_fin')
            
            if not s_id or not fecha_str: continue

            ev_id = upsert_cronograma_event(
                sector_id=s_id,
                fecha_inicio=fecha_str,
                fecha_fin=fecha_fin_str,
                hora_inicio=hora_inicio,
                hora_fin=hora_fin,
                titulo=titulo,
                controlador_id=controlador_id,
                controlado_id=controlado_id,
                recurrencia=None,
                recurrencia_fin=None,
                tipo_control=tipo_control,
                plan_auditoria=plan_auditoria,
                auditor_jefe_nombre=auditor_jefe_nombre,
                auditor_acompanante_nombre=auditor_acompanante_nombre,
                auditor_formacion_nombre=auditor_formacion_nombre,
                event_id=None
            )
            created_events.append(ev_id)

        log_action("CREACION_MASIVA_CRONOGRAMA", f"Asignaciones creadas: {len(created_events)}, tipo: {tipo_control}")
        return jsonify({"status": "success", "count": len(created_events)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/mi-cronograma")
@login_required
def mi_cronograma():
    current_year = datetime.datetime.now().year
    plantas = fetch_all("SELECT id, nombre FROM plantas WHERE activa = 1 ORDER BY nombre")
    sectores = fetch_all("SELECT id, planta_id, nombre FROM sectores WHERE activo = 1 ORDER BY nombre")
    usuarios_activos = fetch_all("""
        SELECT id, nombre, apellido, email, sector_id
        FROM usuarios
        WHERE activo = 1
        ORDER BY nombre, apellido
    """)

    full_name = f"{g.user['nombre']} {g.user['apellido']}".strip()

    controles = fetch_all(f"""
        SELECT
            c.id,
            c.fecha_control,
            p.nombre AS planta,
            s.nombre AS sector,
            c.total_documentos_controlados,
            c.total_documentos_correctos,
            c.total_documentos_incorrectos,
            c.total_personas_entrevistadas,
            c.total_requieren_capacitacion,
            CONCAT(COALESCE(u.nombre, ''), ' ', COALESCE(u.apellido, '')) AS responsable,
            (SELECT COUNT(*) FROM documentos_control dc WHERE dc.control_id = c.id AND dc.imagen_path IS NOT NULL) > 0 AS tiene_fotos
        FROM controles c
        INNER JOIN plantas p ON p.id = c.planta_id
        INNER JOIN sectores s ON s.id = c.sector_id
        LEFT JOIN usuarios u ON u.id = c.controlador_id
        WHERE
            c.controlador_id = %(user_id)s
            OR c.controlado_id = %(user_id)s
            OR {sql_multi_name_match('c.auditor_jefe_nombre', 'full_name')}
            OR {sql_multi_name_match('c.auditor_acompanante_nombre', 'full_name')}
            OR {sql_multi_name_match('c.auditor_formacion_nombre', 'full_name')}
        ORDER BY c.fecha_control DESC, c.id DESC
    """, {
        'user_id': g.user['id'],
        'full_name': full_name
    })

    return render_template(
        "historial.html",
        controles=controles,
        plantas=plantas,
        sectores=sectores,
        current_year=current_year,
        usuarios_activos=usuarios_activos,
        es_mi_cronograma=True
    )

# ==========================================
# MÓDULO DE ACCIONES CORRECTIVAS (CAPA)
# ==========================================

@app.route("/auditoria/hallazgo/<int:hallazgo_id>/nueva_accion", methods=["GET"])
@login_required
def nueva_accion_correctiva(hallazgo_id):
    info = repo.get_hallazgo_capa_context(app, hallazgo_id)

    if not info:
        flash("El hallazgo no existe.", "danger")
        return redirect(url_for('historial'))

    if not can_create_audit_capa(info):
        flash("Solo el auditor jefe designado o un administrador pueden crear acciones correctivas para esta auditoría.", "danger")
        return redirect(url_for('detalle_control', control_id=info['control_id']))

    existing = fetch_one("SELECT id FROM acciones_correctivas WHERE hallazgo_id = %s", (hallazgo_id,))
    if existing:
        flash("La acción correctiva ya está en curso.", "warning")
        return redirect(url_for('accion_correctiva', ac_id=existing['id']))

    ac_id = execute_query("""
        INSERT INTO acciones_correctivas (
            hallazgo_id, control_id, estado_flujo, tipo_auditoria, fecha_auditoria, tipo_hallazgo, requisito_normativo,
            auditor_lider, auditor_acompanante, auditor_formacion, area_auditada,
            evidencia_descripcion, responsable_area, responsable_verificacion, capa_creada_at
        ) VALUES (%s, %s, 'PASO_1', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """, build_capa_insert_values(info))
    
    return redirect(url_for('accion_correctiva', ac_id=ac_id))

def notificar_cambio_capa(ac_id, nuevo_estado):
    try:
        ac = fetch_one("SELECT * FROM acciones_correctivas WHERE id = %s", (ac_id,))
        if not ac: return
        
        nombres = expand_notification_names(ac)
        if not nombres: return
        
        # Buscar por nombre completo parametrizado para no cruzar usuarios con el mismo nombre.
        nombres_unicos = list(dict.fromkeys(nombres))
        placeholders = ", ".join(["%s"] * len(nombres_unicos))
        users = fetch_all(
            f"""
            SELECT email
            FROM usuarios
            WHERE TRIM(CONCAT(COALESCE(nombre, ''), ' ', COALESCE(apellido, ''))) IN ({placeholders})
              AND email IS NOT NULL
              AND email != ''
            """,
            tuple(nombres_unicos)
        )
        
        link = url_for('accion_correctiva', ac_id=ac_id, _external=True)
        asunto = f"Actualización CAPA #{ac_id} - Estado: {nuevo_estado.replace('_', ' ')}"
        cuerpo = f"<h3>Cambio en Acción Correctiva</h3><p>El documento correctivo ha cambiado su estado a <b>{nuevo_estado.replace('_', ' ')}</b>.</p><p><a href='{link}'>Haga clic aquí para revisarlo.</a></p>"
        
        for u in users:
            enviar_correo(u['email'], asunto, cuerpo)
    except Exception as e:
        print(f"No se pudo enviar notificacion CAPA: {e}")

@app.route("/acciones_correctivas/<int:ac_id>", methods=["GET", "POST"])
@login_required
def accion_correctiva(ac_id):
    ac = fetch_one("SELECT * FROM acciones_correctivas WHERE id = %s", (ac_id,))
    if not ac:
        flash("Acción correctiva inexistente.", "danger")
        return redirect(url_for('historial'))

    control_id_row = fetch_one("SELECT control_id FROM hallazgos_auditoria WHERE id = %s", (ac['hallazgo_id'],))
    control_id = control_id_row['control_id'] if control_id_row else None
    full_name = f"{g.user['nombre']} {g.user['apellido']}".strip()
    participantes = set()
    for raw_value in [
        ac.get('auditor_lider'),
        ac.get('auditor_acompanante'),
        ac.get('responsable_area'),
        ac.get('responsable_verificacion'),
        ac.get('responsable_ejecucion')
    ]:
        participantes.update((name or "").strip().lower() for name in split_multi_names(raw_value))

    if not permission_utils.can_view_capa(g.user, ac):
        flash("No tiene permisos para acceder a esta acciÃ³n correctiva.", "danger")
        return redirect(url_for('mi_cronograma'))

    is_auditor = permission_utils.is_capa_auditor(g.user, ac)
    is_responsable = permission_utils.is_capa_responsible(g.user, ac)

    if request.method == "POST":
        paso_actual = request.form.get("paso_guardado")

        if not permission_utils.can_edit_capa_step(g.user, ac, paso_actual):
            flash("No tiene permisos para gestionar este paso.", "danger")
            return redirect(url_for('accion_correctiva', ac_id=ac_id))
        if paso_actual == "PASO_1":
            execute_query("""
                UPDATE acciones_correctivas SET 
                    responsable_area = %s, proceso_auditado = %s, responsable_proceso = %s,
                    fecha_cierre_programado = %s, responsable_verificacion = %s, responsable_ejecucion = %s,
                    estado_flujo = 'PASO_2'
                WHERE id = %s
            """, (*build_capa_step_one_values(ac, request.form), ac_id))
            flash("Plan inicial creado. Enviado al Responsable del Desvío para su análisis de causa.", "success")
            notificar_cambio_capa(ac_id, 'PASO_2')
            
        elif paso_actual == "PASO_2":
            tipos_accion = request.form.getlist('plan_tipo_accion[]')
            plan_tipo_accion = ", ".join([t.strip() for t in tipos_accion if t and t.strip()])
            accion_inmediata_requiere = 1 if request.form.get('accion_inmediata_requiere') == '1' else 0
            prorroga_requiere = 1 if request.form.get('prorroga_requiere') == '1' else 0
            execute_query("""
                UPDATE acciones_correctivas SET
                    accion_inmediata_consecuencias = %s, accion_inmediata_requiere = %s, accion_inmediata_desc = %s,
                    ishikawa_metodo = %s, ishikawa_mano_obra = %s, ishikawa_maquina = %s, ishikawa_material = %s, ishikawa_medicion = %s, ishikawa_medioambiente = %s,
                    porque_1 = %s, porque_2 = %s, porque_3 = %s, porque_4 = %s, porque_5 = %s, porque_6 = %s,
                    plan_tipo_accion = %s, plan_descripcion = %s, prorroga_requiere = %s, prorroga_fecha = %s, prorroga_motivo = %s,
                    estado_flujo = 'PASO_3'
                WHERE id = %s
            """, (
                request.form.get('accion_inmediata_consecuencias'), accion_inmediata_requiere, request.form.get('accion_inmediata_desc'),
                request.form.get('ishikawa_metodo'), request.form.get('ishikawa_mano_obra'), request.form.get('ishikawa_maquina'), request.form.get('ishikawa_material'), request.form.get('ishikawa_medicion'), request.form.get('ishikawa_medioambiente'),
                request.form.get('porque_1'), request.form.get('porque_2'), request.form.get('porque_3'), request.form.get('porque_4'), request.form.get('porque_5'), request.form.get('porque_6'),
                plan_tipo_accion, request.form.get('plan_descripcion'), prorroga_requiere,
                request.form.get('prorroga_fecha') or None, request.form.get('prorroga_motivo'),
                ac_id
            ))
            flash("Análisis de Causa y Plan de Acción guardados. Enviado a Administrador para probación.", "success")
            notificar_cambio_capa(ac_id, 'PASO_3')
            
        elif paso_actual == "PASO_3":
            aprueba = request.form.get("decision_aprobacion") == 'true'
            justificacion = request.form.get("justificacion_resolucion")
            prorroga_decision = request.form.get("prorroga_decision")
            has_prorroga_request = bool(ac.get('prorroga_fecha') or (ac.get('prorroga_motivo') or '').strip())
            if ac.get('prorroga_requiere') and has_prorroga_request:
                if prorroga_decision not in ['APROBADA', 'DENEGADA']:
                    flash("Debe indicar si la prorroga solicitada queda aprobada o denegada.", "danger")
                    return redirect(url_for('accion_correctiva', ac_id=ac_id))
                if prorroga_decision == 'DENEGADA':
                    execute_query("UPDATE acciones_correctivas SET aprueba_causas=0, aprueba_plan=0, prorroga_avances=%s, justificacion_resolucion=%s, estado_flujo='PASO_2' WHERE id=%s", ('DENEGADA', justificacion, ac_id))
                    flash("La prorroga solicitada fue denegada. La accion vuelve al responsable del sector.", "warning")
                    notificar_cambio_capa(ac_id, 'DEVUELTO_A_PASO_2')
                    return redirect(url_for('accion_correctiva', ac_id=ac_id))
            if aprueba:
                execute_query("UPDATE acciones_correctivas SET aprueba_causas=1, aprueba_plan=1, prorroga_avances=%s, estado_flujo='PASO_4' WHERE id=%s", (prorroga_decision or ac.get('prorroga_avances'), ac_id))
                flash("Plan Aprobado. El Responsable debe proceder con la ejecución.", "success")
                notificar_cambio_capa(ac_id, 'PASO_4')
            else:
                execute_query("UPDATE acciones_correctivas SET aprueba_causas=0, aprueba_plan=0, justificacion_resolucion=%s, estado_flujo='PASO_2' WHERE id=%s", (justificacion, ac_id))
                flash("Plan Denegado. La acción ha sido devuelta a Modificación.", "warning")
                notificar_cambio_capa(ac_id, 'DEVUELTO_A_PASO_2')

        elif paso_actual == "PASO_4":
            evidencia = request.files.get("evidencia_path")
            ev_filename = ac['evidencia_path']
            evidencia_fotos = request.files.getlist("evidencia_fotos[]")
            evidencia_fotos_actuales = parse_capa_evidence_photos(ac.get('evidencia_fotos_json'))
            if evidencia and evidencia.filename:
                import os, time
                from werkzeug.utils import secure_filename
                ev_filename = f"evidencia_{ac_id}_{int(time.time())}_{secure_filename(evidencia.filename)}"
                evidencia.save(os.path.join(app.config['UPLOAD_FOLDER'], ev_filename))
            for foto in evidencia_fotos:
                if not foto or not foto.filename:
                    continue
                foto_filename = f"evidencia_foto_{ac_id}_{int(time.time() * 1000)}_{secure_filename(foto.filename)}"
                foto.save(os.path.join(app.config['UPLOAD_FOLDER'], foto_filename))
                evidencia_fotos_actuales.append(foto_filename)
            execute_query(
                "UPDATE acciones_correctivas SET verificacion_implementacion = %s, evidencia_path = %s, evidencia_fotos_json = %s, estado_flujo = 'PASO_5' WHERE id = %s",
                (
                    request.form.get('verificacion_implementacion'),
                    ev_filename,
                    json.dumps(evidencia_fotos_actuales, ensure_ascii=False) if evidencia_fotos_actuales else None,
                    ac_id
                )
            )
            flash("La ejecuci?n ha sido registrada y enviada para verificaci?n de cierre.", "success")
            notificar_cambio_capa(ac_id, 'PASO_5_VERIFICACI?N')
            
        elif paso_actual == "PASO_5":
            cierra = request.form.get("decision_cierre") == 'true'
            if cierra:
                execute_query("UPDATE acciones_correctivas SET estado_flujo = 'CERRADO', capa_closed_at = NOW() WHERE id = %s", (ac_id,))
                flash("¡Acción Correctiva Cerrada exitosamente!", "success")
                notificar_cambio_capa(ac_id, 'CERRADO')
            else:
                execute_query("UPDATE acciones_correctivas SET estado_flujo = 'PASO_4', justificacion_resolucion = %s, capa_closed_at = NULL WHERE id = %s", (request.form.get('justificacion_resolucion'), ac_id))
                flash("Cierre denegado. La acción retorna a estado de ejecución.", "warning")
                notificar_cambio_capa(ac_id, 'DEVUELTO_A_PASO_4_EJECUCION')

        return redirect(url_for('accion_correctiva', ac_id=ac_id))

    return render_template(
        "acciones_correctivas.html",
        ac=ac,
        control_id=control_id,
        full_name=full_name,
        evidencia_fotos=parse_capa_evidence_photos(ac.get('evidencia_fotos_json')),
        is_capa_auditor=is_auditor,
        is_capa_responsable=is_responsable
    )

if __name__ == "__main__":
    run_migrations()
    # reset_control_data() # Uncomment one time to clear all data
    # if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    #     threading.Timer(1.5, open_browser).start()

    app.run(host="0.0.0.0", port=5001, debug=True)
