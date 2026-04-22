import os
from pathlib import Path

import mysql.connector
from mysql.connector import Error
from werkzeug.security import generate_password_hash


BASE_DIR = Path(__file__).resolve().parent


def load_env_file(path: Path = BASE_DIR / ".env") -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "control_documental")
ADMIN_USER = os.getenv("INITIAL_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("INITIAL_ADMIN_PASSWORD", "admin123")
ADMIN_EMAIL = os.getenv("INITIAL_ADMIN_EMAIL", "admin@example.local")


TABLES = [
    """
    CREATE TABLE IF NOT EXISTS plantas (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nombre VARCHAR(255) NOT NULL,
        activa TINYINT(1) NOT NULL DEFAULT 1,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS sectores (
        id INT AUTO_INCREMENT PRIMARY KEY,
        planta_id INT NOT NULL,
        nombre VARCHAR(255) NOT NULL,
        activo TINYINT(1) NOT NULL DEFAULT 1,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_sectores_planta (planta_id),
        CONSTRAINT fk_sectores_planta FOREIGN KEY (planta_id) REFERENCES plantas(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS usuarios (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nombre VARCHAR(120) NOT NULL,
        apellido VARCHAR(120) NOT NULL DEFAULT '',
        usuario VARCHAR(120) NOT NULL,
        email VARCHAR(255) NULL,
        password_hash VARCHAR(255) NOT NULL,
        rol_control VARCHAR(50) NOT NULL DEFAULT 'visor',
        activo TINYINT(1) NOT NULL DEFAULT 1,
        planta_id INT NULL,
        sector_id INT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_usuarios_usuario (usuario),
        KEY idx_usuarios_email (email),
        CONSTRAINT fk_usuarios_planta_base FOREIGN KEY (planta_id) REFERENCES plantas(id) ON DELETE SET NULL,
        CONSTRAINT fk_usuarios_sector_base FOREIGN KEY (sector_id) REFERENCES sectores(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS controles (
        id INT AUTO_INCREMENT PRIMARY KEY,
        cronograma_id INT NULL,
        planta_id INT NOT NULL,
        sector_id INT NOT NULL,
        fecha_control DATE NOT NULL,
        fecha_fin_control DATE NULL,
        responsable_id INT NULL,
        controlador_id INT NULL,
        controlado_id INT NULL,
        observaciones_generales TEXT NULL,
        sector_tiene_quimicos TINYINT(1) NOT NULL DEFAULT 0,
        total_personas_entrevistadas INT NOT NULL DEFAULT 0,
        total_requieren_capacitacion INT NOT NULL DEFAULT 0,
        total_documentos_controlados INT NOT NULL DEFAULT 0,
        total_documentos_correctos INT NOT NULL DEFAULT 0,
        total_documentos_incorrectos INT NOT NULL DEFAULT 0,
        tipo_control VARCHAR(10) NOT NULL DEFAULT 'P',
        riesgos_pdf_path VARCHAR(255) NULL,
        estado_flujo VARCHAR(50) NOT NULL DEFAULT 'Completado',
        auditor_jefe_id INT NULL,
        auditor_acompanante_id INT NULL,
        auditor_formacion_id INT NULL,
        auditor_jefe_nombre VARCHAR(255) NULL,
        auditor_acompanante_nombre VARCHAR(255) NULL,
        auditor_formacion_nombre VARCHAR(255) NULL,
        sistema_gestion_auditoria VARCHAR(255) NULL,
        objetivo_auditoria TEXT NULL,
        criterios_auditoria TEXT NULL,
        descripcion_actividades_auditoria TEXT NULL,
        recursos_auditoria TEXT NULL,
        agenda_auditoria_path VARCHAR(255) NULL,
        agenda_auditoria TEXT NULL,
        fortalezas_auditoria TEXT NULL,
        conclusiones_auditoria TEXT NULL,
        plan_completado_at DATETIME NULL,
        informe_emitido_at DATETIME NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_controles_fecha (fecha_control),
        KEY idx_controles_tipo (tipo_control),
        CONSTRAINT fk_controles_planta_base FOREIGN KEY (planta_id) REFERENCES plantas(id),
        CONSTRAINT fk_controles_sector_base FOREIGN KEY (sector_id) REFERENCES sectores(id),
        CONSTRAINT fk_controles_responsable_base FOREIGN KEY (responsable_id) REFERENCES usuarios(id) ON DELETE SET NULL,
        CONSTRAINT fk_controles_controlador_base FOREIGN KEY (controlador_id) REFERENCES usuarios(id) ON DELETE SET NULL,
        CONSTRAINT fk_controles_controlado_base FOREIGN KEY (controlado_id) REFERENCES usuarios(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS cronograma_semanal (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sector_id INT NULL,
        fecha_inicio DATE NOT NULL,
        fecha_fin DATE NOT NULL,
        hora_inicio TIME NULL,
        hora_fin TIME NULL,
        titulo VARCHAR(255) NULL,
        tipo_control VARCHAR(10) NOT NULL DEFAULT 'P',
        controlador_id INT NULL,
        controlado_id INT NULL,
        control_id INT NULL,
        parent_id INT NULL,
        recurrencia VARCHAR(50) NULL,
        recurrencia_fin DATE NULL,
        plan_auditoria TEXT NULL,
        auditor_jefe_id INT NULL,
        auditor_acompanante_id INT NULL,
        auditor_formacion_id INT NULL,
        auditor_jefe_nombre VARCHAR(255) NULL,
        auditor_acompanante_nombre VARCHAR(255) NULL,
        auditor_formacion_nombre VARCHAR(255) NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_cronograma_fechas (fecha_inicio, fecha_fin),
        CONSTRAINT fk_cronograma_sector_base FOREIGN KEY (sector_id) REFERENCES sectores(id) ON DELETE SET NULL,
        CONSTRAINT fk_cronograma_controlador_base FOREIGN KEY (controlador_id) REFERENCES usuarios(id) ON DELETE SET NULL,
        CONSTRAINT fk_cronograma_controlado_base FOREIGN KEY (controlado_id) REFERENCES usuarios(id) ON DELETE SET NULL,
        CONSTRAINT fk_cronograma_control_base FOREIGN KEY (control_id) REFERENCES controles(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS personal_control (
        id INT AUTO_INCREMENT PRIMARY KEY,
        control_id INT NOT NULL,
        nombre_apellido VARCHAR(255) NOT NULL,
        conoce_gestion_documental TINYINT(1) NOT NULL DEFAULT 0,
        realizo_capacitacion TINYINT(1) NOT NULL DEFAULT 0,
        requiere_capacitacion TINYINT(1) NOT NULL DEFAULT 0,
        observaciones TEXT NULL,
        CONSTRAINT fk_personal_control_base FOREIGN KEY (control_id) REFERENCES controles(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS documentos_control (
        id INT AUTO_INCREMENT PRIMARY KEY,
        control_id INT NOT NULL,
        nombre_documento VARCHAR(255) NULL,
        codigo_documento VARCHAR(255) NULL,
        revision VARCHAR(100) NULL,
        copia_controlada TINYINT(1) NOT NULL DEFAULT 0,
        copia_controlada_numero VARCHAR(100) NULL,
        no_cargado_portal TINYINT(1) NOT NULL DEFAULT 0,
        motivo_no_cargado TEXT NULL,
        estado VARCHAR(80) NULL,
        observaciones TEXT NULL,
        imagen_path VARCHAR(255) NULL,
        CONSTRAINT fk_documentos_control_base FOREIGN KEY (control_id) REFERENCES controles(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS productos_quimicos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        control_id INT NOT NULL,
        nombre_producto VARCHAR(255) NOT NULL,
        bajo_llave TINYINT(1) NOT NULL DEFAULT 0,
        envase_original TINYINT(1) NOT NULL DEFAULT 0,
        etiquetado_correcto TINYINT(1) NOT NULL DEFAULT 0,
        hoja_seguridad TINYINT(1) NOT NULL DEFAULT 0,
        observaciones TEXT NULL,
        medida VARCHAR(255) NULL,
        CONSTRAINT fk_productos_quimicos_base FOREIGN KEY (control_id) REFERENCES controles(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS hallazgos_auditoria (
        id INT AUTO_INCREMENT PRIMARY KEY,
        control_id INT NOT NULL,
        requisito VARCHAR(255) NULL,
        tipo_hallazgo VARCHAR(100) NULL,
        descripcion TEXT NULL,
        CONSTRAINT fk_hallazgos_control_base FOREIGN KEY (control_id) REFERENCES controles(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS acciones_correctivas (
        id INT AUTO_INCREMENT PRIMARY KEY,
        hallazgo_id INT NOT NULL,
        control_id INT NULL,
        estado_flujo VARCHAR(50) DEFAULT 'PASO_1',
        tipo_auditoria VARCHAR(100) NULL,
        fecha_auditoria DATE NULL,
        tipo_hallazgo VARCHAR(100) NULL,
        requisito_normativo VARCHAR(255) NULL,
        auditor_lider VARCHAR(255) NULL,
        auditor_acompanante VARCHAR(255) NULL,
        auditor_formacion VARCHAR(255) NULL,
        area_auditada VARCHAR(255) NULL,
        responsable_area VARCHAR(255) NULL,
        proceso_auditado VARCHAR(255) NULL,
        responsable_proceso VARCHAR(255) NULL,
        fecha_cierre_programado DATE NULL,
        responsable_verificacion VARCHAR(255) NULL,
        responsable_ejecucion VARCHAR(255) NULL,
        evidencia_descripcion TEXT NULL,
        accion_inmediata_consecuencias TEXT NULL,
        accion_inmediata_requiere TINYINT(1) DEFAULT 0,
        accion_inmediata_desc TEXT NULL,
        ishikawa_metodo TEXT NULL,
        ishikawa_mano_obra TEXT NULL,
        ishikawa_maquina TEXT NULL,
        ishikawa_material TEXT NULL,
        ishikawa_medicion TEXT NULL,
        ishikawa_medioambiente TEXT NULL,
        porque_1 TEXT NULL,
        porque_2 TEXT NULL,
        porque_3 TEXT NULL,
        porque_4 TEXT NULL,
        porque_5 TEXT NULL,
        porque_6 TEXT NULL,
        plan_tipo_accion VARCHAR(100) NULL,
        plan_descripcion TEXT NULL,
        prorroga_requiere TINYINT(1) DEFAULT 0,
        prorroga_fecha DATE NULL,
        prorroga_motivo TEXT NULL,
        prorroga_avances TEXT NULL,
        aprueba_causas TINYINT(1) DEFAULT 0,
        aprueba_plan TINYINT(1) DEFAULT 0,
        justificacion_resolucion TEXT NULL,
        verificacion_implementacion TEXT NULL,
        evidencia_path VARCHAR(255) NULL,
        evidencia_fotos_json LONGTEXT NULL,
        capa_creada_at DATETIME NULL,
        capa_closed_at DATETIME NULL,
        CONSTRAINT fk_acciones_hallazgo_base FOREIGN KEY (hallazgo_id) REFERENCES hallazgos_auditoria(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        usuario_id INT NULL,
        accion VARCHAR(255) NULL,
        detalles TEXT NULL,
        fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_audit_usuario_base FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS documentos_no_controlados (
        id INT AUTO_INCREMENT PRIMARY KEY
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS evidencias (
        id INT AUTO_INCREMENT PRIMARY KEY
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS cronogramas (
        id INT AUTO_INCREMENT PRIMARY KEY
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
]


def connect_server():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )


def connect_db():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def ensure_database() -> None:
    try:
        conn = connect_server()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cur.close()
        conn.close()
    except Error as exc:
        print(
            "No pude crear/verificar la base a nivel servidor. "
            "Si la base ya existe y el usuario tiene permisos sobre ella, sigo igual. "
            f"Detalle: {exc}"
        )


def seed_data(cur) -> None:
    cur.execute("INSERT IGNORE INTO plantas (id, nombre, activa) VALUES (1, 'Planta principal', 1)")
    cur.execute("INSERT IGNORE INTO sectores (id, planta_id, nombre, activo) VALUES (1, 1, 'General', 1)")
    cur.execute("SELECT id FROM usuarios WHERE usuario = %s LIMIT 1", (ADMIN_USER,))
    if not cur.fetchone():
        cur.execute(
            """
            INSERT INTO usuarios
                (nombre, apellido, usuario, email, password_hash, rol_control, activo, planta_id, sector_id)
            VALUES (%s, %s, %s, %s, %s, 'superadmin', 1, 1, 1)
            """,
            ("Admin", "", ADMIN_USER, ADMIN_EMAIL, generate_password_hash(ADMIN_PASSWORD)),
        )


def main() -> None:
    ensure_database()
    conn = connect_db()
    cur = conn.cursor()
    for statement in TABLES:
        cur.execute(statement)
    seed_data(cur)
    conn.commit()
    cur.close()
    conn.close()
    upload_dir = BASE_DIR / "static" / "uploads" / "documentos"
    upload_dir.mkdir(parents=True, exist_ok=True)
    print(f"Base de datos lista: {MYSQL_DATABASE}")


if __name__ == "__main__":
    main()
