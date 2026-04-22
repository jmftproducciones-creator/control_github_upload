import datetime
import os


def _add_column_if_missing(cursor, table_name, column_name, definition):
    cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
    if not cursor.fetchone():
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _ensure_schema_migrations(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version VARCHAR(100) PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _migration_applied(cursor, version):
    cursor.execute("SELECT version FROM schema_migrations WHERE version = %s", (version,))
    return bool(cursor.fetchone())


def _record_migration(cursor, version):
    cursor.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))


def _workflow_timestamps_migration(conn, cursor):
    _add_column_if_missing(cursor, "controles", "plan_completado_at", "DATETIME NULL")
    _add_column_if_missing(cursor, "controles", "informe_emitido_at", "DATETIME NULL")
    _add_column_if_missing(cursor, "acciones_correctivas", "capa_creada_at", "DATETIME NULL")
    _add_column_if_missing(cursor, "acciones_correctivas", "capa_closed_at", "DATETIME NULL")
    conn.commit()


def _capa_evidence_gallery_migration(conn, cursor):
    _add_column_if_missing(cursor, "acciones_correctivas", "evidencia_fotos_json", "LONGTEXT NULL")
    conn.commit()


def run_versioned_migrations(get_connection, bootstrap_callback):
    log_path = os.path.join(os.getcwd(), "migration_debug.log")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n--- Migración Versionada: {datetime.datetime.now()} ---\n")

    conn = get_connection()
    cursor = conn.cursor()
    try:
        _ensure_schema_migrations(cursor)
        conn.commit()

        migrations = [
            ("001_legacy_bootstrap", lambda: bootstrap_callback()),
            ("002_workflow_timestamps", lambda: _workflow_timestamps_migration(conn, cursor)),
            ("003_capa_evidence_gallery", lambda: _capa_evidence_gallery_migration(conn, cursor)),
        ]

        for version, callback in migrations:
            if _migration_applied(cursor, version):
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(f"[MIGRATE] {version} ya aplicada\n")
                continue

            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"[MIGRATE] Ejecutando {version}\n")
            callback()
            cursor = conn.cursor()
            _ensure_schema_migrations(cursor)
            _record_migration(cursor, version)
            conn.commit()
    finally:
        cursor.close()
        conn.close()
