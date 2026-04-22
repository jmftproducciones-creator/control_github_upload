import json

from db import fetch_all, fetch_one


def get_control_detail(app, control_id):
    return fetch_one(app, """
        SELECT
            c.id, c.fecha_control, c.fecha_fin_control, c.observaciones_generales, c.sector_tiene_quimicos,
            c.total_personas_entrevistadas, c.total_requieren_capacitacion,
            c.total_documentos_controlados, c.total_documentos_correctos, c.total_documentos_incorrectos,
            c.tipo_control, c.riesgos_pdf_path, c.estado_flujo, c.controlador_id, c.auditor_jefe_id, c.auditor_acompanante_id,
            c.auditor_jefe_nombre, c.auditor_acompanante_nombre, c.auditor_formacion_nombre,
            c.sistema_gestion_auditoria, c.objetivo_auditoria, c.criterios_auditoria, c.recursos_auditoria, c.descripcion_actividades_auditoria, c.agenda_auditoria,
            c.conclusiones_auditoria, c.fortalezas_auditoria,
            p.nombre AS planta, s.nombre AS sector,
            CONCAT(COALESCE(u.nombre, ''), ' ', COALESCE(u.apellido, '')) AS responsable,
            COALESCE(NULLIF(CONCAT(COALESCE(aj.nombre, ''), ' ', COALESCE(aj.apellido, '')), ' '), c.auditor_jefe_nombre) AS auditor_jefe,
            COALESCE(NULLIF(CONCAT(COALESCE(aa.nombre, ''), ' ', COALESCE(aa.apellido, '')), ' '), c.auditor_acompanante_nombre) AS auditor_acompanante,
            COALESCE(NULLIF(CONCAT(COALESCE(af.nombre, ''), ' ', COALESCE(af.apellido, '')), ' '), c.auditor_formacion_nombre) AS auditor_formacion
        FROM controles c
        INNER JOIN plantas p ON p.id = c.planta_id
        INNER JOIN sectores s ON s.id = c.sector_id
        LEFT JOIN usuarios u ON u.id = c.responsable_id
        LEFT JOIN usuarios aj ON aj.id = c.auditor_jefe_id
        LEFT JOIN usuarios aa ON aa.id = c.auditor_acompanante_id
        LEFT JOIN usuarios af ON af.id = c.auditor_formacion_id
        WHERE c.id = %s
    """, (control_id,))


def get_control_detail_public(app, control_id):
    return fetch_one(app, """
        SELECT
            c.id, c.fecha_control, c.fecha_fin_control, c.observaciones_generales, c.sector_tiene_quimicos,
            c.total_personas_entrevistadas, c.total_requieren_capacitacion,
            c.total_documentos_controlados, c.total_documentos_correctos, c.total_documentos_incorrectos,
            c.tipo_control, c.riesgos_pdf_path, c.estado_flujo,
            c.auditor_jefe_nombre, c.auditor_acompanante_nombre, c.auditor_formacion_nombre,
            c.sistema_gestion_auditoria, c.objetivo_auditoria, c.criterios_auditoria, c.recursos_auditoria, c.descripcion_actividades_auditoria, c.agenda_auditoria,
            p.nombre AS planta, s.nombre AS sector,
            CONCAT(COALESCE(u.nombre, ''), ' ', COALESCE(u.apellido, '')) AS responsable,
            COALESCE(NULLIF(CONCAT(COALESCE(aj.nombre, ''), ' ', COALESCE(aj.apellido, '')), ' '), c.auditor_jefe_nombre) AS auditor_jefe,
            COALESCE(NULLIF(CONCAT(COALESCE(aa.nombre, ''), ' ', COALESCE(aa.apellido, '')), ' '), c.auditor_acompanante_nombre) AS auditor_acompanante,
            COALESCE(NULLIF(CONCAT(COALESCE(af.nombre, ''), ' ', COALESCE(af.apellido, '')), ' '), c.auditor_formacion_nombre) AS auditor_formacion
        FROM controles c
        INNER JOIN plantas p ON p.id = c.planta_id
        INNER JOIN sectores s ON s.id = c.sector_id
        LEFT JOIN usuarios u ON u.id = c.responsable_id
        LEFT JOIN usuarios aj ON aj.id = c.auditor_jefe_id
        LEFT JOIN usuarios aa ON aa.id = c.auditor_acompanante_id
        LEFT JOIN usuarios af ON af.id = c.auditor_formacion_id
        WHERE c.id = %s
    """, (control_id,))


def get_control_personal(app, control_id):
    return fetch_all(app, """
        SELECT
            nombre_apellido,
            conoce_gestion_documental,
            realizo_capacitacion,
            requiere_capacitacion,
            observaciones
        FROM personal_control
        WHERE control_id = %s
        ORDER BY id ASC
    """, (control_id,))


def get_control_documentos(app, control_id):
    return fetch_all(app, """
        SELECT
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
        FROM documentos_control
        WHERE control_id = %s
        ORDER BY id ASC
    """, (control_id,))


def get_control_quimicos(app, control_id):
    return fetch_all(app, """
        SELECT
            nombre_producto,
            bajo_llave,
            envase_original,
            etiquetado_correcto,
            hoja_seguridad,
            observaciones,
            medida
        FROM productos_quimicos
        WHERE control_id = %s
        ORDER BY id ASC
    """, (control_id,))


def parse_audit_agenda(control):
    agenda_existente = []
    if control and control.get('agenda_auditoria'):
        try:
            agenda_existente = json.loads(control['agenda_auditoria'])
        except Exception:
            pass
    return agenda_existente


def get_hallazgos_with_capa(app, control_id):
    return fetch_all(app, """
        SELECT h.*, ac.id AS accion_correctiva_id, ac.estado_flujo AS ac_estado
        FROM hallazgos_auditoria h
        LEFT JOIN acciones_correctivas ac ON h.id = ac.hallazgo_id
        WHERE h.control_id = %s ORDER BY h.id ASC
    """, (control_id,))


def get_hallazgo_capa_context(app, hallazgo_id):
    return fetch_one(app, """
        SELECT h.id AS hallazgo_id, h.requisito, h.tipo_hallazgo, h.descripcion,
               c.tipo_control, c.fecha_control, c.auditor_jefe_id, c.auditor_jefe_nombre, c.auditor_acompanante_nombre, c.auditor_formacion_nombre,
               p.nombre AS planta, s.nombre AS sector,
               (SELECT CONCAT(u2.nombre, ' ', u2.apellido) FROM usuarios u2 WHERE u2.sector_id = c.sector_id AND u2.activo = 1 LIMIT 1) AS res_sector_vinc,
               CONCAT(u.nombre, ' ', u.apellido) AS res_auditado,
               c.id AS control_id
        FROM hallazgos_auditoria h
        JOIN controles c ON h.control_id = c.id
        JOIN plantas p ON c.planta_id = p.id
        JOIN sectores s ON c.sector_id = s.id
        LEFT JOIN usuarios u ON c.controlado_id = u.id
        WHERE h.id = %s
    """, (hallazgo_id,))
