from permissions import split_multi_names


def resolve_area_responsible_name(info):
    return (info.get('res_sector_vinc') or info.get('res_auditado') or '').strip()


def build_capa_inherited_fields(info):
    area_responsable = resolve_area_responsible_name(info)
    return {
        'tipo_auditoria': "Interna" if info.get('tipo_control') == 'A' else "Externa",
        'fecha_auditoria': info.get('fecha_control'),
        'tipo_hallazgo': info.get('tipo_hallazgo'),
        'requisito_normativo': info.get('requisito'),
        'auditor_lider': info.get('auditor_jefe_nombre') or '',
        'auditor_acompanante': info.get('auditor_acompanante_nombre') or '',
        'auditor_formacion': info.get('auditor_formacion_nombre') or '',
        'area_auditada': f"{info.get('planta', '')} - {info.get('sector', '')}".strip(" -"),
        'evidencia_descripcion': info.get('descripcion'),
        'responsable_area': area_responsable,
        'responsable_verificacion': info.get('auditor_jefe_nombre') or ''
    }


def build_capa_insert_values(info):
    inherited = build_capa_inherited_fields(info)
    return (
        info['hallazgo_id'],
        info['control_id'],
        inherited['tipo_auditoria'],
        inherited['fecha_auditoria'],
        inherited['tipo_hallazgo'],
        inherited['requisito_normativo'],
        inherited['auditor_lider'],
        inherited['auditor_acompanante'],
        inherited['auditor_formacion'],
        inherited['area_auditada'],
        inherited['evidencia_descripcion'],
        inherited['responsable_area'],
        inherited['responsable_verificacion']
    )


def build_capa_step_one_values(ac, form):
    return (
        ac.get('responsable_area'),
        form.get('proceso_auditado'),
        form.get('responsable_proceso'),
        form.get('fecha_cierre_programado') or None,
        ac.get('responsable_verificacion'),
        form.get('responsable_ejecucion')
    )


def expand_notification_names(ac):
    names = []
    for raw_value in [ac.get('responsable_area'), ac.get('auditor_lider'), ac.get('auditor_acompanante')]:
        names.extend(split_multi_names(raw_value))
    return list(dict.fromkeys([name for name in names if name]))
