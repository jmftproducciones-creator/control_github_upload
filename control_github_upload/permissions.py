def split_multi_names(raw_value):
    if not raw_value:
        return []
    text = str(raw_value).replace(";", ",").replace("\r", "\n")
    parts = []
    for chunk in text.split("\n"):
        for item in chunk.split(","):
            clean = item.strip()
            if clean:
                parts.append(clean)
    return parts


def user_name_matches(raw_value, full_name):
    normalized_full_name = (full_name or "").strip().lower()
    if not normalized_full_name:
        return False
    return normalized_full_name in [(name or "").strip().lower() for name in split_multi_names(raw_value)]


def current_user_full_name(user):
    if not user:
        return ""
    return f"{user.get('nombre', '')} {user.get('apellido', '')}".strip().lower()


def is_admin(user):
    return bool(user and user.get('rol') in ["admin", "superadmin"])


def is_superadmin(user):
    return bool(user and user.get('rol') == "superadmin")


def is_audit_lead(user, control):
    if not user or not control:
        return False
    if control.get('auditor_jefe_id') and control.get('auditor_jefe_id') == user.get('id'):
        return True
    return user_name_matches(control.get('auditor_jefe_nombre'), current_user_full_name(user))


def can_edit_audit_plan(user, control):
    return bool(control and control.get('tipo_control') == 'A' and is_audit_lead(user, control))


def can_edit_audit_report(user, control):
    return can_edit_audit_plan(user, control)


def can_create_capa(user, control):
    return bool(control and control.get('tipo_control') == 'A' and (is_audit_lead(user, control) or is_admin(user)))


def can_view_capa(user, ac):
    if not user or not ac:
        return False
    if is_admin(user):
        return True
    full_name = current_user_full_name(user)
    participantes = set()
    for raw_value in [
        ac.get('auditor_lider'),
        ac.get('auditor_acompanante'),
        ac.get('responsable_area'),
        ac.get('responsable_verificacion'),
        ac.get('responsable_ejecucion')
    ]:
        participantes.update((name or "").strip().lower() for name in split_multi_names(raw_value))
    return full_name in participantes


def is_capa_auditor(user, ac):
    return bool(user and ac and user_name_matches(ac.get('auditor_lider'), current_user_full_name(user)))


def is_capa_responsible(user, ac):
    return bool(user and ac and user_name_matches(ac.get('responsable_area'), current_user_full_name(user)))


def can_edit_capa_step(user, ac, step_code):
    if step_code in ['PASO_1', 'PASO_3', 'PASO_5']:
        return is_capa_auditor(user, ac)
    if step_code in ['PASO_2', 'PASO_4']:
        return is_capa_responsible(user, ac)
    return False
