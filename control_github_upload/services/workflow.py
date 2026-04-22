import datetime


def describe_capa_visual_state(item, today=None):
    today = today or datetime.date.today()
    estado = item.get("estado_flujo")
    due_date = item.get("fecha_cierre_programado")

    if estado == "CERRADO":
        return "Cerrado"
    if due_date and isinstance(due_date, datetime.date) and due_date < today:
        return "Vencido"
    if item.get("prorroga_requiere") and estado == "PASO_3":
        return "Bloqueado por prórroga"
    if estado in ("PASO_2", "PASO_4"):
        return "En revisión sector"
    if estado in ("PASO_1", "PASO_3", "PASO_5"):
        return "En revisión auditor"
    return "Pendiente"
