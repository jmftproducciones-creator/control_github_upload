# Control Documental

App Flask/MySQL preparada para subir a GitHub y montar como segundo proyecto en VPS.

## Local

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python init_db.py
python app.py
```

## VPS

Ver `VPS_DEPLOY_CONTROL.md`.

## No subir

No subir `.env`, `.venv`, logs, backups ni archivos dentro de `static/uploads/documentos`.
