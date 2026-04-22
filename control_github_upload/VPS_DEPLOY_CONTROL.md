# Despliegue en el mismo VPS

Ejemplo para montar esta app como segundo proyecto usando puerto publico `8081`.

## 1. Clonar

```bash
mkdir -p /var/www/control
cd /var/www/control
git clone URL_DEL_REPO .
```

Si el repo queda con una carpeta interna, entra a esa carpeta antes de seguir.

## 2. Entorno Python

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Configurar .env

```bash
cp .env.example .env
nano .env
```

Ajustar `MYSQL_PASSWORD` y `SECRET_KEY`.

## 4. MySQL

```bash
mysql -u root
```

```sql
CREATE DATABASE IF NOT EXISTS control_documental CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'control_user'@'localhost' IDENTIFIED BY 'CAMBIAR_CLAVE';
GRANT ALL PRIVILEGES ON control_documental.* TO 'control_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

La clave debe coincidir con `MYSQL_PASSWORD` en `.env`.

Inicializar tablas:

```bash
python init_db.py
```

Usuario inicial:

```text
usuario: admin
password: admin123
```

Cambiar esa clave apenas entres.

## 5. Probar Gunicorn

```bash
gunicorn -c deploy/gunicorn.conf.py wsgi:application
```

Probar en otra consola:

```bash
curl http://127.0.0.1:5001/
```

## 6. Servicio systemd

```bash
cp deploy/control.service /etc/systemd/system/control.service
systemctl daemon-reload
systemctl enable --now control
systemctl status control --no-pager
```

## 7. Nginx por puerto 8081

```bash
cp deploy/nginx-control.conf /etc/nginx/sites-available/control
ln -s /etc/nginx/sites-available/control /etc/nginx/sites-enabled/control
nginx -t
systemctl reload nginx
```

Abrir:

```text
http://IP_DEL_VPS:8081
```

Usa el mismo login basico de Nginx creado para el primer sistema.
