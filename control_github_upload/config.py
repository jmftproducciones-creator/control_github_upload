import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

if load_dotenv:
    load_dotenv(PROJECT_DIR / ".env")
    load_dotenv(BASE_DIR / ".env")

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'cambia-esta-clave')
    MYSQL_HOST = os.getenv('MYSQL_HOST', '127.0.0.1')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
    MYSQL_USER = os.getenv('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'root')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'control_documental')
    
    # CONFIGURACIÓN DE CORREO (SMTP)
    MAIL_SERVER = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
    MAIL_PORT = int(os.getenv('MAIL_PORT', '587'))
    MAIL_USERNAME = os.getenv('MAIL_USERNAME', '')
    MAIL_PASSWORD = os.getenv('MAIL_PASSWORD', '')
