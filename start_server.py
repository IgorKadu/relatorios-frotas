#!/usr/bin/env python3
"""
Script para iniciar o servidor FastAPI
"""

import os
import sys
import subprocess

# Adiciona o diretório do projeto ao Python path
project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_dir)

# Muda para o diretório do projeto
os.chdir(project_dir)

if __name__ == "__main__":
    # Inicia o servidor uvicorn
    subprocess.run([
        sys.executable, "-m", "uvicorn", 
        "app.main:app", 
        "--host", "0.0.0.0", 
        "--port", "5000", 
        "--reload"
    ])