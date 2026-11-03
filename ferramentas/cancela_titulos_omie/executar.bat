@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
title Cancelamento de titulos no Omie

rem Acha o Python instalado (py ou python).
set "PY="
where py >nul 2>nul && set "PY=py -3"
if not defined PY where python >nul 2>nul && set "PY=python"
if not defined PY (
  echo.
  echo Python nao encontrado. Instale em https://www.python.org/downloads/
  echo e marque a opcao "Add python.exe to PATH" na instalacao.
  pause
  exit /b 1
)

if not exist ".env" (
  copy "modelo_env.txt" ".env" >nul
  echo.
  echo Criei o arquivo .env nesta pasta. Abra no Bloco de Notas, preencha
  echo OMIE_KEY e OMIE_SECRET, salve e rode o executar.bat de novo.
  notepad ".env"
  pause
  exit /b 1
)

rem Na primeira vez, cria o ambiente e instala o que o programa usa.
if not exist ".venv\Scripts\python.exe" (
  echo Preparando o ambiente pela primeira vez, aguarde...
  %PY% -m venv .venv || (echo Falha ao criar o ambiente. & pause & exit /b 1)
  ".venv\Scripts\python.exe" -m pip install --quiet --upgrade pip
  ".venv\Scripts\python.exe" -m pip install --quiet -r requirements.txt || (echo Falha ao instalar. Confira a internet. & pause & exit /b 1)
)

".venv\Scripts\python.exe" cancela_omie.py %1
echo.
pause
