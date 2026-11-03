@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
title Exclusao no Omie de titulos de cards cancelados
set PYTHONDONTWRITEBYTECODE=1

rem O ambiente do Python fica FORA desta pasta, no proprio PC.
rem Motivo: se esta pasta estiver no Dropbox/OneDrive, a sincronizacao
rem trava os arquivos durante a instalacao e ela quebra no meio.
set "VENV=%LOCALAPPDATA%\BWS\cancela_titulos_omie\venv"
set "OK=%VENV%\instalacao_concluida.txt"

rem Restos de versao anterior, que criava o ambiente dentro desta pasta.
if exist ".venv" rmdir /s /q ".venv" 2>nul

rem Acha o Python instalado: py ou python.
set "PY="
where py >nul 2>nul && set "PY=py -3"
if not defined PY where python >nul 2>nul && set "PY=python"
if not defined PY goto :sem_python

if not exist ".env" goto :sem_env

rem Instalacao: so na primeira vez, ou se a anterior nao terminou.
if exist "%OK%" if exist "%VENV%\Scripts\python.exe" goto :rodar
echo Preparando o ambiente, aguarde. So acontece na primeira vez...
if exist "%VENV%" rmdir /s /q "%VENV%"
%PY% -m venv "%VENV%"
if errorlevel 1 goto :falha_venv
"%VENV%\Scripts\python.exe" -m pip install --disable-pip-version-check --quiet -r requirements.txt
if errorlevel 1 goto :falha_pip
echo ok> "%OK%"

:rodar
"%VENV%\Scripts\python.exe" cancela_omie.py %1
echo.
pause
exit /b 0

:sem_python
echo.
echo Python nao encontrado. Instale em https://www.python.org/downloads/
echo e marque a opcao "Add python.exe to PATH" na instalacao.
pause
exit /b 1

:sem_env
copy "modelo_env.txt" ".env" >nul
echo.
echo Criei o arquivo .env nesta pasta. Preencha OMIE_KEY e OMIE_SECRET,
echo salve e rode o executar.bat de novo.
notepad ".env"
pause
exit /b 1

:falha_venv
echo.
echo Nao consegui criar o ambiente do Python em %VENV%
pause
exit /b 1

:falha_pip
echo.
echo A instalacao das bibliotecas falhou - veja a mensagem acima.
echo Confira a internet e rode o executar.bat de novo: ele recomeca do zero.
if exist "%VENV%" rmdir /s /q "%VENV%"
pause
exit /b 1
