import requests
import csv

SHEET_ID = "1k-ydMq9JEhWGSt7P3D0ucYj2bWNMkhA9uk1kBJiOMb8"
SHEET_NAME = "Links"

def normalizar_coluna(col):
    return col.strip().lower().replace("ó", "o").replace("ã", "a").replace("ê", "e").replace("é", "e")

def buscar_url_por_codigo(codigo):
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        content = response.content.decode("utf-8").splitlines()
        reader = csv.DictReader(content)

        field_map = {normalizar_coluna(k): k for k in reader.fieldnames}

        col_codigo = field_map.get("codigo")
        col_url = field_map.get("url")
        col_expira = field_map.get("expira_em")

        if not col_codigo or not col_url or not col_expira:
            print("[ERRO] Cabeçalhos ausentes ou mal formatados:", reader.fieldnames)
            return None

        for row in reader:
            print("[DEBUG] Linha:", row)
            if row[col_codigo].strip().lower() == codigo.lower():
                return {
                    "url": row[col_url].strip(),
                    "expira_em": row[col_expira].strip()
                }

        print(f"[INFO] Código '{codigo}' não encontrado.")
    except Exception as e:
        print(f"[ERRO] Falha ao acessar planilha: {e}")
    return None

def adicionar_link(codigo, url, expira_em):
    try:
        api_url = "https://script.google.com/macros/s/AKfycby9h_L99DY8Dp0xW3N8ci6KZizyeRcmQB53f3phbh0XqcqkZkaHc7UDmfQpD3Amj5l5CQ/exec"
        payload = {
            "codigo": codigo,
            "url": url,
            "expira_em": expira_em
        }
        response = requests.post(api_url, data=payload)
        print("[DEBUG] Enviado para Apps Script:", response.text)
        return response.status_code == 200
    except Exception as e:
        print(f"[ERRO] ao adicionar link: {e}")
        return False
