import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURAÇÕES ---
STREAMLINE_KEY = os.getenv("STREAMLINE_KEY")
STREAMLINE_SECRET = os.getenv("STREAMLINE_SECRET")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

URL_STREAMLINE = "https://web.streamlinevrs.com/api/json"
URL_NOTION = "https://api.notion.com/v1"

HEADERS_NOTION = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# Cache para o DNA das casas (Bolivar/San Antonio/etc)
MAPA_DNA_CASAS = {}

# --- FUNÇÕES DE APOIO ---

def parse_dt_robusto(data_str):
    if not data_str: return None
    try:
        data_str = str(data_str).strip()
        if data_str.startswith("0000-00-00"): return None
        formatos = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]
        for fmt in formatos:
            try: return datetime.strptime(data_str, fmt)
            except ValueError: continue
    except: return None
    return None

def formatar_iso_date(dt_obj):
    return dt_obj.strftime("%Y-%m-%d") if dt_obj else None

def obter_estado_binario(code):
    return "CANCELLED" if str(code) == '8' else "CONFIRMED"

def gerar_status_visual(tipo, code):
    code_str = str(code)
    suffix = "UNK"
    if code_str == '8': suffix = "CXL"
    elif code_str in ['2', '4']: suffix = "BKD"
    elif code_str == '5': suffix = "OUT"
    tipo_limpo = str(tipo).split(' ')[0][:10]
    return f"{tipo_limpo}-{suffix}"

# --- BUSCA O GRUPO ATUAL DA CASA ---

def buscar_dna_da_casa(unit_id):
    if not unit_id: return "Geral"
    if str(unit_id) in MAPA_DNA_CASAS:
        return MAPA_DNA_CASAS[str(unit_id)]

    payload = {
        "methodName": "GetPropertyInfo",
        "params": {
            "token_key": STREAMLINE_KEY,
            "token_secret": STREAMLINE_SECRET,
            "unit_id": unit_id
        }
    }
    try:
        r = requests.post(URL_STREAMLINE, json=payload, timeout=30)
        dados = r.json()
        res_data = dados.get('data', {}) or dados.get('Response', {}).get('data', {})
        
        grupo_atual = (
            res_data.get('location_resort_name') or 
            res_data.get('condo_type_group_name') or 
            "Geral"
        )
        
        u_name = str(res_data.get('unit_name', '')).lower()
        if "bolivar" in u_name or "bolivar" in str(grupo_atual).lower():
            grupo_atual = "Bolivar Vacations"
        elif "san antonio" in u_name or "san antonio" in str(grupo_atual).lower():
            grupo_atual = "San Antonio"

        MAPA_DNA_CASAS[str(unit_id)] = grupo_atual
        return grupo_atual
    except:
        return "Geral"

def buscar_pagina_notion(res_number):
    url = f"{URL_NOTION}/databases/{NOTION_DATABASE_ID}/query"
    payload = {"filter": {"property": "Res #", "rich_text": {"equals": str(res_number)}}}
    try:
        response = requests.post(url, json=payload, headers=HEADERS_NOTION)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results: return results[0]["id"]
    except: pass
    return None

# --- UPSERT ---

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    dt_ci = parse_dt_robusto(reserva.get('startdate') or reserva.get('start_date'))
    
    # --- NOVO FILTRO: 2024 ATÉ 2026 ---
    if not dt_ci or dt_ci.year not in [2024, 2025, 2026]:
        return

    unit_id = reserva.get('unit_id') or reserva.get('home_id')
    nome_grupo = buscar_dna_da_casa(unit_id)

    dt_criacao = parse_dt_robusto(reserva.get('creation_date'))
    dt_co = parse_dt_robusto(reserva.get('enddate') or reserva.get('end_date'))
    nome = f"{reserva.get('first_name', '')} {reserva.get('last_name', '')}".strip()
    status_visual = gerar_status_visual(reserva.get('type_name', '---'), reserva.get('status_code'))
    state_binario = obter_estado_binario(reserva.get('status_code'))
    room = str(reserva.get('unit_name', 'Unknown'))
    gst = f"{reserva.get('occupants',0)}|{reserva.get('occupants_small',0)}"
    
    try: total = float(reserva.get('price_total', 0))
    except: total = 0.0
    try: rate = float(reserva.get('price_nightly', 0))
    except: rate = 0.0
    try: nights = int(reserva.get('days_number', 0))
    except: nights = 0

    props = {
        "Name": {"title": [{"text": {"content": nome[:100]}}]},
        "Res #": {"rich_text": [{"text": {"content": res_id}}]},
        "Status": {"select": {"name": status_visual}},
        "State": {"select": {"name": state_binario}},
        "NTS": {"number": nights},
        "GST": {"rich_text": [{"text": {"content": gst}}]},
        "Room": {"rich_text": [{"text": {"content": room[:200]}}]},
        "Property Group": {"select": {"name": str(nome_grupo)}},
        "Total": {"number": total},
        "TL Rate": {"number": rate}
    }
    if dt_criacao: props["Created"] = {"date": {"start": formatar_iso_date(dt_criacao)}}
    if dt_ci: props["CI"] = {"date": {"start": formatar_iso_date(dt_ci)}}
    if dt_co: props["CO"] = {"date": {"start": formatar_iso_date(dt_co)}}

    page_id = buscar_pagina_notion(res_id)
    payload = {"properties": props}
    
    if page_id:
        requests.patch(f"{URL_NOTION}/pages/{page_id}", json=payload, headers=HEADERS_NOTION)
        print(f"   🔄 {res_id} (Ano {dt_ci.year}) -> {nome_grupo}")
    else:
        payload["parent"] = {"database_id": NOTION_DATABASE_ID}
        requests.post(f"{URL_NOTION}/pages", json=payload, headers=HEADERS_NOTION)
        print(f"   ✨ {res_id} (Ano {dt_ci.year}) -> {nome_grupo}")

def executar():
    print("🚀 SINCRONIZANDO HISTÓRICO TOTAL (2024 - 2026)...")
    page = 1
    while True:
        print(f"\n📖 Lendo Página {page}...")
        payload = {
            "methodName": "GetReservationsFiltered",
            "params": {
                "token_key": STREAMLINE_KEY,
                "token_secret": STREAMLINE_SECRET,
                "return_full": True,
                "limit": 50,
                "p": page,
                "modified_since": "2024-01-01 00:00:00" # Começa do passado
            }
        }
        try:
            r = requests.post(URL_STREAMLINE, json=payload, timeout=60)
            dados = r.json()
            data_resp = dados.get('data', {}) or dados.get('Response', {}).get('data', {})
            reservas = data_resp.get('reservations', [])
            if not reservas: break
            for res in reservas: upsert_reserva(res)
            page += 1
            time.sleep(0.5)
        except: break

if __name__ == "__main__":
    executar()
