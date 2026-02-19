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

CACHE_GRUPOS = {}

# --- FUNÇÕES ---

def buscar_nomes_dos_grupos():
    payload = {
        "methodName": "GetRoomTypeGroupsList",
        "params": {"token_key": STREAMLINE_KEY, "token_secret": STREAMLINE_SECRET}
    }
    try:
        r = requests.post(URL_STREAMLINE, json=payload, timeout=30)
        dados = r.json()
        grupos = dados.get('data', {}).get('group', [])
        if isinstance(grupos, dict): grupos = [grupos]
        return {str(g.get('id')): g.get('name') for g in grupos}
    except: return {}

def extrair_property_group(r):
    """
    NOVA LÓGICA: Procura 'Bolivar' ou 'San Antonio' em qualquer lugar da reserva.
    """
    prioritarios = ["Bolivar Vacations", "San Antonio"]
    
    # 1. BUSCA EXAUSTIVA: Procura as palavras-chave em todos os campos de texto da reserva
    # Isso resolve o caso da 'Flip Flop' que tem 'Bolivar' escondido em algum campo
    for p in prioritarios:
        for valor in r.values():
            if isinstance(valor, str) and p.lower() in valor.lower():
                return p

    # 2. SE NÃO ACHOU PRIORIDADE, tenta a lógica de mapeamento por ID
    group_id = str(r.get('room_type_group_id', ''))
    api_group_name = CACHE_GRUPOS.get(group_id, "")
    
    # 3. SE O NOME DA API FOR GENÉRICO (ex: '4 Bedroom'), tenta limpar o nome da casa
    unit_name = str(r.get('unit_name', '')).strip()
    if api_group_name and ("bedroom" in api_group_name.lower() or "studio" in api_group_name.lower()):
        for sep in [" - ", " | ", " # "]:
            if sep in unit_name:
                return unit_name.split(sep)[0].strip()

    # Fallback final
    return api_group_name or r.get('condo_type_name') or r.get('location_name') or "Geral"

def parse_dt_robusto(data_str):
    if not data_str: return None
    try:
        data_str = str(data_str).strip()
        formatos = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]
        for fmt in formatos:
            try: return datetime.strptime(data_str, fmt)
            except ValueError: continue
    except: return None
    return None

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

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    dt_ci = parse_dt_robusto(reserva.get('startdate') or reserva.get('start_date'))
    
    # Mantendo o filtro de 2026 para o seu teste
    if not dt_ci or dt_ci.year != 2026: return

    # Aqui a nova lógica entra em ação
    pg_clean = str(extrair_property_group(reserva)).replace(",", "").strip()[:100]

    props = {
        "Name": {"title": [{"text": {"content": f"{reserva.get('first_name', '')} {reserva.get('last_name', '')}"[:100]}}]},
        "Res #": {"rich_text": [{"text": {"content": res_id}}]},
        "Room": {"rich_text": [{"text": {"content": str(reserva.get('unit_name', ''))[:200]}}]},
        "Property Group": {"select": {"name": pg_clean}},
        "Total": {"number": float(reserva.get('price_total', 0) or 0)},
        "CI": {"date": {"start": dt_ci.strftime("%Y-%m-%d")}}
    }

    page_id = buscar_pagina_notion(res_id)
    payload = {"properties": props}
    
    if page_id:
        requests.patch(f"{URL_NOTION}/pages/{page_id}", json=payload, headers=HEADERS_NOTION)
        print(f"   🔄 {res_id} Atualizada -> {pg_clean}")
    else:
        payload["parent"] = {"database_id": NOTION_DATABASE_ID}
        requests.post(f"{URL_NOTION}/pages", json=payload, headers=HEADERS_NOTION)
        print(f"   ✨ {res_id} Criada -> {pg_clean}")

def executar_sincronizacao():
    global CACHE_GRUPOS
    print("🚀 Sincronizando (Busca Total de Prioridades)...")
    CACHE_GRUPOS = buscar_nomes_dos_grupos()

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
                "modified_since": "2024-01-01 00:00:00"
            }
        }
        r = requests.post(URL_STREAMLINE, json=payload, timeout=60)
        lista = r.json().get('data', {}).get('reservations', [])
        if not lista: break

        for res in lista: upsert_reserva(res)
        page += 1
        time.sleep(1)

if __name__ == "__main__":
    executar_sincronizacao()
