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

# Variável global para guardar os nomes dos grupos
CACHE_GRUPOS = {}

# --- FUNÇÕES AUXILIARES ---

def buscar_nomes_dos_grupos():
    """Baixa a lista do GetRoomTypeGroupsList para traduzir IDs em nomes"""
    print("🔍 Buscando definições de grupos no Streamline...")
    payload = {
        "methodName": "GetRoomTypeGroupsList",
        "params": {
            "token_key": STREAMLINE_KEY,
            "token_secret": STREAMLINE_SECRET
        }
    }
    try:
        r = requests.post(URL_STREAMLINE, json=payload, timeout=30)
        dados = r.json()
        grupos = dados.get('data', {}).get('group', [])
        if isinstance(grupos, dict): grupos = [grupos] # Caso venha só um
        
        mapping = {}
        for g in grupos:
            mapping[str(g.get('id'))] = g.get('name')
        return mapping
    except:
        return {}

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

def extrair_property_group(r):
    """
    Lógica priorizada:
    1. Tenta traduzir o ID do grupo vindo da API.
    2. Tenta quebrar o nome da unidade (Casa).
    3. Usa o nome do condomínio.
    """
    # 1. Tenta pelo ID do Grupo (usando o que você achou na doc)
    group_id = str(r.get('room_type_group_id', ''))
    if group_id in CACHE_GRUPOS:
        return CACHE_GRUPOS[group_id]

    # 2. Tenta quebrar o nome da casa (ex: "Magic Village - 101" -> "Magic Village")
    unit_name = str(r.get('unit_name', '')).strip()
    for sep in [" - ", " | ", " # ", " @ "]:
        if sep in unit_name:
            return unit_name.split(sep)[0].strip()
    
    # 3. Fallback para campos nativos
    return r.get('condo_type_name') or r.get('location_name') or "Geral"

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
    if not res_id: return
    
    dt_ci = parse_dt_robusto(reserva.get('startdate') or reserva.get('start_date'))
    if not dt_ci or dt_ci.year != 2026:
        return # Só 2026 como você pediu

    nome = f"{reserva.get('first_name', '')} {reserva.get('last_name', '')}".strip()
    room = str(reserva.get('unit_name', 'Unknown'))
    
    # Property Group Inteligente
    pg_raw = extrair_property_group(reserva)
    pg_clean = str(pg_raw).replace(",", "").strip()[:100]

    props = {
        "Name": {"title": [{"text": {"content": nome[:100]}}]},
        "Res #": {"rich_text": [{"text": {"content": res_id}}]},
        "Status": {"select": {"name": "BKD"}}, # Simplificado para o teste
        "Room": {"rich_text": [{"text": {"content": room[:200]}}]},
        "Property Group": {"select": {"name": pg_clean}}, # Campo solicitado
        "Total": {"number": float(reserva.get('price_total', 0) or 0)}
    }

    # Datas
    dt_ci_str = dt_ci.strftime("%Y-%m-%d")
    props["CI"] = {"date": {"start": dt_ci_str}}

    page_id = buscar_pagina_notion(res_id)
    payload = {"properties": props}
    
    if page_id:
        requests.patch(f"{URL_NOTION}/pages/{page_id}", json=payload, headers=HEADERS_NOTION)
        print(f"   🔄 {res_id} (Atualizada) -> {pg_clean}")
    else:
        payload["parent"] = {"database_id": NOTION_DATABASE_ID}
        requests.post(f"{URL_NOTION}/pages", json=payload, headers=HEADERS_NOTION)
        print(f"   ✨ {res_id} (Nova) -> {pg_clean}")

def executar_sincronizacao():
    global CACHE_GRUPOS
    print("🚀 Iniciando Teste 2026...")
    
    # Carrega o mapeamento de grupos antes de começar
    CACHE_GRUPOS = buscar_nomes_dos_grupos()
    print(f"✅ {len(CACHE_GRUPOS)} grupos mapeados.")

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
                "modified_since": "2024-01-01 00:00:00" # Pega as que foram mexidas
            }
        }
        try:
            r = requests.post(URL_STREAMLINE, json=payload, timeout=60)
            dados = r.json()
            lista = dados.get('data', {}).get('reservations', [])
            if not lista: break

            for r_item in lista:
                upsert_reserva(r_item)
            
            page += 1
            time.sleep(1)
        except Exception as e:
            print(f"❌ Erro: {e}")
            break

if __name__ == "__main__":
    executar_sincronizacao()
