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

# --- FUNÇÕES DE DATA E NOTION ---

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
    
    dt_criacao = parse_dt_robusto(reserva.get('creation_date'))
    dt_ci = parse_dt_robusto(reserva.get('startdate') or reserva.get('start_date'))
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
        "Name": {"title": [{"text": {"content": nome}}]},
        "Res #": {"rich_text": [{"text": {"content": res_id}}]},
        "Status": {"select": {"name": status_visual}},
        "State": {"select": {"name": state_binario}},
        "NTS": {"number": nights},
        "GST": {"rich_text": [{"text": {"content": gst}}]},
        "Room": {"rich_text": [{"text": {"content": room}}]},
        "Total": {"number": total},
        "TL Rate": {"number": rate}
    }
    if dt_criacao: props["Created"] = {"date": {"start": formatar_iso_date(dt_criacao)}}
    if dt_ci: props["CI"] = {"date": {"start": formatar_iso_date(dt_ci)}}
    if dt_co: props["CO"] = {"date": {"start": formatar_iso_date(dt_co)}}

    page_id = buscar_pagina_notion(res_id)
    payload = {"properties": props}
    
    # Tentativa de salvamento com Retry
    for tentativa in range(3):
        try:
            if page_id:
                res = requests.patch(f"{URL_NOTION}/pages/{page_id}", json=payload, headers=HEADERS_NOTION)
                print(f"   🔄 Atualizada: {res_id}")
            else:
                payload["parent"] = {"database_id": NOTION_DATABASE_ID}
                res = requests.post(f"{URL_NOTION}/pages", json=payload, headers=HEADERS_NOTION)
                print(f"   ✨ Criada: {res_id}")
            
            if res.status_code == 429: # Rate limit
                time.sleep(2)
                continue
            break
        except:
            time.sleep(1)

# --- FUNÇÕES CORE DO STREAMLINE ---

def baixar_detalhes_em_lote(lista_ids):
    """Pega uma lista de IDs e baixa os detalhes usando GetReservationsFiltered filtrando por ID"""
    
    # Transforma a lista [123, 124] em string "123,124" (se API pedir string)
    # Mas o Streamline geralmente aceita array no JSON. Vamos tentar array.
    
    payload = {
        "methodName": "GetReservationsFiltered",
        "params": {
            "token_key": STREAMLINE_KEY,
            "token_secret": STREAMLINE_SECRET,
            "confirmation_id": lista_ids, # O TRUQUE: Filtrar por estes IDs específicos
            "return_full": True
        }
    }
    
    try:
        r = requests.post(URL_STREAMLINE, json=payload, timeout=60)
        dados = r.json()
        
        reservas = []
        if 'data' in dados and 'reservations' in dados['data']:
            reservas = dados['data']['reservations']
        elif 'Response' in dados:
            reservas = dados['Response'].get('data', [])
            
        return reservas
    except Exception as e:
        print(f"❌ Erro ao baixar lote: {e}")
        return []

def executar_sincronizacao():
    print("🚀 Sincronização HÍBRIDA (IDs -> Detalhes)...")
    
    # 1. PEGAR A LISTA DE IDS (Isso é rápido e leve)
    print("📋 Baixando lista de IDs recentes (Desde 2024)...")
    
    payload_ids = {
        "methodName": "GetReservationsFiltered",
        "params": {
            "token_key": STREAMLINE_KEY,
            "token_secret": STREAMLINE_SECRET,
            "modified_since": "2024-01-01 00:00:00",
            "return_full": False # APENAS IDs
        }
    }
    
    todos_ids = []
    try:
        r = requests.post(URL_STREAMLINE, json=payload_ids, timeout=60)
        dados = r.json()
        
        # O debug mostrou que vem em data -> confirmation_id (lista)
        if 'data' in dados and 'confirmation_id' in dados['data']:
            todos_ids = dados['data']['confirmation_id']
        else:
            print(f"❌ Estrutura inesperada: {dados.keys()}")
            return

    except Exception as e:
        print(f"❌ Erro fatal ao buscar IDs: {e}")
        return

    total = len(todos_ids)
    print(f"✅ Encontrados {total} IDs para processar.")
    
    if total == 0: return

    # 2. PROCESSAR EM LOTES DE 20 (Para não travar)
    tamanho_lote = 20
    
    for i in range(0, total, tamanho_lote):
        lote_ids = todos_ids[i : i + tamanho_lote]
        print(f"\n📦 Processando lote {i} a {i+len(lote_ids)} de {total}...")
        
        # Baixa detalhes só desses 20
        reservas_detalhadas = baixar_detalhes_em_lote(lote_ids)
        
        if not reservas_detalhadas:
            print("   ⚠️ Lote vazio ou erro de download.")
            continue
            
        # Salva no Notion
        for reserva in reservas_detalhadas:
            upsert_reserva(reserva)
            
        time.sleep(0.5) # Respira para não travar API

    print("\n🏁 Sincronização Finalizada!")

if __name__ == "__main__":
    executar_sincronizacao()
