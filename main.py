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

# Memória do Robô
CACHE_GRUPOS = {}
MAPA_PROPRIEDADES = {}

# --- 1. MAPEAMENTO (PEGA A CONFIGURAÇÃO ATUAL) ---

def carregar_mapeamento_atual():
    global CACHE_GRUPOS, MAPA_PROPRIEDADES
    print("\n--- 🧠 CARREGANDO CONFIGURAÇÃO ATUAL DAS CASAS ---")
    
    # Parte A: Lista os 21 Grupos
    payload_gr = {
        "methodName": "GetRoomTypeGroupsList",
        "params": {"token_key": STREAMLINE_KEY, "token_secret": STREAMLINE_SECRET}
    }
    try:
        r = requests.post(URL_STREAMLINE, json=payload_gr, timeout=45)
        res_json = r.json()
        # Trata as variações de estrutura da API
        data_gr = res_json.get('data', {}) or res_json.get('Response', {}).get('data', {})
        grupos = data_gr.get('group', [])
        if isinstance(grupos, dict): grupos = [grupos]
        CACHE_GRUPOS = {str(g.get('id')): g.get('name') for g in grupos}
        print(f"✅ {len(CACHE_GRUPOS)} grupos identificados.")
    except: print("⚠️ Falha ao carrerar nomes dos grupos.")

    # Parte B: Lista todas as Casas (Properties) e seus grupos atuais
    payload_prop = {
        "methodName": "GetPropertiesList",
        "params": {"token_key": STREAMLINE_KEY, "token_secret": STREAMLINE_SECRET, "return_full": True}
    }
    try:
        r = requests.post(URL_STREAMLINE, json=payload_prop, timeout=60)
        res_json = r.json()
        data_prop = res_json.get('data', {}) or res_json.get('Response', {}).get('data', {})
        casas = data_prop.get('property', [])
        if isinstance(casas, dict): casas = [casas]
        
        for c in casas:
            u_id = str(c.get('unit_id'))
            g_id = str(c.get('room_type_group_id'))
            u_name = str(c.get('unit_name', '')).lower()
            
            nome_grupo = CACHE_GRUPOS.get(g_id, "Geral")
            
            # Prioridade Bolivar e San Antonio baseada no nome da casa
            if "bolivar" in u_name: nome_grupo = "Bolivar Vacations"
            elif "san antonio" in u_name: nome_grupo = "San Antonio"
            
            MAPA_PROPRIEDADES[u_id] = nome_grupo
        print(f"✅ {len(MAPA_PROPRIEDADES)} casas mapeadas aos seus grupos atuais.")
    except Exception as e:
        print(f"❌ Erro ao mapear casas: {e}")

# --- 2. AUXILIARES ---

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

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    dt_ci = parse_dt_robusto(reserva.get('startdate') or reserva.get('start_date'))
    
    # FILTRO 2026
    if not dt_ci or dt_ci.year != 2026: return

    # PEGA O GRUPO ATUAL DA CASA (IGNORA O PASSADO DA RESERVA)
    unit_id = str(reserva.get('unit_id'))
    nome_grupo = MAPA_PROPRIEDADES.get(unit_id)
    
    # Se não achou no mapa, tenta uma busca por texto no nome da reserva
    if not nome_grupo:
        u_name_res = str(reserva.get('unit_name', '')).lower()
        if "bolivar" in u_name_res: nome_grupo = "Bolivar Vacations"
        elif "san antonio" in u_name_res: nome_grupo = "San Antonio"
        else: nome_grupo = "Geral"

    nome_hospede = f"{reserva.get('first_name', '')} {reserva.get('last_name', '')}"[:100]
    unit_name = str(reserva.get('unit_name', ''))[:200]

    props = {
        "Name": {"title": [{"text": {"content": nome_hospede}}]},
        "Res #": {"rich_text": [{"text": {"content": res_id}}]},
        "Room": {"rich_text": [{"text": {"content": unit_name}}]},
        "Property Group": {"select": {"name": str(nome_grupo)}},
        "Total": {"number": float(reserva.get('price_total', 0) or 0)},
        "CI": {"date": {"start": dt_ci.strftime("%Y-%m-%d")}}
    }

    # Busca no Notion
    query = requests.post(f"{URL_NOTION}/databases/{NOTION_DATABASE_ID}/query", 
                          json={"filter": {"property": "Res #", "rich_text": {"equals": res_id}}}, 
                          headers=HEADERS_NOTION).json()
    
    if query.get("results"):
        page_id = query["results"][0]["id"]
        requests.patch(f"{URL_NOTION}/pages/{page_id}", json={"properties": props}, headers=HEADERS_NOTION)
        print(f"   🔄 {res_id} ({unit_name}) -> {nome_grupo}")
    else:
        requests.post(f"{URL_NOTION}/pages", 
                      json={"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props}, 
                      headers=HEADERS_NOTION)
        print(f"   ✨ {res_id} ({unit_name}) -> {nome_grupo}")

# --- 3. EXECUÇÃO ---

def executar():
    carregar_mapeamento_atual()
    
    page = 1
    while True:
        print(f"\n📖 Lendo Reservas - Página {page}...")
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
        
        try:
            r = requests.post(URL_STREAMLINE, json=payload, timeout=60)
            res_json = r.json()
            
            # TRATAMENTO ROBUSTO PARA ACHAR A LISTA DE RESERVAS
            data_resp = res_json.get('data', {}) or res_json.get('Response', {}).get('data', {})
            # Em algumas versões a lista está direto em Response -> data
            reservas = data_resp.get('reservations') if isinstance(data_resp, dict) else None
            if reservas is None:
                # Tentativa final: Response -> data é a própria lista
                if isinstance(data_resp, list):
                    reservas = data_resp
                else:
                    reservas = []

            qtd = len(reservas)
            print(f"📦 {qtd} reservas encontradas.")

            if qtd == 0: break

            for res in reservas:
                upsert_reserva(res)
            
            page += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Erro na página {page}: {e}")
            break

    print("\n🏁 Sincronização Finalizada!")

if __name__ == "__main__":
    executar()
