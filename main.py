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

# Mapas de Memória
CACHE_NOMES_GRUPOS = {}
MAPA_HOME_PARA_GRUPO = {}

# --- 1. MAPEAMENTO DE PROPRIEDADES ---

def carregar_mapeamento_atual():
    global CACHE_NOMES_GRUPOS, MAPA_HOME_PARA_GRUPO
    print("\n--- 🧠 MAPEANDO GRUPOS E HOMES ---")
    
    # Parte A: Pegar nomes dos 21 Grupos
    try:
        payload_gr = {
            "methodName": "GetRoomTypeGroupsList",
            "params": {"token_key": STREAMLINE_KEY, "token_secret": STREAMLINE_SECRET}
        }
        r = requests.post(URL_STREAMLINE, json=payload_gr, timeout=40)
        dados = r.json()
        data_resp = dados.get('data', {}) or dados.get('Response', {}).get('data', {})
        grupos = data_resp.get('group', [])
        if isinstance(grupos, dict): grupos = [grupos]
        
        CACHE_NOMES_GRUPOS = {str(g.get('id')): g.get('name') for g in grupos}
        print(f"✅ {len(CACHE_NOMES_GRUPOS)} grupos carregados.")
    except: print("⚠️ Erro ao carregar nomes dos grupos.")

    # Parte B: Mapear home_id para o Grupo
    try:
        payload_prop = {
            "methodName": "GetPropertiesList",
            "params": {"token_key": STREAMLINE_KEY, "token_secret": STREAMLINE_SECRET}
        }
        r = requests.post(URL_STREAMLINE, json=payload_prop, timeout=60)
        dados = r.json()
        
        # Procura a lista de casas (pode vir como 'property' ou 'home')
        data_resp = dados.get('data', {}) or dados.get('Response', {}).get('data', {})
        casas = data_resp.get('property', []) or data_resp.get('home', [])
        
        if isinstance(casas, dict): casas = [casas]
        
        for c in casas:
            # Pegamos o home_id ou unit_id (o que estiver disponível)
            h_id = str(c.get('home_id') or c.get('unit_id'))
            g_id = str(c.get('room_type_group_id'))
            
            nome_grupo = CACHE_NOMES_GRUPOS.get(g_id, "Geral")
            
            # Forçamos a prioridade por nome se o grupo na API estiver errado
            u_name = str(c.get('unit_name', '')).lower()
            if "bolivar" in u_name: nome_grupo = "Bolivar Vacations"
            elif "san antonio" in u_name: nome_grupo = "San Antonio"
            
            if h_id != "None":
                MAPA_HOME_PARA_GRUPO[h_id] = nome_grupo

        print(f"✅ {len(MAPA_HOME_PARA_GRUPO)} casas mapeadas com sucesso.")
    except Exception as e:
        print(f"❌ Erro no mapeamento: {e}")

# --- 2. SINCRONIZAÇÃO ---

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    
    # Filtro de Ano (Sempre 2026 para o teste)
    dt_raw = reserva.get('startdate') or reserva.get('start_date')
    if not dt_raw or "2026" not in str(dt_raw): return

    # A CHAVE: Procurar pelo home_id da reserva no nosso mapa de casas
    h_id_res = str(reserva.get('home_id') or reserva.get('unit_id'))
    nome_grupo = MAPA_HOME_PARA_GRUPO.get(h_id_res, "Geral")
    
    unit_name = str(reserva.get('unit_name', ''))
    hospede = f"{reserva.get('first_name', '')} {reserva.get('last_name', '')}"[:100]

    props = {
        "Name": {"title": [{"text": {"content": hospede}}]},
        "Res #": {"rich_text": [{"text": {"content": res_id}}]},
        "Room": {"rich_text": [{"text": {"content": unit_name[:200]}}]},
        "Property Group": {"select": {"name": nome_grupo}},
        "Total": {"number": float(reserva.get('price_total', 0) or 0)},
        "CI": {"date": {"start": str(dt_raw)[:10]}}
    }

    # Verifica se já existe para atualizar
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

def executar():
    carregar_mapeamento_atual()
    
    if not MAPA_HOME_PARA_GRUPO:
        print("⚠️ Atenção: Nenhuma casa foi mapeada. Verifique os campos da API.")
    
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
            dados = r.json()
            data_resp = dados.get('data', {}) or dados.get('Response', {}).get('data', {})
            reservas = data_resp.get('reservations', [])
            
            if not reservas: break

            for res in reservas:
                upsert_reserva(res)
            
            page += 1
            time.sleep(1)
        except Exception as e:
            print(f"❌ Erro: {e}")
            break

if __name__ == "__main__":
    executar()
