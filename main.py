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

# Cache para não repetir chamadas de API para a mesma casa
MAPA_DNA_CASAS = {}

# --- FUNÇÕES ---

def buscar_dna_da_casa(unit_id):
    """
    USA O GetPropertyInfo QUE VOCÊ ENCONTROU.
    Descobre o grupo atual da casa no Streamline.
    """
    if str(unit_id) in MAPA_DNA_CASAS:
        return MAPA_DNA_CASAS[str(unit_id)]

    print(f"   🔍 Investigando DNA da Unidade {unit_id}...")
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
        # Acessa os dados conforme a documentação enviada
        res_data = dados.get('data', {}) or dados.get('Response', {}).get('data', {})
        
        # Prioridade de Campos do Property Group:
        # 1. location_resort_name (Onde costuma estar Bolivar/San Antonio)
        # 2. condo_type_group_name (ex: 4 Bedroom)
        # 3. condo_type_name
        
        grupo_atual = (
            res_data.get('location_resort_name') or 
            res_data.get('condo_type_group_name') or 
            res_data.get('condo_type_name') or 
            "Geral"
        )
        
        # Limpeza extra para os seus grupos prioritários
        u_name = str(res_data.get('unit_name', '')).lower()
        if "bolivar" in u_name or "bolivar" in str(grupo_atual).lower():
            grupo_atual = "Bolivar Vacations"
        elif "san antonio" in u_name or "san antonio" in str(grupo_atual).lower():
            grupo_atual = "San Antonio"

        MAPA_DNA_CASAS[str(unit_id)] = grupo_atual
        return grupo_atual

    except Exception as e:
        print(f"   ⚠️ Erro ao buscar info da casa {unit_id}: {e}")
        return "Geral"

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    dt_raw = reserva.get('startdate') or reserva.get('start_date')
    
    # Filtro focado em 2026
    if not dt_raw or "2026" not in str(dt_raw):
        return

    # BUSCA O DNA ATUAL DA CASA USANDO O ID
    unit_id = reserva.get('unit_id') or reserva.get('home_id')
    nome_grupo = buscar_dna_da_casa(unit_id)
    
    unit_name = str(reserva.get('unit_name', ''))
    hospede = f"{reserva.get('first_name', '')} {reserva.get('last_name', '')}"[:100]

    props = {
        "Name": {"title": [{"text": {"content": hospede}}]},
        "Res #": {"rich_text": [{"text": {"content": res_id}}]},
        "Room": {"rich_text": [{"text": {"content": unit_name[:200]}}]},
        "Property Group": {"select": {"name": str(nome_grupo)}},
        "Total": {"number": float(reserva.get('price_total', 0) or 0)},
        "CI": {"date": {"start": str(dt_raw)[:10]}}
    }

    # Notion: Update ou Create
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
    print("🚀 Sincronizando 2026 com Mapeamento de DNA sob demanda...")
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
                "modified_since": "2026-01-01 00:00:00"
            }
        }
        
        try:
            r = requests.post(URL_STREAMLINE, json=payload, timeout=60)
            dados = r.json()
            data_resp = dados.get('data', {}) or dados.get('Response', {}).get('data', {})
            reservas = data_resp.get('reservations', [])
            
            if not reservas:
                print("🏁 Fim das reservas encontradas.")
                break

            for res in reservas:
                upsert_reserva(res)
            
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ Erro na leitura das reservas: {e}")
            break

if __name__ == "__main__":
    executar()
