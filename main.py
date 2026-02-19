import requests
import os
import time
from datetime import datetime
from notion_client import Client

# --- CONFIGURAÇÕES ---
STREAMLINE_KEY = os.getenv("STREAMLINE_KEY")
STREAMLINE_SECRET = os.getenv("STREAMLINE_SECRET")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")

URL_STREAMLINE = "https://web.streamlinevrs.com/api/json"

# Inicializa Notion
notion = Client(auth=NOTION_TOKEN)

# Cache para evitar chamadas repetidas
CACHE_PROPERTY_GROUPS = {}

# --- FUNÇÕES ---

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
    elif code_str == '9': suffix = "REQ"
    
    # Previne erro se tipo for None
    tipo_safe = str(tipo) if tipo else "---"
    tipo_limpo = tipo_safe.split(' ')[0][:10]
    return f"{tipo_limpo}-{suffix}"

def buscar_pagina_notion(res_number):
    try:
        response = notion.databases.query(
            database_id=NOTION_DB_ID,
            filter={"property": "Res #", "rich_text": {"equals": str(res_number)}}
        )
        if response["results"]:
            return response["results"][0]["id"]
    except:
        pass
    return None

def buscar_grupo_oficial(unit_id):
    """
    Consulta a API GetPropertyInfo para pegar o Property Group REAL.
    """
    unit_id_str = str(unit_id)
    if unit_id_str in CACHE_PROPERTY_GROUPS:
        return CACHE_PROPERTY_GROUPS[unit_id_str]

    # print(f"   🔍 Consultando Unit ID {unit_id_str}...") # Debug visual
    payload = {
        "methodName": "GetPropertyInfo",
        "params": {
            "token_key": STREAMLINE_KEY,
            "token_secret": STREAMLINE_SECRET,
            "unit_id": unit_id
        }
    }
    
    try:
        # Timeout curto para essa chamada específica não travar tudo
        response = requests.post(URL_STREAMLINE, json=payload, timeout=20)
        data = response.json()
        
        info = {}
        if 'data' in data: info = data['data']
        elif 'Response' in data and 'data' in data['Response']: info = data['Response']['data']
        
        # Tenta pegar o nome oficial do grupo
        group_name = info.get('condo_type_group_name')
        
        if not group_name:
            group_name = info.get('location_name', '---')
            
        CACHE_PROPERTY_GROUPS[unit_id_str] = str(group_name).strip()
        return CACHE_PROPERTY_GROUPS[unit_id_str]

    except Exception:
        # Se der erro, assume vazio para não parar o script
        return None

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    if not res_id: return
    
    # 1. Busca o Grupo Oficial (Cruzamento de Dados)
    unit_id = reserva.get('unit_id')
    prop_group_real = buscar_grupo_oficial(unit_id)
    
    # Formata para Select do Notion (sem vírgulas)
    if prop_group_real:
        prop_group_clean = prop_group_real.replace(",", "").strip()[:100]
        prop_group_payload = {"select": {"name": prop_group_clean}}
    else:
        prop_group_payload = {"select": None}

    # 2. Prepara os dados
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
        "Name": {"title": [{"text": {"content": nome[:100]}}]},
        "Res #": {"rich_text": [{"text": {"content": res_id}}]},
        "Status": {"select": {"name": status_visual}},
        "State": {"select": {"name": state_binario}},
        "NTS": {"number": nights},
        "GST": {"rich_text": [{"text": {"content": gst}}]},
        "Room": {"rich_text": [{"text": {"content": room[:200]}}]},
        "Property Group": prop_group_payload, 
        "Total": {"number": total},
        "TL Rate": {"number": rate}
    }

    if dt_criacao: props["Created"] = {"date": {"start": formatar_iso_date(dt_criacao)}}
    if dt_ci: props["CI"] = {"date": {"start": formatar_iso_date(dt_ci)}}
    if dt_co: props["CO"] = {"date": {"start": formatar_iso_date(dt_co)}}

    # 3. Envia ao Notion
    page_id = buscar_pagina_notion(res_id)
    
    for _ in range(3):
        try:
            if page_id:
                notion.pages.update(page_id=page_id, properties=props)
                print(f"   🔄 {res_id} (Upd) -> {prop_group_clean}")
            else:
                notion.pages.create(parent={"database_id": NOTION_DB_ID}, properties=props)
                print(f"   ✨ {res_id} (New) -> {prop_group_clean}")
            time.sleep(0.4) # Respeita limite da API Notion
            return
        except Exception as e:
            # print(f"Erro Notion: {e}")
            time.sleep(1)

def executar_sincronizacao():
    print("🚀 Sincronizando TUDO (Modo Direto sem Paginação)...")
    
    # Removi 'limit' e 'p' para obrigar a API a trazer tudo que mudou desde 2023
    # Usei formato de data com barras, às vezes a API prefere.
    payload = {
        "methodName": "GetReservationsFiltered",
        "params": {
            "token_key": STREAMLINE_KEY,
            "token_secret": STREAMLINE_SECRET,
            "return_full": True,
            "modified_since": "01/01/2023" 
        }
    }

    try:
        # Timeout de 120s para garantir que dê tempo de baixar o listão
        response = requests.post(URL_STREAMLINE, json=payload, timeout=120)
        
        # --- DEBUG: Se der erro, vamos ver o que é ---
        if response.status_code != 200:
            print(f"❌ Erro HTTP: {response.status_code}")
            print(response.text)
            return

        dados = response.json()

        lista_reservas = []
        if 'data' in dados and 'reservations' in dados['data']:
            lista_reservas = dados['data']['reservations']
        elif 'Response' in dados:
            lista_reservas = dados['Response'].get('data', [])
        
        print(f"📦 Recebi {len(lista_reservas)} reservas no total.")

        for i, r in enumerate(lista_reservas):
            # Print de progresso a cada 20 itens para você saber que não travou
            if i > 0 and i % 20 == 0: 
                print(f"   ...Processando {i}/{len(lista_reservas)}")
                
            upsert_reserva(r)

    except Exception as e:
        print(f"❌ Erro fatal: {e}")

    print("\n✅ FIM DA SINCRONIZAÇÃO!")

if __name__ == "__main__":
    executar_sincronizacao()
