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
    
    tipo_limpo = str(tipo).split(' ')[0][:10]
    return f"{tipo_limpo}-{suffix}"

def determinar_stat_pagamento(r):
    try:
        balance = float(r.get('price_balance', 0))
        return "PI" if balance <= 0 else "CC"
    except:
        return "CC"

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

def extrair_property_group(r):
    """
    Tenta extrair o Grupo removendo o nome da unidade do tipo do condomínio.
    Ex: 'Bolivar Vacations Sangria Sunset' - 'Sangria Sunset' = 'Bolivar Vacations'
    """
    unit_name = str(r.get('unit_name', '')).strip()
    condo_type = str(r.get('condo_type_name', '')).strip()
    
    if unit_name and unit_name in condo_type:
        group = condo_type.replace(unit_name, "").strip()
        if group: return group
    
    # Se falhar a limpeza, retorna o condo_type inteiro ou location_name como fallback
    return condo_type if condo_type else str(r.get('location_name', '---'))

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    if not res_id: return
    
    # Datas
    dt_criacao = parse_dt_robusto(reserva.get('creation_date'))
    dt_ci = parse_dt_robusto(reserva.get('startdate') or reserva.get('start_date'))
    dt_co = parse_dt_robusto(reserva.get('enddate') or reserva.get('end_date'))
    
    nome = f"{reserva.get('first_name', '')} {reserva.get('last_name', '')}".strip()
    status_visual = gerar_status_visual(reserva.get('type_name', '---'), reserva.get('status_code'))
    state_binario = obter_estado_binario(reserva.get('status_code'))
    room = str(reserva.get('unit_name', 'Unknown'))
    gst = f"{reserva.get('occupants',0)}|{reserva.get('occupants_small',0)}"
    
    # --- EXTRAÇÃO INTELIGENTE DO PROPERTY GROUP ---
    prop_group = extrair_property_group(reserva)

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
        "Property Group": {"rich_text": [{"text": {"content": prop_group[:200]}}]}, # Aqui vai o grupo limpo
        "Total": {"number": total},
        "TL Rate": {"number": rate}
    }

    if dt_created: props["Created"] = {"date": {"start": formatar_iso_date(dt_criacao)}}
    if dt_ci: props["CI"] = {"date": {"start": formatar_iso_date(dt_ci)}}
    if dt_co: props["CO"] = {"date": {"start": formatar_iso_date(dt_co)}}

    page_id = buscar_pagina_notion(res_id)
    
    for _ in range(3):
        try:
            if page_id:
                notion.pages.update(page_id=page_id, properties=props)
                print(f"   🔄 {res_id} (Upd) -> {prop_group}")
            else:
                notion.pages.create(parent={"database_id": NOTION_DB_ID}, properties=props)
                print(f"   ✨ {res_id} (New) -> {prop_group}")
            time.sleep(0.4)
            return
        except Exception as e:
            time.sleep(1)

def executar_sincronizacao():
    print("🚀 Sincronizando TUDO (Com Property Group Extraído)...")
    
    page = 1
    total_processado = 0
    limit = 50 

    while True:
        print(f"\n📖 Lendo Página {page}...")

        payload = {
            "methodName": "GetReservationsFiltered",
            "params": {
                "token_key": STREAMLINE_KEY,
                "token_secret": STREAMLINE_SECRET,
                "return_full": True,
                "limit": limit,      
                "p": page,
                "modified_since": "2023-01-01 00:00:00" # Histórico longo
            }
        }

        try:
            response = requests.post(URL_STREAMLINE, json=payload, timeout=90)
            
            try: dados = response.json()
            except: 
                page += 1
                continue

            # Tratamento de erro de limite da API
            if isinstance(dados, dict) and 'status' in dados and dados['status'].get('code') == 'E0105':
                print("⚠️ Erro de limite API. Pausando 10s...")
                time.sleep(10)
                continue

            lista_reservas = []
            if 'data' in dados and 'reservations' in dados['data']:
                lista_reservas = dados['data']['reservations']
            elif 'Response' in dados:
                lista_reservas = dados['Response'].get('data', [])
            
            qtd = len(lista_reservas)
            print(f"📦 {qtd} reservas na página.")

            if qtd == 0:
                print("🏁 Sincronização Finalizada!")
                break

            for r in lista_reservas:
                upsert_reserva(r)
            
            total_processado += qtd
            page += 1
            time.sleep(1) 

        except Exception as e:
            print(f"❌ Erro de conexão: {e}")
            time.sleep(5)

    print(f"\n✅ Total Processado: {total_processado}")

if __name__ == "__main__":
    executar_sincronizacao()
