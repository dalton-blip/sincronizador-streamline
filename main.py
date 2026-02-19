import requests
import json
import os
import time
from datetime import datetime
from notion_client import Client

# --- CONFIGURAÇÕES ---
# Certifique-se que essas variáveis estão no seu arquivo .env ou no servidor
STREAMLINE_KEY = os.getenv("STREAMLINE_KEY")
STREAMLINE_SECRET = os.getenv("STREAMLINE_SECRET")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")

URL_STREAMLINE = "https://web.streamlinevrs.com/api/json"

# Inicializa Notion
notion = Client(auth=NOTION_TOKEN)

# --- FUNÇÕES DE TRATAMENTO ---

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

def extrair_property_group(r):
    """
    Extrai o nome do Grupo a partir do nome da Unidade (Casa).
    Ex: "Residencial Flores - Casa 01" -> "Residencial Flores"
    """
    unit_name = str(r.get('unit_name', '')).strip()
    
    # Se o nome da casa tiver um separador, pegamos a parte antes dele
    separadores = [" - ", " | ", " # "]
    for sep in separadores:
        if sep in unit_name:
            return unit_name.split(sep)[0].strip()
    
    # Fallback: Se não houver separador, tenta o campo nativo de condomínio
    condo = str(r.get('condo_type_name', '')).strip()
    if condo and condo.lower() != "none" and condo != "":
        return condo
        
    return str(r.get('location_name', 'Geral')).strip()

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

def upsert_reserva(reserva, ano_alvo):
    res_id = str(reserva.get('confirmation_id'))
    if not res_id: return
    
    # Datas
    dt_criacao = parse_dt_robusto(reserva.get('creation_date'))
    dt_ci = parse_dt_robusto(reserva.get('startdate') or reserva.get('start_date'))
    dt_co = parse_dt_robusto(reserva.get('enddate') or reserva.get('end_date'))
    
    # Filtro opcional: Só processa se o check-in for do ano que estamos varrendo
    if dt_ci and dt_ci.year != ano_alvo:
        return

    nome = f"{reserva.get('first_name', '')} {reserva.get('last_name', '')}".strip()
    status_visual = gerar_status_visual(reserva.get('type_name', '---'), reserva.get('status_code'))
    state_binario = obter_estado_binario(reserva.get('status_code'))
    room = str(reserva.get('unit_name', 'Unknown'))
    gst = f"{reserva.get('occupants',0)}|{reserva.get('occupants_small',0)}"
    
    # --- TRATAMENTO PROPERTY GROUP (SELECT) ---
    prop_group_raw = extrair_property_group(reserva)
    prop_group_clean = prop_group_raw.replace(",", "").strip()[:100]
    prop_group_payload = {"select": {"name": prop_group_clean}} if prop_group_clean else {"select": None}

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

    page_id = buscar_pagina_notion(res_id)
    
    for _ in range(3):
        try:
            if page_id:
                notion.pages.update(page_id=page_id, properties=props)
                print(f"    🔄 {res_id} (Upd) -> {prop_group_clean}")
            else:
                notion.pages.create(parent={"database_id": NOTION_DB_ID}, properties=props)
                print(f"    ✨ {res_id} (New) -> {prop_group_clean}")
            time.sleep(0.4)
            return
        except Exception as e:
            time.sleep(1)

def executar_sincronizacao():
    # --- AJUSTE AQUI OS ANOS QUE QUER RODAR ---
    anos_para_processar = [2024, 2025, 2026]
    
    print(f"🚀 Iniciando Sincronização para os anos: {anos_para_processar}")
    
    for ano in anos_para_processar:
        print(f"\n📅 --- PROCESSANDO ANO {ano} ---")
        page = 1
        limit = 50 

        while True:
            print(f"📖 Lendo Página {page}...")

            payload = {
                "methodName": "GetReservationsFiltered",
                "params": {
                    "token_key": STREAMLINE_KEY,
                    "token_secret": STREAMLINE_SECRET,
                    "return_full": True,
                    "limit": limit,      
                    "p": page,
                    "modified_since": f"{ano}-01-01 00:00:00"
                }
            }

            try:
                response = requests.post(URL_STREAMLINE, json=payload, timeout=90)
                dados = response.json()

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
                    print(f"✅ Ano {ano} finalizado!")
                    break

                for r in lista_reservas:
                    upsert_reserva(r, ano)
                
                page += 1
                time.sleep(1) 

            except Exception as e:
                print(f"❌ Erro de conexão: {e}")
                time.sleep(5)

    print(f"\n🏁 SINCRONIZAÇÃO COMPLETA!")

if __name__ == "__main__":
    executar_sincronizacao()
