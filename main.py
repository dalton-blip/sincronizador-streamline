import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis
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

# --- FUNÇÕES DE DATA (Blindada contra erros) ---

def parse_dt_robusto(data_str):
    # Se for vazio, None ou zero, retorna None imediatamente
    if not data_str: return None
    
    try:
        data_str = str(data_str).strip()
        if data_str.startswith("0000-00-00"): return None
        
        formatos = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]
        for fmt in formatos:
            try:
                return datetime.strptime(data_str, fmt)
            except ValueError:
                continue
    except Exception:
        return None # Se der qualquer erro bizarro, retorna None
    
    return None

def formatar_iso_date(dt_obj):
    if not dt_obj: return None
    return dt_obj.strftime("%Y-%m-%d")

# --- TRADUTORES ---
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

# --- NOTION ---

def buscar_pagina_notion(res_number):
    url = f"{URL_NOTION}/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "Res #",
            "rich_text": {"equals": str(res_number)}
        }
    }
    try:
        response = requests.post(url, json=payload, headers=HEADERS_NOTION)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                return results[0]["id"]
        else:
            print(f"⚠️ Erro Notion {response.status_code}: {response.text}")
    except Exception as e:
        print(f"⚠️ Erro conexão Notion: {e}")
    return None

def montar_propriedades(r):
    # Tratamento seguro de cada campo
    dt_criacao = parse_dt_robusto(r.get('creation_date'))
    dt_ci = parse_dt_robusto(r.get('startdate') or r.get('start_date'))
    dt_co = parse_dt_robusto(r.get('enddate') or r.get('end_date'))
    
    nome = f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
    res_num = str(r.get('confirmation_id', ''))
    
    status_visual = gerar_status_visual(r.get('type_name', '---'), r.get('status_code'))
    state_binario = obter_estado_binario(r.get('status_code'))
    
    room_name = str(r.get('unit_name', 'Unknown'))
    gst_fmt = f"{r.get('occupants',0)}|{r.get('occupants_small',0)}"
    
    try: total = float(r.get('price_total', 0))
    except: total = 0.0
    
    try: rate = float(r.get('price_nightly', 0))
    except: rate = 0.0
    
    try: nights = int(r.get('days_number', 0))
    except: nights = 0

    props = {
        "Name": {"title": [{"text": {"content": nome}}]},
        "Res #": {"rich_text": [{"text": {"content": res_num}}]},
        "Status": {"select": {"name": status_visual}},
        "State": {"select": {"name": state_binario}},
        "NTS": {"number": nights},
        "GST": {"rich_text": [{"text": {"content": gst_fmt}}]},
        "Room": {"rich_text": [{"text": {"content": room_name}}]},
        "Total": {"number": total},
        "TL Rate": {"number": rate}
    }

    if dt_criacao: props["Created"] = {"date": {"start": formatar_iso_date(dt_criacao)}}
    if dt_ci: props["CI"] = {"date": {"start": formatar_iso_date(dt_ci)}}
    if dt_co: props["CO"] = {"date": {"start": formatar_iso_date(dt_co)}}

    return {"properties": props}

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    if not res_id: return

    page_id = buscar_pagina_notion(res_id)
    payload = montar_propriedades(reserva)

    if page_id:
        # Tenta atualizar
        requests.patch(f"{URL_NOTION}/pages/{page_id}", json=payload, headers=HEADERS_NOTION)
        print(f"🔄 Atualizado: {res_id}")
    else:
        # Tenta criar
        payload["parent"] = {"database_id": NOTION_DATABASE_ID}
        requests.post(f"{URL_NOTION}/pages", json=payload, headers=HEADERS_NOTION)
        print(f"✨ Criado: {res_id}")

# --- EXECUÇÃO ---

def executar_sincronizacao():
    print("🚀 Iniciando Sincronização...")
    
    # Busca histórico desde 2015
    data_historico = "2015-01-01 00:00:00"

    payload = {
        "methodName": "GetReservationsFiltered",
        "params": {
            "token_key": STREAMLINE_KEY,
            "token_secret": STREAMLINE_SECRET,
            "return_full": True, 
            "modified_since": data_historico
        }
    }

    try:
        response = requests.post(URL_STREAMLINE, json=payload, timeout=120)
        dados = response.json()

        lista_reservas = []
        if 'data' in dados and 'reservations' in dados['data']:
            lista_reservas = dados['data']['reservations']
        elif 'Response' in dados:
            lista_reservas = dados['Response'].get('data', [])

        print(f"📦 Total de reservas encontradas no Streamline: {len(lista_reservas)}")

        count = 0
        for r in lista_reservas:
            # TRY/EXCEPT DENTRO DO LOOP (O SEGREDO)
            # Se uma reserva falhar, ele apenas avisa e continua para a próxima
            try:
                upsert_reserva(r)
                count += 1
                time.sleep(0.4) # Respeitar limites do Notion
                
                if count % 10 == 0:
                    print(f"--- Processados {count} ---")
            
            except Exception as e:
                print(f"⚠️ Erro ao processar reserva {r.get('confirmation_id')}: {e}")
                continue

        print("✅ Sincronização concluída!")

    except Exception as e:
        print(f"❌ Erro fatal de conexão: {e}")

if __name__ == "__main__":
    executar_sincronizacao()
