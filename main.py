import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env (para testes locais)
load_dotenv()

# --- CONFIGURAÇÕES ---
# Substitua pelas suas chaves reais ou use variáveis de ambiente
STREAMLINE_KEY = os.getenv("STREAMLINE_KEY", "SUA_KEY_AQUI") 
STREAMLINE_SECRET = os.getenv("STREAMLINE_SECRET", "SUA_SECRET_AQUI")
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "SEU_TOKEN_NOTION_AQUI")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "SEU_DATABASE_ID_AQUI")

URL_STREAMLINE = "https://web.streamlinevrs.com/api/json"
URL_NOTION = "https://api.notion.com/v1"

HEADERS_NOTION = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- FUNÇÕES DE DATA E FORMATAÇÃO ---

def parse_dt_robusto(data_str):
    if not data_str or str(data_str).startswith("0000-00-00"): return None
    data_str = str(data_str).strip()
    formatos = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]
    for fmt in formatos:
        try:
            return datetime.strptime(data_str, fmt)
        except ValueError:
            continue
    return None

def formatar_iso_date(dt_obj):
    # Notion precisa de YYYY-MM-DD
    if not dt_obj: return None
    return dt_obj.strftime("%Y-%m-%d")

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

# --- FUNÇÕES DO NOTION (CÉREBRO DA INTEGRAÇÃO) ---

def buscar_pagina_notion(res_number):
    """
    Procura no Notion se já existe uma página com esse 'Res #'
    Retorna o ID da página se achar, ou None.
    """
    url = f"{URL_NOTION}/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "Res #",
            "rich_text": {
                "equals": str(res_number)
            }
        }
    }
    try:
        response = requests.post(url, json=payload, headers=HEADERS_NOTION)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                return results[0]["id"]
    except Exception as e:
        print(f"⚠️ Erro ao buscar no Notion: {e}")
    return None

def montar_propriedades(r):
    """Mapeia os dados do Streamline para as colunas do Notion"""
    
    # Tratamento de dados
    dt_criacao = parse_dt_robusto(r.get('creation_date'))
    dt_ci = parse_dt_robusto(r.get('startdate') or r.get('start_date'))
    dt_co = parse_dt_robusto(r.get('enddate') or r.get('end_date'))
    
    nome = f"{r.get('first_name', '')} {r.get('last_name', '')}".strip()
    res_num = str(r.get('confirmation_id', ''))
    
    status_visual = gerar_status_visual(r.get('type_name', '---'), r.get('status_code'))
    state_binario = obter_estado_binario(r.get('status_code'))
    
    room_name = r.get('unit_name', 'Unknown')
    gst_fmt = f"{r.get('occupants',0)}|{r.get('occupants_small',0)}"
    
    # Tratamento de valores numéricos
    try:
        total = float(r.get('price_total', 0))
    except: total = 0.0
    
    try:
        rate = float(r.get('price_nightly', 0))
    except: rate = 0.0
    
    try:
        nights = int(r.get('days_number', 0))
    except: nights = 0

    # Montagem do JSON do Notion
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

    # Datas (só adiciona se existirem para não dar erro)
    if dt_criacao:
        props["Created"] = {"date": {"start": formatar_iso_date(dt_criacao)}}
    if dt_ci:
        props["CI"] = {"date": {"start": formatar_iso_date(dt_ci)}}
    if dt_co:
        props["CO"] = {"date": {"start": formatar_iso_date(dt_co)}}

    return {"properties": props}

def upsert_reserva(reserva):
    res_id = str(reserva.get('confirmation_id'))
    if not res_id: return

    # 1. Verifica se existe
    page_id = buscar_pagina_notion(res_id)
    
    # 2. Monta os dados
    payload = montar_propriedades(reserva)

    if page_id:
        # ATUALIZAÇÃO (PATCH)
        print(f"🔄 Atualizando: {res_id} ...")
        requests.patch(f"{URL_NOTION}/pages/{page_id}", json=payload, headers=HEADERS_NOTION)
    else:
        # CRIAÇÃO (POST)
        print(f"✨ Criando: {res_id} ...")
        payload["parent"] = {"database_id": NOTION_DATABASE_ID}
        requests.post(f"{URL_NOTION}/pages", json=payload, headers=HEADERS_NOTION)

# --- FLUXO PRINCIPAL ---

def executar_sincronizacao():
    print("🚀 Iniciando Sincronização Streamline -> Notion")
    
    # Data bem antiga para pegar TODO o histórico
    # O Streamline vai retornar tudo que foi CRIADO ou MODIFICADO depois dessa data
    data_inicio_historico = "2015-01-01 00:00:00"

    payload = {
        "methodName": "GetReservationsFiltered",
        "params": {
            "token_key": STREAMLINE_KEY,
            "token_secret": STREAMLINE_SECRET,
            "return_full": True, 
            "modified_since": data_inicio_historico
        }
    }

    try:
        response = requests.post(URL_STREAMLINE, json=payload, timeout=120) # Timeout maior pois a lista é grande
        dados = response.json()

        lista_reservas = []
        if 'data' in dados and 'reservations' in dados['data']:
            lista_reservas = dados['data']['reservations']
        elif 'Response' in dados:
            lista_reservas = dados['Response'].get('data', [])

        print(f"📦 Total de reservas encontradas no Streamline: {len(lista_reservas)}")

        # Processa uma por uma
        count = 0
        for r in lista_reservas:
            upsert_reserva(r)
            count += 1
            
            # Rate Limit Protection: Notion permite média de 3 requests/segundo
            # Dormir um pouco a cada requisição evita erro 429
            time.sleep(0.4) 
            
            if count % 10 == 0:
                print(f"--- Processados {count} de {len(lista_reservas)} ---")

        print("✅ Sincronização concluída com sucesso!")

    except Exception as e:
        print(f"❌ Erro fatal: {e}")

if __name__ == "__main__":
    executar_sincronizacao()
