import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis (caso rode localmente)
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

# --- 🕵️‍♂️ FUNÇÃO ESPIÃ DE IP ---
def descobrir_ip():
    print("\n" + "="*40)
    print("🕵️‍♂️ DETECÇÃO DE IP INICIADA...")
    try:
        # Pergunta para um serviço externo qual é o meu IP
        ip = requests.get('https://api.ipify.org').text
        print(f"🌍 O IP DESTE SERVIDOR É:  {ip}")
        print("⚠️  Copie o número acima e cadastre no Streamline!")
    except Exception as e:
        print(f"❌ Não consegui descobrir o IP: {e}")
    print("="*40 + "\n")

# --- FUNÇÕES AUXILIARES ---

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

# --- NOTION ---

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
    
    # Tratamento de dados
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
    
    if page_id:
        requests.patch(f"{URL_NOTION}/pages/{page_id}", json=payload, headers=HEADERS_NOTION)
        # print(f"🔄 Atualizado: {res_id}") # Descomente se quiser ver linha por linha
    else:
        payload["parent"] = {"database_id": NOTION_DATABASE_ID}
        requests.post(f"{URL_NOTION}/pages", json=payload, headers=HEADERS_NOTION)
        print(f"✨ Criado: {res_id}")

# --- EXECUÇÃO PRINCIPAL ---

def executar_sincronizacao():
    # 1. DESCOBRE O IP
    descobrir_ip()
    
    print("🚀 Iniciando Sincronização Streamline -> Notion")
    
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
        
        # Se deu acesso negado, avisa
        if response.status_code in [401, 403]:
            print("❌ ACESSO NEGADO PELO STREAMLINE! (Verifique o IP acima)")
        
        dados = response.json()

        # Verifica se o JSON veio com erro explicito
        if isinstance(dados, dict) and 'error' in dados:
             print(f"❌ Erro retornado pela API: {dados['error']}")

        lista_reservas = []
        if 'data' in dados and 'reservations' in dados['data']:
            lista_reservas = dados['data']['reservations']
        elif 'Response' in dados:
            lista_reservas = dados['Response'].get('data', [])

        print(f"📦 Total de reservas encontradas: {len(lista_reservas)}")

        count = 0
        for r in lista_reservas:
            try:
                upsert_reserva(r)
                count += 1
                time.sleep(0.4) # Respeita limite do Notion
                if count % 20 == 0: print(f"--- Processados {count} ---")
            except Exception as e:
                print(f"⚠️ Pulei reserva {r.get('confirmation_id')} por erro: {e}")
                continue

        print("✅ Sincronização concluída!")

    except Exception as e:
        print(f"❌ Erro fatal ou de conexão: {e}")

if __name__ == "__main__":
    executar_sincronizacao()
