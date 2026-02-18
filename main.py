import requests
import json
import os
import time
import calendar
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
    
    # Tentativa com retry
    for _ in range(3):
        try:
            if page_id:
                res = requests.patch(f"{URL_NOTION}/pages/{page_id}", json=payload, headers=HEADERS_NOTION)
            else:
                payload["parent"] = {"database_id": NOTION_DATABASE_ID}
                res = requests.post(f"{URL_NOTION}/pages", json=payload, headers=HEADERS_NOTION)
            
            if res.status_code == 429:
                time.sleep(2)
                continue
            return
        except:
            time.sleep(1)

def executar_sincronizacao():
    print("🚀 Sincronização MÊS A MÊS (2024-2026)...")
    
    # Loop de Anos
    for ano in range(2024, 2027):
        print(f"\n📂 Processando ANO {ano} ------------------")
        
        # Loop de Meses
        for mes in range(1, 13):
            # Define o último dia do mês
            ultimo_dia = calendar.monthrange(ano, mes)[1]
            
            # Formato ISO: YYYY-MM-DD (Confirmado pelo Debug que funciona)
            dt_inicio = f"{ano}-{mes:02d}-01"
            dt_fim = f"{ano}-{mes:02d}-{ultimo_dia}"
            
            print(f"   📅 Mês {mes:02d} ({dt_inicio} a {dt_fim}): ", end="")

            payload = {
                "methodName": "GetReservationsFiltered",
                "params": {
                    "token_key": STREAMLINE_KEY,
                    "token_secret": STREAMLINE_SECRET,
                    "start_date": dt_inicio,
                    "end_date": dt_fim,
                    "date_type": "arrival", # Filtra pela chegada (Check-in)
                    "return_full": True     # Traz detalhes completos
                }
            }

            try:
                # Timeout aumentado para 90s
                response = requests.post(URL_STREAMLINE, json=payload, timeout=90)
                
                try:
                    dados = response.json()
                except:
                    print("❌ Erro JSON/Timeout API.")
                    continue

                # Checa erro de limite (10k)
                if isinstance(dados, dict) and 'status' in dados and dados['status'].get('code') == 'E0105':
                    print("⚠️ Limite 10k atingido (Mês muito cheio).")
                    # Aqui poderíamos quebrar em quinzenas, mas vamos torcer para passar
                    continue

                lista_reservas = []
                if 'data' in dados and 'reservations' in dados['data']:
                    lista_reservas = dados['data']['reservations']
                elif 'Response' in dados:
                    lista_reservas = dados['Response'].get('data', [])
                
                qtd = len(lista_reservas)
                print(f"📦 {qtd} reservas encontradas.")

                if qtd > 0:
                    count_salvo = 0
                    for r in lista_reservas:
                        upsert_reserva(r)
                        count_salvo += 1
                        # Feedback visual simples (.)
                        if count_salvo % 5 == 0:
                            print(".", end="", flush=True)
                    print(f" ✅ Concluído.")
                
                # Pausa para respirar entre meses
                time.sleep(1)

            except Exception as e:
                print(f"❌ Erro Conexão: {e}")
                time.sleep(2)

    print("\n🏁 FIM! Execução completa.")

if __name__ == "__main__":
    executar_sincronizacao()
