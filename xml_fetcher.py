import requests, xmltodict, json, os, re
from datetime import datetime
from unidecode import unidecode

XML_URL = os.getenv("XML_URL")
JSON_FILE = "data.json"

# =================== MAPS =======================
MAPEAMENTO_CATEGORIAS = {
    # (cole todo o conteúdo do seu mapeamento aqui)
}

MAPEAMENTO_CILINDRADAS = {
    # (cole todo o conteúdo do seu mapeamento aqui)
}

# =================== UTILS =======================

def normalizar_modelo(modelo):
    if not modelo:
        return ""
    modelo_norm = unidecode(modelo).lower()
    modelo_norm = re.sub(r'[^a-z0-9]', '', modelo_norm)
    return modelo_norm

def inferir_categoria(modelo):
    if not modelo:
        return None
    modelo_norm = normalizar_modelo(modelo)
    for mapeado, categoria in MAPEAMENTO_CATEGORIAS.items():
        mapeado_norm = normalizar_modelo(mapeado)
        if mapeado_norm in modelo_norm:
            return categoria
    return None

def inferir_cilindrada(modelo):
    if not modelo:
        return None
    modelo_norm = normalizar_modelo(modelo)
    for mapeado, cilindrada in MAPEAMENTO_CILINDRADAS.items():
        mapeado_norm = normalizar_modelo(mapeado)
        if mapeado_norm in modelo_norm:
            return cilindrada
    return None

def converter_preco_xml(valor_str):
    if not valor_str:
        return None
    try:
        valor = str(valor_str).replace("R$", "").replace(".", "").replace(",", ".").strip()
        return float(valor)
    except ValueError:
        return None

def extrair_fotos(v):
    fotos = v.get("fotos")
    if not fotos:
        return []
    fotos_foto = fotos.get("foto")
    if not fotos_foto:
        return []
    if isinstance(fotos_foto, dict):
        fotos_foto = [fotos_foto]
    return [
        img["url"].split("?")[0]
        for img in fotos_foto
        if isinstance(img, dict) and "url" in img
    ]

# =================== MAIN =======================

def fetch_and_convert_xml():
    try:
        if not XML_URL:
            raise ValueError("Variável XML_URL não definida")

        response = requests.get(XML_URL)
        data_dict = xmltodict.parse(response.content)

        parsed_vehicles = []

        for v in data_dict["estoque"]["veiculo"]:
            try:
                parsed = {
                    "id": v.get("idveiculo"),
                    "tipo": v.get("tipoveiculo"),
                    "marca": v.get("marca"),
                    "modelo": v.get("modelo"),
                    "categoria": inferir_categoria(v.get("modelo")),
                    "cilindrada": inferir_cilindrada(v.get("modelo")),
                    "ano": v.get("anomodelo"),
                    "km": v.get("quilometragem"),
                    "cor": v.get("cor"),
                    "combustivel": v.get("combustivel"),
                    "cambio": v.get("cambio"),
                    "portas": v.get("numeroportas"),
                    "tipoveiculo": v.get("tipoveiculo"),
                    "preco": converter_preco_xml(v.get("preco")),
                    "opcionais": v.get("opcionais").get("opcional") if v.get("opcionais") else None,
                    "fotos": extrair_fotos(v)
                }
                parsed_vehicles.append(parsed)
            except Exception as e:
                print(f"[ERRO ao converter veículo ID {v.get('idveiculo')}] {e}")

        data_dict = {
            "veiculos": parsed_vehicles,
            "_updated_at": datetime.now().isoformat()
        }

        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(data_dict, f, ensure_ascii=False, indent=2)

        print("[OK] Dados atualizados com sucesso.")
        return data_dict

    except Exception as e:
        print(f"[ERRO] Falha ao converter XML: {e}")
        return {}
