# -*- coding: utf-8 -*-
"""
Cancela no Omie os títulos (contas a pagar) de cards do Pipefy já cancelados.

Roda no PC, fora do sistema web. Fluxo:

  1. Lê a planilha com os números dos cards (uma coluna, um por linha).
  2. Para cada card, consulta no Omie a conta a pagar de código de integração
     "Int" + número do card (o mesmo que o processarnovasp grava na criação).
  3. Classifica: não existe / já cancelado / pago ou com baixa / em aberto.
  4. Mostra o resumo e pede confirmação ("SIM") antes de cancelar qualquer coisa.
  5. Cancela só os que estão EM ABERTO e sem nenhum pagamento, e confere
     consultando de novo.
  6. Grava o relatório em Excel na pasta "relatorios".

Nada é excluído: só consulta e cancelamento.

Cuidado com o Omie: ele bloqueia quem consulta demais. Por isso há uma pausa
fixa entre as chamadas, e quando o Omie manda esperar ("tente novamente em N
segundos", HTTP 425/429, "consumo redundante") o programa espera o tempo pedido
— com contagem regressiva na tela — antes de tentar de novo a MESMA consulta.
Erro definitivo (título não existe, período contábil fechado) não é repetido.
"""
import json
import os
import re
import sys
import time
from datetime import datetime

import requests

PASTA = os.path.dirname(os.path.abspath(__file__))
URL_CONTAPAGAR = "https://app.omie.com.br/api/v1/financas/contapagar/"

# Situações do Omie em que o título está em aberto. SÓ nelas há cancelamento.
# Qualquer outra (PAGO, PARCIAL, LIQUIDADO, uma que não conhecemos...) fica de
# fora e vai para o relatório como "revisar" — na dúvida, não se mexe.
STATUS_EM_ABERTO = {"A VENCER", "ATRASADO", "VENCE HOJE", "A PAGAR",
                    "ABERTO", "EM ABERTO"}
STATUS_CANCELADO = {"CANCELADO", "CANCELADA"}

# Campos que, preenchidos com valor maior que zero, indicam pagamento.
CAMPOS_DE_PAGAMENTO = ("valor_pago", "valor_baixado", "valor_recebido",
                       "nValorPago", "valor_pagamento")

# Frases do Omie que dizem "isso não vai mudar repetindo".
FRASES_NAO_EXISTE = ("nao cadastrad", "não cadastrad", "nao encontrad",
                     "não encontrad", "nao existe", "não existe", "inexistente")
FRASES_DEFINITIVAS = FRASES_NAO_EXISTE + (
    "periodo contabil", "período contábil", "bloqueado por integra",
    "nao e permitido", "não é permitido", "nao pode ser", "não pode ser",
)
FRASES_DE_LIMITE = ("consumo indevido", "consumo redundante", "bloqueado por excesso",
                    "numero de requisicoes", "número de requisições", "excedeu",
                    "tente novamente", "try again", "aguarde")
SEGUNDOS_PEDIDOS = re.compile(
    r"(?:aguarde|tente\s+novamente\s+em|try\s+again\s+in)\s+(\d+)\s*seg", re.I)


# ---------------------------------------------------------------------------
# Configuração (.env)
# ---------------------------------------------------------------------------
def carregar_env(caminho):
    """Lê CHAVE=VALOR do .env para o ambiente. Aceita aspas, BOM e comentários."""
    if not os.path.exists(caminho):
        return
    with open(caminho, "r", encoding="utf-8-sig") as f:
        for linha in f:
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, _, valor = linha.partition("=")
            os.environ[chave.strip()] = valor.strip().strip('"').strip("'").strip()


def _sem_espacos(texto):
    """Credencial colada do portal costuma vir com espaço invisível grudado."""
    invisiveis = {0x200b, 0x200c, 0x200d, 0x2060, 0xfeff}
    return "".join(c for c in (texto or "") if not (c.isspace() or ord(c) in invisiveis))


def _numero(nome, padrao):
    try:
        return float(os.environ.get(nome, "").replace(",", ".") or padrao)
    except ValueError:
        return float(padrao)


# ---------------------------------------------------------------------------
# Planilha de entrada
# ---------------------------------------------------------------------------
def normalizar_id(valor):
    """'12345', '12345.0', 'Int12345', ' INT 12345 ' -> '12345'. Vazio se não for número."""
    if valor is None:
        return ""
    if isinstance(valor, float) and valor.is_integer():
        valor = int(valor)
    texto = str(valor).strip()
    texto = re.sub(r"^int\s*", "", texto, flags=re.I)
    texto = re.sub(r"\.0+$", "", texto)
    return texto if texto.isdigit() else ""


def ler_ids(caminho):
    """Lê os números dos cards: 1ª coluna da planilha .xlsx, ou 1º pedaço de cada
    linha de um .txt/.csv (lista colada).

    Linhas com texto (o cabeçalho, por exemplo) são ignoradas. Número repetido
    entra uma vez só — consultar duas vezes o mesmo título é exatamente o que o
    Omie pune."""
    ext = os.path.splitext(caminho)[1].lower()
    valores = []
    if ext in (".xlsx", ".xlsm"):
        from openpyxl import load_workbook
        wb = load_workbook(caminho, read_only=True, data_only=True)
        ws = wb.worksheets[0]
        for linha in ws.iter_rows(min_col=1, max_col=1, values_only=True):
            valores.append(linha[0] if linha else None)
        wb.close()
    elif ext in (".csv", ".txt"):
        # Lista colada (do Excel, do Pipefy, de onde for): vale o primeiro pedaço
        # de cada linha, separado por tab, ponto e vírgula, vírgula ou espaço.
        with open(caminho, "r", encoding="utf-8-sig", errors="replace") as f:
            for linha in f:
                pedacos = [x for x in re.split(r"[\t;,]", linha.strip()) if x.strip()]
                valores.append(pedacos[0].strip() if pedacos else None)
    else:
        raise ValueError(f"Formato de planilha não reconhecido: {ext}. Use .xlsx ou .csv.")

    vistos, ids, ignorados = set(), [], []
    for v in valores:
        card = normalizar_id(v)
        if not card:
            if v not in (None, ""):
                ignorados.append(str(v))
            continue
        if card not in vistos:
            vistos.add(card)
            ids.append(card)
    return ids, ignorados


def achar_planilhas(argumento):
    """De onde vem a lista, nesta ordem:
      1. o arquivo arrastado em cima do executar.bat;
      2. o ARQUIVO_PLANILHA do .env;
      3. TODOS os .xlsx/.csv/.txt da pasta 'entrada' (juntos, sem repetir card).
    Devolve (lista de arquivos, se_eles_saem_da_entrada_no_fim)."""
    if argumento and os.path.exists(argumento):
        return [argumento], False
    configurado = os.environ.get("ARQUIVO_PLANILHA", "").strip()
    if configurado:
        caminho = configurado if os.path.isabs(configurado) else os.path.join(PASTA, configurado)
        if os.path.exists(caminho):
            return [caminho], False
    pasta_entrada = os.path.join(PASTA, "entrada")
    if not os.path.isdir(pasta_entrada):
        return [], False
    arquivos = sorted(
        os.path.join(pasta_entrada, n) for n in os.listdir(pasta_entrada)
        if n.lower().endswith((".xlsx", ".csv", ".txt"))
        and not n.startswith("~$") and n.upper() != "LEIA-ME.TXT"
        and os.path.isfile(os.path.join(pasta_entrada, n)))
    return arquivos, True


# Resultados que pedem nova tentativa. O resto está RESOLVIDO para o programa:
# cancelado, já cancelado, não existe — ou depende de gente (pago, revisar).
RESULTADOS_PENDENTES = ("NÃO PROCESSADO", "FALHOU")


def fechar_entrada(linhas, arquivos, mover, simulacao):
    """Deixa na pasta 'entrada' SÓ o que precisa ser tentado de novo.

    Os arquivos lidos vão para 'entrada/processados' (com data na frente), e os
    cards pendentes viram 'entrada/pendentes_<data>.txt'. Assim, rodar de novo
    consulta só os pendentes — o que foi resolvido não volta.
    Em simulação nada se move: nada foi cancelado, a lista continua valendo."""
    carimbo = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    pendentes = [l["Card Pipefy"] for l in linhas if l["Resultado"] in RESULTADOS_PENDENTES]
    if simulacao:
        return None, pendentes
    pasta_entrada = os.path.join(PASTA, "entrada")
    os.makedirs(pasta_entrada, exist_ok=True)
    if mover:
        destino = os.path.join(pasta_entrada, "processados")
        os.makedirs(destino, exist_ok=True)
        for arq in arquivos:
            try:
                os.replace(arq, os.path.join(destino, f"{carimbo}_{os.path.basename(arq)}"))
            except OSError as e:   # planilha aberta no Excel, por exemplo
                print(f"   (!) Não consegui mover {os.path.basename(arq)} ({e}). "
                      "Tire-o da pasta 'entrada' à mão, senão ele será lido de novo.")
    caminho = None
    if pendentes:
        caminho = os.path.join(pasta_entrada, f"pendentes_{carimbo}.txt")
        with open(caminho, "w", encoding="utf-8", newline="\r\n") as f:
            f.write("\n".join(pendentes) + "\n")
    return caminho, pendentes


# ---------------------------------------------------------------------------
# Omie
# ---------------------------------------------------------------------------
class ErroOmie(Exception):
    def __init__(self, texto, nao_existe=False):
        super().__init__(texto)
        self.texto = texto
        self.nao_existe = nao_existe


def _contem(texto, frases):
    t = (texto or "").lower()
    return any(f in t for f in frases)


def esperar(segundos, motivo):
    """Espera mostrando a contagem — para quem olha a tela saber que não travou."""
    fim = time.monotonic() + segundos
    while True:
        falta = int(fim - time.monotonic())
        if falta <= 0:
            break
        print(f"\r   ... {motivo}: aguardando {falta:>4} s ...   ", end="", flush=True)
        time.sleep(min(1, falta))
    print("\r" + " " * 70 + "\r", end="", flush=True)


class Omie:
    def __init__(self, app_key, app_secret, pausa=1.5, max_tentativas=6, teto_espera=3600):
        self.app_key = app_key
        self.app_secret = app_secret
        self.pausa = pausa
        self.max_tentativas = max_tentativas
        self.teto_espera = teto_espera
        self.sessao = requests.Session()
        self.sessao.headers.update({"Content-Type": "application/json"})
        self._ultima = 0.0
        self.chamadas = 0

    def _respeitar_pausa(self):
        falta = self.pausa - (time.monotonic() - self._ultima)
        if falta > 0:
            time.sleep(falta)
        self._ultima = time.monotonic()

    def chamar(self, call, param):
        corpo = json.dumps({"call": call, "app_key": self.app_key,
                            "app_secret": self.app_secret, "param": [param]})
        ultimo = ""
        for tentativa in range(1, self.max_tentativas + 1):
            self._respeitar_pausa()
            self.chamadas += 1
            try:
                resp = self.sessao.post(URL_CONTAPAGAR, data=corpo, timeout=90)
            except requests.RequestException as e:
                ultimo = f"falha de rede: {e}"
                esperar(min(10 * tentativa, 120), "a internet falhou")
                continue

            try:
                dados = resp.json()
            except ValueError:
                dados = {}
            falha = ""
            if isinstance(dados, dict):
                falha = str(dados.get("faultstring") or "")
            if resp.status_code == 200 and not falha:
                return dados

            texto = falha or (resp.text or "")[:300] or f"HTTP {resp.status_code}"
            ultimo = texto
            # Definitivo: não adianta repetir (e repetir é o que o Omie pune).
            if _contem(texto, FRASES_DEFINITIVAS):
                raise ErroOmie(texto, nao_existe=_contem(texto, FRASES_NAO_EXISTE))

            limite = resp.status_code in (425, 429) or _contem(texto, FRASES_DE_LIMITE)
            if limite:
                m = SEGUNDOS_PEDIDOS.search(texto)
                if m:
                    espera = int(m.group(1)) + 5
                elif "redundante" in texto.lower():
                    espera = 65   # o Omie compara com a mesma chamada do último minuto
                else:
                    espera = min(30 * tentativa, 300)
                if espera > self.teto_espera:
                    raise ErroOmie(f"O Omie pediu {espera} s de espera, acima do teto "
                                   f"configurado ({int(self.teto_espera)} s): {texto}")
                print(f"\n   (!) Omie pediu pausa: {texto[:150]}")
                esperar(espera, "o Omie pediu para esperar")
                continue

            if resp.status_code >= 500:
                # 500 do Omie costuma ser passageiro.
                esperar(min(15 * tentativa, 180), f"o Omie respondeu erro {resp.status_code}")
                continue

            # 4xx ou recusa de regra de negócio: não repete.
            raise ErroOmie(texto)
        raise ErroOmie(f"Desisti depois de {self.max_tentativas} tentativas. Último erro: {ultimo}")

    def consultar(self, codigo_integracao):
        return self.chamar("ConsultarContaPagar",
                           {"codigo_lancamento_integracao": codigo_integracao})

    def cancelar(self, codigo_omie, metodo):
        return self.chamar(metodo, {"codigo_lancamento_omie": int(codigo_omie)})


# ---------------------------------------------------------------------------
# Regra
# ---------------------------------------------------------------------------
def _valor(v):
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def classificar(titulo):
    """Diz o que fazer com o título consultado. Devolve (situação, pode_cancelar, motivo)."""
    status = str(titulo.get("status_titulo") or "").strip().upper()
    if status in STATUS_CANCELADO:
        return "JÁ CANCELADO", False, "O título já estava cancelado no Omie."
    pago = [c for c in CAMPOS_DE_PAGAMENTO if _valor(titulo.get(c)) > 0]
    if pago:
        return "COM PAGAMENTO", False, (
            f"Situação {status or '?'} com pagamento registrado ({', '.join(pago)}). "
            "Não cancelado — precisa ser revisto à mão.")
    if status in STATUS_EM_ABERTO:
        return "EM ABERTO", True, f"Situação {status}, sem pagamento."
    if status in ("PAGO", "LIQUIDADO", "RECEBIDO"):
        return "PAGO", False, f"Situação {status}. Título pago não se cancela."
    if "PARCIAL" in status:
        return "COM PAGAMENTO", False, f"Situação {status}: há baixa parcial. Não cancelado."
    return "REVISAR", False, (
        f"Situação '{status or 'vazia'}' não reconhecida. Por segurança, não foi cancelado.")


# ---------------------------------------------------------------------------
# Relatório
# ---------------------------------------------------------------------------
COLUNAS = ["Card Pipefy", "Código de integração", "Código Omie", "Fornecedor",
           "Valor", "Vencimento", "Situação no Omie (antes)", "Resultado", "Detalhe"]


def gravar_relatorio(linhas, simulacao):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    pasta = os.path.join(PASTA, "relatorios")
    os.makedirs(pasta, exist_ok=True)
    carimbo = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    nome = f"relatorio_{'SIMULACAO_' if simulacao else ''}{carimbo}.xlsx"
    caminho = os.path.join(pasta, nome)

    wb = Workbook()
    ws = wb.active
    ws.title = "Detalhe"
    ws.append(COLUNAS)
    for c in ws[1]:
        c.font = Font(bold=True)
    cores = {"CANCELADO AGORA": "C6EFCE", "FALHOU": "FFC7CE", "REVISAR": "FFEB9C",
             "PAGO / COM BAIXA": "FFEB9C", "NÃO PROCESSADO": "FFC7CE"}
    for l in linhas:
        ws.append([l.get(k, "") for k in COLUNAS])
        cor = cores.get(l["Resultado"])
        if cor:
            for c in ws[ws.max_row]:
                c.fill = PatternFill("solid", fgColor=cor)
    for col, larg in zip("ABCDEFGHI", (13, 20, 16, 35, 12, 12, 22, 22, 80)):
        ws.column_dimensions[col].width = larg
    ws.freeze_panes = "A2"

    resumo = wb.create_sheet("Resumo", 0)
    resumo.append(["Execução", datetime.now().strftime("%d/%m/%Y %H:%M:%S")])
    resumo.append(["Modo", "SIMULAÇÃO (nada foi cancelado)" if simulacao else "CANCELAMENTO REAL"])
    resumo.append(["Cards na planilha", len(linhas)])
    resumo.append([])
    resumo.append(["Resultado", "Quantidade"])
    for c in resumo[resumo.max_row]:
        c.font = Font(bold=True)
    contagem = {}
    for l in linhas:
        contagem[l["Resultado"]] = contagem.get(l["Resultado"], 0) + 1
    for k in sorted(contagem):
        resumo.append([k, contagem[k]])
    resumo.column_dimensions["A"].width = 30
    resumo.column_dimensions["B"].width = 34
    wb.save(caminho)
    return caminho, contagem


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------
# Guardado fora do main() para o relatório sair mesmo se a execução for interrompida.
_ESTADO = {"linhas": [], "simular": True, "arquivos": [], "mover": False}

def main():
    carregar_env(os.path.join(PASTA, ".env"))
    print("=" * 70)
    print(" CANCELAMENTO NO OMIE DE TÍTULOS DE CARDS CANCELADOS NO PIPEFY")
    print("=" * 70)

    key = _sem_espacos(os.environ.get("OMIE_KEY") or os.environ.get("OMIE_APP_KEY"))
    secret = _sem_espacos(os.environ.get("OMIE_SECRET") or os.environ.get("OMIE_APP_SECRET"))
    if not key or not secret:
        print("\nERRO: Faltam OMIE_KEY e/ou OMIE_SECRET no arquivo .env desta pasta.")
        return 1

    prefixo = os.environ.get("PREFIXO_INTEGRACAO", "Int") or "Int"
    metodo = os.environ.get("OMIE_METODO_CANCELAR", "CancelarContaPagar").strip() or "CancelarContaPagar"
    simular = os.environ.get("SIMULAR", "").strip().lower() in ("1", "sim", "s", "true", "yes")

    arquivos, mover = achar_planilhas(sys.argv[1] if len(sys.argv) > 1 else None)
    if not arquivos:
        print("\nERRO: Não achei a lista. Cole os números dos cards num arquivo .txt (ou"
              " ponha a planilha .xlsx/.csv) na pasta 'entrada', ou arraste o arquivo"
              " em cima do executar.bat.")
        return 1
    ids, ignorados = [], []
    for arq in arquivos:
        lidos, ign = ler_ids(arq)
        print(f"\nArquivo: {arq}  ->  {len(lidos)} card(s)")
        ignorados += ign
        ids += [c for c in lidos if c not in ids]
    _ESTADO["arquivos"], _ESTADO["mover"], _ESTADO["ids"] = arquivos, mover, ids
    print(f"Cards para consultar: {len(ids)}" + (f"  (linhas ignoradas: {len(ignorados)})" if ignorados else ""))
    if not ids:
        print("ERRO: Nenhum número de card encontrado.")
        return 1

    omie = Omie(key, secret,
                pausa=_numero("PAUSA_ENTRE_CHAMADAS", 1.5),
                teto_espera=_numero("TETO_ESPERA_SEGUNDOS", 3600))

    # ---------- 1. Consulta ----------
    print(f"\n[1/3] Consultando {len(ids)} título(s) no Omie (pausa de {omie.pausa:g} s entre consultas)...")
    linhas, titulos = _ESTADO["linhas"], {}
    for n, card in enumerate(ids, 1):
        codigo = f"{prefixo}{card}"
        l = {"Card Pipefy": card, "Código de integração": codigo, "Código Omie": "",
             "Fornecedor": "", "Valor": "", "Vencimento": "",
             "Situação no Omie (antes)": "", "Resultado": "", "Detalhe": ""}
        linhas.append(l)
        print(f"  {n:>4}/{len(ids)}  {codigo:<16}", end=" ", flush=True)
        try:
            t = omie.consultar(codigo)
        except ErroOmie as e:
            if e.nao_existe:
                l["Resultado"] = "NÃO EXISTE NO OMIE"
                l["Detalhe"] = "Título não encontrado (provavelmente excluído). " + e.texto[:200]
            else:
                l["Resultado"] = "NÃO PROCESSADO"
                l["Detalhe"] = "Erro na consulta: " + e.texto[:300]
            print("→", l["Resultado"])
            continue
        l["Código Omie"] = t.get("codigo_lancamento_omie", "")
        l["Fornecedor"] = t.get("nome_fornecedor") or t.get("codigo_cliente_fornecedor", "")
        l["Valor"] = t.get("valor_documento", "")
        l["Vencimento"] = t.get("data_vencimento", "")
        l["Situação no Omie (antes)"] = str(t.get("status_titulo") or "")
        situacao, pode, motivo = classificar(t)
        l["Detalhe"] = motivo
        if pode:
            l["Resultado"] = "A CANCELAR"
            titulos[card] = t
        elif situacao == "JÁ CANCELADO":
            l["Resultado"] = "JÁ ESTAVA CANCELADO"
        elif situacao in ("PAGO", "COM PAGAMENTO"):
            l["Resultado"] = "PAGO / COM BAIXA"
        else:
            l["Resultado"] = "REVISAR"
        print("→", l["Resultado"], f"({l['Situação no Omie (antes)']})")

    a_cancelar = [l for l in linhas if l["Resultado"] == "A CANCELAR"]
    print(f"\nResumo da consulta: {len(a_cancelar)} em aberto para cancelar, "
          f"{sum(l['Resultado'] == 'JÁ ESTAVA CANCELADO' for l in linhas)} já cancelados, "
          f"{sum(l['Resultado'] == 'NÃO EXISTE NO OMIE' for l in linhas)} não existem, "
          f"{sum(l['Resultado'] in ('PAGO / COM BAIXA', 'REVISAR', 'NÃO PROCESSADO') for l in linhas)} para revisar.")

    # ---------- 2. Confirmação ----------
    if a_cancelar and not simular:
        print(f"\n[2/3] Vou CANCELAR {len(a_cancelar)} título(s) no Omie. Isso não se desfaz por aqui.")
        resposta = input("      Digite SIM para cancelar, ou só Enter para sair sem cancelar: ").strip().upper()
        simular = resposta != "SIM"
    _ESTADO["simular"] = simular
    if simular:
        for l in a_cancelar:
            l["Resultado"] = "SERIA CANCELADO (simulação)"
        print("\n[2/3] Simulação: nada foi cancelado.")
    else:
        # ---------- 3. Cancelamento ----------
        print(f"\n[3/3] Cancelando {len(a_cancelar)} título(s)...")
        mesmo_erro, ultimo_erro, parar = 0, "", ""
        for n, l in enumerate(a_cancelar, 1):
            print(f"  {n:>4}/{len(a_cancelar)}  {l['Código de integração']:<16}", end=" ", flush=True)
            if parar:
                l["Resultado"] = "NÃO PROCESSADO"
                l["Detalhe"] = "Cancelamentos interrompidos: " + parar
                print("→ pulado")
                continue
            try:
                omie.cancelar(l["Código Omie"], metodo)
            except ErroOmie as e:
                # Pode ter cancelado mesmo assim (resposta perdida na rede): confere.
                try:
                    depois = omie.consultar(l["Código de integração"])
                    if str(depois.get("status_titulo") or "").upper() in STATUS_CANCELADO:
                        l["Resultado"] = "CANCELADO AGORA"
                        l["Detalhe"] = "Cancelado (confirmado na consulta seguinte)."
                        print("→ CANCELADO")
                        mesmo_erro = 0
                        continue
                except ErroOmie:
                    pass
                l["Resultado"] = "FALHOU"
                l["Detalhe"] = "O Omie recusou o cancelamento: " + e.texto[:300]
                print("→ FALHOU:", e.texto[:120])
                mesmo_erro = mesmo_erro + 1 if e.texto == ultimo_erro else 1
                ultimo_erro = e.texto
                if _contem(e.texto, ("method", "metodo", "método", "soap")) or mesmo_erro >= 3:
                    parar = ("o Omie recusou várias vezes com o mesmo erro, ou não reconheceu o "
                             f"comando '{metodo}'. Último erro: {e.texto[:200]}")
                continue
            # Confere
            try:
                depois = omie.consultar(l["Código de integração"])
                status = str(depois.get("status_titulo") or "").upper()
            except ErroOmie as e:
                status = f"(não deu para conferir: {e.texto[:100]})"
            if status in STATUS_CANCELADO:
                l["Resultado"] = "CANCELADO AGORA"
                l["Detalhe"] = "Cancelado e conferido no Omie."
            else:
                l["Resultado"] = "FALHOU"
                l["Detalhe"] = (f"O Omie aceitou o pedido, mas a consulta seguinte mostra "
                                f"situação {status or 'vazia'}. Conferir à mão.")
            mesmo_erro = 0
            print("→", l["Resultado"])

    caminho, contagem = gravar_relatorio(linhas, simular)
    print("\n" + "=" * 70)
    print(" RESULTADO FINAL" + ("  (SIMULAÇÃO)" if simular else ""))
    print("=" * 70)
    for k in sorted(contagem):
        print(f"  {k:<32} {contagem[k]:>5}")
    print(f"\n  Chamadas ao Omie: {omie.chamadas}")
    print(f"  Relatório: {caminho}")
    _avisar_pendentes(*fechar_entrada(linhas, arquivos, mover, simular), simular)
    return 0


def _avisar_pendentes(caminho, pendentes, simulacao):
    if simulacao:
        print("\n  Simulação: a lista da pasta 'entrada' ficou como estava.")
    elif pendentes:
        print(f"\n  {len(pendentes)} card(s) para tentar de novo, em: {caminho}")
        print("  Rodando o executar.bat de novo, só eles serão consultados.")
    else:
        print("\n  Nenhum pendente: tudo resolvido. A pasta 'entrada' ficou vazia.")


if __name__ == "__main__":
    try:
        codigo = main()
    except KeyboardInterrupt:
        print("\n\nInterrompido pelo usuário. Os cancelamentos feitos até aqui valem;"
              " rode de novo — os já cancelados aparecem como 'já estava cancelado'.")
        for l in _ESTADO["linhas"]:
            if l["Resultado"] in ("", "A CANCELAR"):
                l["Resultado"] = "NÃO PROCESSADO"
                l["Detalhe"] = (l["Detalhe"] + " | Execução interrompida antes deste.").strip(" |")
        if _ESTADO["linhas"]:
            # Card que nem chegou a ser consultado também é pendente.
            vistos = {l["Card Pipefy"] for l in _ESTADO["linhas"]}
            _ESTADO["linhas"] += [{"Card Pipefy": c, "Resultado": "NÃO PROCESSADO",
                                   "Detalhe": "Execução interrompida antes deste."}
                                  for c in _ESTADO.get("ids", []) if c not in vistos]
            print("Relatório parcial:", gravar_relatorio(_ESTADO["linhas"], _ESTADO["simular"])[0])
            _avisar_pendentes(*fechar_entrada(_ESTADO["linhas"], _ESTADO["arquivos"],
                                              _ESTADO["mover"], _ESTADO["simular"]),
                              _ESTADO["simular"])
        codigo = 1
    sys.exit(codigo)
