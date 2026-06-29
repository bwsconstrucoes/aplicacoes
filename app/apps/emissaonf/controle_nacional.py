# -*- coding: utf-8 -*-
"""
Controle do fluxo nacional: guarda numa aba da planilha as notas que já foram
emitidas e estão AGUARDANDO o nacional (DANFSe), e o último NSU lido do ADN.

Aba "Controle Nacional" (na planilha Notas BWS / ID_PROC). Colunas:
  A numero | B card_id | C cod_verif | D obra | E med | F ano | G toma_cnpj
  H vServ | I dCompet(AAAA-MM) | J xml_abrasf_file_id | K status | L link_nac
  M link_mun | N chave
Célula P1 = último NSU lido do ADN.
"""
from __future__ import annotations

ABA_CONTROLE = "Controle Nacional"
CAB = ["numero", "card_id", "cod_verif", "obra", "med", "ano", "toma_cnpj",
       "vServ", "dCompet", "xml_abrasf_file_id", "status", "link_nac", "link_mun",
       "chave", "link_rec"]


def _ws(planilha):
    try:
        return planilha.worksheet(ABA_CONTROLE)
    except Exception:
        ws = planilha.add_worksheet(title=ABA_CONTROLE, rows=2000, cols=20)
        ws.update("A1:O1", [CAB])
        return ws


def registrar_pendente(planilha, dados: dict) -> None:
    """Acrescenta (ou ignora se já existir) uma nota aguardando o nacional."""
    ws = _ws(planilha)
    col_num = ws.col_values(1)
    if str(dados["numero"]) in [c.strip() for c in col_num]:
        return
    linha = [dados.get("numero", ""), dados.get("card_id", ""), dados.get("cod_verif", ""),
             dados.get("obra", ""), dados.get("med", ""), dados.get("ano", ""),
             "".join(c for c in str(dados.get("toma_cnpj", "")) if c.isdigit()),
             str(dados.get("vServ", "")), dados.get("dCompet", ""),
             dados.get("xml_abrasf_file_id", ""), "pendente", "",
             dados.get("link_mun", ""), "", dados.get("link_rec", "")]
    ws.append_row(linha, value_input_option="USER_ENTERED", table_range="A1")


def listar_pendentes(planilha) -> list[dict]:
    ws = _ws(planilha)
    vals = ws.get_all_values()
    out = []
    for i, row in enumerate(vals[1:], start=2):
        row = (row + [""] * 15)[:15]
        if row[10].strip().lower() == "pendente":
            d = dict(zip(CAB, row))
            d["_linha"] = i
            out.append(d)
    return out


def marcar_concluido(planilha, numero, link_nac, link_mun, chave) -> None:
    ws = _ws(planilha)
    col_num = ws.col_values(1)
    alvo = str(numero).strip()
    for i, v in enumerate(col_num, start=1):
        if v.strip() == alvo:
            ws.update(f"K{i}:N{i}", [["concluido", link_nac, link_mun, chave]],
                      value_input_option="USER_ENTERED")
            return


def atualizar_pendente(planilha, numero, link_mun=None, link_rec=None, xml_abrasf_file_id=None) -> bool:
    """Atualiza, na linha do pendente (coluna A = numero), os campos passados:
    J=xml_abrasf_file_id, M=link_mun, O=link_rec."""
    ws = _ws(planilha)
    col = ws.col_values(1)
    alvo = str(numero).strip()
    for i, v in enumerate(col, start=1):
        if v.strip() == alvo:
            if xml_abrasf_file_id is not None:
                ws.update(f"J{i}", [[xml_abrasf_file_id]], value_input_option="USER_ENTERED")
            if link_mun is not None:
                ws.update(f"M{i}", [[link_mun]], value_input_option="USER_ENTERED")
            if link_rec is not None:
                ws.update(f"O{i}", [[link_rec]], value_input_option="USER_ENTERED")
            return True
    return False


def ler_nsu(planilha) -> int:
    ws = _ws(planilha)
    try:
        v = ws.acell("P1").value
        return int(v) if v and str(v).strip().isdigit() else 0
    except Exception:
        return 0


def gravar_nsu(planilha, nsu) -> None:
    ws = _ws(planilha)
    ws.update("P1", [[int(nsu)]], value_input_option="USER_ENTERED")
