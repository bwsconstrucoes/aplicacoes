# ============================================================================
# BWS ERP — core/pagamentos/boleto.py
# Validação estrutural de boletos (E6/C1 da especificação). Funções puras.
#
# Cobre:
#   - Linha digitável BANCÁRIA (47 dígitos): DVs dos 3 campos (módulo 10),
#     DV geral do código de barras (módulo 11), fator de vencimento (com a
#     rolagem oficial de 22/02/2025) e valor embutido.
#   - Linha digitável de ARRECADAÇÃO/GUIA (48 dígitos, começa com 8):
#     DVs dos 4 blocos (módulo 10 ou 11 conforme o indicador de valor).
# ============================================================================
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from app.apps.erp.core.cadastros.validadores import somente_digitos

# Fator de vencimento: base histórica 07/10/1997 = fator 0; ao atingir 9999
# (21/02/2025), rola: 22/02/2025 volta a ser fator 1000 (FEBRABAN).
_BASE_1997 = date(1997, 10, 7)
_DATA_ROLAGEM = date(2025, 2, 22)          # fator 1000 na nova era
_FATOR_MIN_NOVA_ERA = 1000


@dataclass
class DadosBoleto:
    valido: bool
    mensagem: str = ""
    tipo: str = ""                          # BANCARIO | ARRECADACAO
    banco: Optional[str] = None             # código COMPE (bancário)
    valor: Optional[Decimal] = None         # valor embutido (None se aberto)
    vencimento: Optional[date] = None       # None quando fator = 0000
    codigo_barras: Optional[str] = None
    linha_digitavel: Optional[str] = None


def _modulo10(numero: str) -> int:
    soma, peso = 0, 2
    for d in reversed(numero):
        parcial = int(d) * peso
        soma += parcial if parcial < 10 else (parcial // 10) + (parcial % 10)
        peso = 1 if peso == 2 else 2
    resto = soma % 10
    return 0 if resto == 0 else 10 - resto


def _modulo11_bancario(numero: str) -> int:
    soma, peso = 0, 2
    for d in reversed(numero):
        soma += int(d) * peso
        peso = 2 if peso == 9 else peso + 1
    resto = soma % 11
    dv = 11 - resto
    return 1 if dv in (0, 10, 11) else dv


def _modulo11_arrecadacao(numero: str) -> int:
    soma, peso = 0, 2
    for d in reversed(numero):
        soma += int(d) * peso
        peso = 2 if peso == 9 else peso + 1
    resto = soma % 11
    if resto in (0, 1):
        return 0
    if resto == 10:
        return 1
    return 11 - resto


def _data_do_fator(fator: int, referencia: Optional[date] = None) -> Optional[date]:
    """Converte fator de vencimento em data, resolvendo a ambiguidade da
    rolagem de 2025: para fatores >= 1000 existem duas datas possíveis
    (era antiga e era nova); escolhe-se a mais próxima da referência."""
    if fator <= 0:
        return None
    antiga = _BASE_1997 + timedelta(days=fator)
    if fator < _FATOR_MIN_NOVA_ERA:
        return antiga  # só existe na era antiga (fatores 1..999 pré-2000)
    nova = _DATA_ROLAGEM + timedelta(days=fator - _FATOR_MIN_NOVA_ERA)
    ref = referencia or date.today()
    return antiga if abs((antiga - ref).days) <= abs((nova - ref).days) else nova


def validar_linha_digitavel(linha: str, referencia: Optional[date] = None) -> DadosBoleto:
    """Valida a linha digitável e extrai banco, valor e vencimento."""
    dig = somente_digitos(linha)

    if len(dig) == 47:
        return _validar_bancario(dig, referencia)
    if len(dig) == 48:
        return _validar_arrecadacao(dig)
    return DadosBoleto(False, f"Linha digitável com {len(dig)} dígitos — esperado 47 (boleto) ou 48 (guia).")


def _validar_bancario(dig: str, referencia: Optional[date]) -> DadosBoleto:
    campo1, dv1 = dig[0:9], int(dig[9])
    campo2, dv2 = dig[10:20], int(dig[20])
    campo3, dv3 = dig[21:31], int(dig[31])
    dv_geral = int(dig[32])
    fator = int(dig[33:37])
    valor_cent = int(dig[37:47])

    if _modulo10(campo1) != dv1:
        return DadosBoleto(False, "DV do 1º campo da linha digitável não confere (módulo 10).")
    if _modulo10(campo2) != dv2:
        return DadosBoleto(False, "DV do 2º campo da linha digitável não confere (módulo 10).")
    if _modulo10(campo3) != dv3:
        return DadosBoleto(False, "DV do 3º campo da linha digitável não confere (módulo 10).")

    # Remonta o código de barras: banco(3) moeda(1) DV(1) fator(4) valor(10) + campo livre(25)
    campo_livre = campo1[4:9] + campo2 + campo3
    barras_sem_dv = dig[0:4] + dig[33:47] + campo_livre
    if _modulo11_bancario(barras_sem_dv) != dv_geral:
        return DadosBoleto(False, "DV geral do código de barras não confere (módulo 11).")

    codigo_barras = barras_sem_dv[0:4] + str(dv_geral) + barras_sem_dv[4:]
    valor = Decimal(valor_cent) / 100 if valor_cent > 0 else None
    return DadosBoleto(
        True, tipo="BANCARIO", banco=dig[0:3],
        valor=valor, vencimento=_data_do_fator(fator, referencia),
        codigo_barras=codigo_barras, linha_digitavel=dig,
    )


def _validar_arrecadacao(dig: str) -> DadosBoleto:
    if dig[0] != "8":
        return DadosBoleto(False, "Linha de 48 dígitos deve iniciar com 8 (arrecadação/guia).")
    indicador_valor = dig[2]
    usa_mod10 = indicador_valor in ("6", "7")
    blocos = [dig[0:11] + dig[11], dig[12:23] + dig[23], dig[24:35] + dig[35], dig[36:47] + dig[47]]
    for i, bloco in enumerate(blocos, start=1):
        corpo, dv = bloco[:-1], int(bloco[-1])
        calc = _modulo10(corpo) if usa_mod10 else _modulo11_arrecadacao(corpo)
        if calc != dv:
            return DadosBoleto(False, f"DV do bloco {i} da guia não confere "
                                      f"(módulo {'10' if usa_mod10 else '11'}).")
    corpo_barras = dig[0:11] + dig[12:23] + dig[24:35] + dig[36:47]
    valor_cent = int(corpo_barras[4:15])
    valor = Decimal(valor_cent) / 100 if valor_cent > 0 else None
    return DadosBoleto(True, tipo="ARRECADACAO", valor=valor,
                       codigo_barras=corpo_barras, linha_digitavel=dig)
