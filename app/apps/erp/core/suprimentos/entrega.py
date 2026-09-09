# ============================================================================
# ERP — core/suprimentos/entrega.py
# ONDE o material desce.
#
# Módulo pequeno de propósito: a cotação e o pedido precisam da mesma resposta
# ("qual o endereço desta obra?") e ela não pode ser escrita duas vezes. Se um
# dia a obra ganhar um campo de endereço novo, muda aqui e os dois documentos
# passam a dizer a mesma coisa.
#
# Por que o endereço vai no documento do fornecedor:
#   - na COTAÇÃO, porque o frete depende da distância. Pedir preço sem dizer
#     onde entregar é receber preço que muda depois;
#   - no PEDIDO, porque é o motorista quem lê. Um mesmo pedido leva material
#     para obras diferentes, e o que desce em cada lugar tem de estar separado.
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

SEM_ENDERECO = "Endereço não informado"


def endereco_da_obra(obra: Optional[Any]) -> str:
    """O endereço de entrega em uma linha. Obra sem endereço cadastrado diz
    isso com todas as letras — em branco, o motorista descobre no caminho."""
    if obra is None:
        return SEM_ENDERECO
    partes = [getattr(obra, "endereco", None),
              getattr(obra, "numero_endereco", None),
              getattr(obra, "bairro", None),
              getattr(obra, "municipio", None),
              getattr(obra, "uf", None)]
    texto = ", ".join(" ".join(str(p).split()) for p in partes if p)
    return texto or SEM_ENDERECO


def rotulo_da_obra(obra: Optional[Any]) -> str:
    """"CREPETRIUNFO · Creche Triunfo", sem repetir quando os dois são iguais."""
    if obra is None:
        return ""
    codigo = " ".join(str(getattr(obra, "codigo", "") or "").split())
    nome = " ".join(str(getattr(obra, "nome", "") or "").split())
    if not nome or nome.lower() == codigo.lower():
        return codigo
    return f"{codigo} · {nome}"
