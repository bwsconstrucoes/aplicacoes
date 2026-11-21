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
              getattr(obra, "complemento", None),
              getattr(obra, "bairro", None),
              getattr(obra, "municipio", None),
              getattr(obra, "uf", None)]
    texto = ", ".join(" ".join(str(p).split()) for p in partes if p)
    # O CEP vai junto desde 20/11/2026: o modelo de pedido que a equipe usava
    # trazia, e é por ele que a transportadora calcula o frete.
    cep = " ".join(str(getattr(obra, "cep", "") or "").split())
    if texto and cep:
        texto += f" - CEP {cep}"
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


# ---------------------------------------------------------------------------
# Quando NÃO é na obra (migração 120)
#
# O dono, em 20/11/2026: "precisamos prever também quando a entrega não for na
# obra, por exemplo na sede. Isso acho que pode ser definido no fechamento. A
# princípio a entrega é na obra associada ao pedido."
#
# A regra continua sendo a obra. O desvio é POR OBRA DENTRO DO PEDIDO — o
# mesmo pedido pode mandar o material da obra A para o canteiro e o da obra B
# para a sede — e é decidido por quem fecha a compra.
# ---------------------------------------------------------------------------
TIPOS_DE_LOCAL = ("OBRA", "SEDE", "OUTRO")


def endereco_da_empresa(empresa: Optional[Any]) -> str:
    """O endereço da sede em uma linha, no mesmo formato do da obra."""
    if empresa is None:
        return SEM_ENDERECO
    partes = [getattr(empresa, "logradouro", None),
              getattr(empresa, "numero", None),
              getattr(empresa, "complemento", None),
              getattr(empresa, "bairro", None),
              getattr(empresa, "municipio", None),
              getattr(empresa, "uf", None)]
    texto = ", ".join(" ".join(str(p).split()) for p in partes if p)
    cep = " ".join(str(getattr(empresa, "cep", "") or "").split())
    if texto and cep:
        texto += f" - CEP {cep}"
    return texto or SEM_ENDERECO


def link_do_mapa(obra: Optional[Any]) -> Optional[str]:
    """O link do Google Maps do canteiro, quando a obra tem as coordenadas.

    O modelo de pedido que a equipe usava trazia esse link, e ele resolve o
    caso que o endereço não resolve: loteamento novo, rua projetada, zona rural.
    """
    lat, lon = getattr(obra, "latitude", None), getattr(obra, "longitude", None)
    if lat is None or lon is None:
        return None
    return f"https://www.google.com/maps?q={lat},{lon}"


def sede_da_obra(s, obra: Optional[Any]) -> dict[str, Any]:
    """A sede que serve de destino alternativo para o material desta obra: a
    empresa da obra, ou a empresa padrão quando a obra não tem empresa."""
    from app.apps.erp.core.cadastros import empresas as svc_empresas
    from app.apps.erp.db.models.cadastros import Empresa
    empresa = None
    if obra is not None and getattr(obra, "empresa_id", None):
        empresa = s.get(Empresa, obra.empresa_id)
    if empresa is None:
        empresa = svc_empresas.padrao(s)
    if empresa is None:
        return {"rotulo": "Sede", "endereco": SEM_ENDERECO, "tem": False}
    nome = (getattr(empresa, "nome_fantasia", None) or empresa.razao_social or "").strip()
    endereco = endereco_da_empresa(empresa)
    return {"rotulo": f"Sede — {nome}" if nome else "Sede",
            "endereco": endereco, "tem": endereco != SEM_ENDERECO}


def desvios_do_pedido(s, pedido_id: int) -> dict[int, Any]:
    """obra_id → o local alternativo definido no fechamento (quando há)."""
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import PedidoEntrega
    return {e.obra_id: e for e in s.scalars(
        select(PedidoEntrega).where(PedidoEntrega.pedido_id == pedido_id)).all()
        if e.pedido_id == pedido_id}


def local_de_entrega(obra: Optional[Any], desvio: Optional[Any] = None) -> dict[str, Any]:
    """Onde desce o material desta obra: o desvio, se houver; senão a obra."""
    if desvio is not None:
        return {"tipo": desvio.tipo, "rotulo": desvio.rotulo,
                "endereco": desvio.endereco, "na_obra": False, "mapa": None}
    return {"tipo": "OBRA", "rotulo": rotulo_da_obra(obra),
            "endereco": endereco_da_obra(obra), "na_obra": True,
            "mapa": link_do_mapa(obra)}


def definir_locais(s, pedido, locais: Optional[list[dict[str, Any]]],
                   obras_do_pedido: set[int], usuario) -> list[dict[str, Any]]:
    """Grava onde o material de cada obra desce. Chamado no fechamento.

    `locais` é uma lista de {obra_id, tipo, rotulo?, endereco?}. Obra que não
    aparece — ou aparece como OBRA — fica na regra: entrega no canteiro.
    """
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    from app.apps.erp.db.models.cadastros import Obra, PedidoEntrega

    for antigo in desvios_do_pedido(s, pedido.id).values():
        s.delete(antigo)
    s.flush()

    gravados = []
    for bruto in locais or []:
        try:
            obra_id = int(bruto.get("obra_id") or 0)
        except (TypeError, ValueError):
            raise ErroValidacao("Local de entrega com obra inválida.")
        tipo = str(bruto.get("tipo") or "OBRA").strip().upper()
        if tipo not in TIPOS_DE_LOCAL:
            raise ErroValidacao("Local de entrega inválido.")
        if tipo == "OBRA":
            continue
        if obra_id not in obras_do_pedido:
            raise ErroValidacao(
                "Local de entrega definido para uma obra que não está neste pedido.")
        obra = s.get(Obra, obra_id)
        if tipo == "SEDE":
            sede = sede_da_obra(s, obra)
            if not sede["tem"]:
                raise ErroValidacao(
                    "A empresa desta obra está sem endereço no cadastro. Preencha "
                    "em Configurações › Empresas, ou escolha \"outro endereço\" e "
                    "digite onde o material desce.")
            rotulo, endereco = sede["rotulo"], sede["endereco"]
        else:
            endereco = " ".join(str(bruto.get("endereco") or "").split())
            rotulo = " ".join(str(bruto.get("rotulo") or "").split()) or "Outro endereço"
            if len(endereco) < 10:
                raise ErroValidacao(
                    "Escreva o endereço completo de onde o material desce — é o "
                    "motorista do fornecedor quem vai ler.")
        s.add(PedidoEntrega(pedido_id=pedido.id, obra_id=obra_id, tipo=tipo,
                            rotulo=rotulo, endereco=endereco,
                            definido_por=getattr(usuario, "id", None)))
        gravados.append({"obra_id": obra_id, "tipo": tipo, "rotulo": rotulo,
                         "endereco": endereco})
    s.flush()
    return gravados
