# ============================================================================
# ERP — core/titulos/empreiteiro.py
# OS DADOS DO EMPREITEIRO PELO DOCUMENTO — Cartão CNPJ, RG, CNH, comprovante.
#
# ⚑ O PEDIDO (27/11/2026): *"Para formalizar o contrato, vamos usar a mesma
# lógica de todo o resto do ERP, que é a importação de informações a partir da
# leitura dos documentos (…) na hora de preencher os dados da empresa ou da
# pessoa, utilizar um cartão CNPJ, uma identidade, RG. Então tem que ter esse
# campo aí para poder fazer a leitura."*
#
# É a regra "um cadastro, uma porta" (CLAUDE.md): o formulário com os campos, e
# o documento DENTRO dele, no alto — "jogue aqui, ele preenche os campos
# abaixo". O que a leitura preenche fica editável e vai para conferência; nada é
# gravado sem alguém olhar. E o documento lido é ARQUIVADO no cadastro do
# empreiteiro, na mesma gravação.
#
# NENHUM LEITOR NOVO: o Cartão CNPJ passa pelo mesmo leitor do cadastro de
# empresas (`arquivo.leitura` + `preenchimento`), e RG/CNH/comprovante pelo
# mesmo da admissão (`documentos_de_pessoa.ler_varios`, uma chamada por lote).
#
# ⚑ O QUE IDENTIFICA O EMPREITEIRO É O CPF OU O CNPJ (27/11/2026). Ele: *"quando
# eu falei RG, na verdade eu me referi a documento da pessoa de maneira geral
# (…) o documento para se fazer esse tipo de coisa é o CPF e CNPJ."* O RG não
# é pedido nem entra no contrato: o documento serve para a LEITURA (dele saem
# CPF, nome e endereço) e fica arquivado no cadastro.
# ============================================================================
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento

logger = logging.getLogger(__name__)

CAMPOS = ("tipo_pessoa", "documento", "nome", "cep",
          "logradouro", "numero", "complemento", "bairro", "municipio", "uf",
          "telefone", "email")
TETO_DE_ARQUIVOS = 6


def _digitos(v: Any) -> str:
    return re.sub(r"\D", "", str(v or ""))


def do_cadastro(s: Session, fornecedor_id: int) -> dict[str, Any]:
    """Os dados que o credor já tem — para o formulário abrir preenchido."""
    from app.apps.erp.db.models.cadastros import Fornecedor
    f = s.get(Fornecedor, int(fornecedor_id))
    if f is None:
        raise ErroValidacao("Empreiteiro não encontrado.")
    doc = _digitos(f.cnpj_cpf)
    return {"id": f.id, "tipo_pessoa": "PF" if len(doc) == 11 else "PJ",
            "documento": doc, "nome": f.razao_social, "cep": f.cep or "",
            "logradouro": f.logradouro or "", "numero": f.numero or "",
            "complemento": f.complemento or "", "bairro": f.bairro or "",
            "municipio": f.municipio or "", "uf": f.uf or "",
            "telefone": f.telefone or "", "email": f.email or ""}


def ler(s: Session, arquivos: list[tuple[bytes, str]], tipo_pessoa: str) -> dict[str, Any]:
    """Lê os papéis e devolve os campos sugeridos. Nunca grava.

    PJ: o Cartão CNPJ (o primeiro arquivo) pelo leitor das empresas.
    PF: RG, CNH, CPF e comprovante de endereço, num lote só.
    """
    arquivos = list(arquivos)[:TETO_DE_ARQUIVOS]
    if not arquivos:
        raise ErroValidacao("Escolha o documento.")
    if (tipo_pessoa or "").upper() == "PJ":
        from app.apps.erp.core.arquivo import leitura, preenchimento
        conteudo, nome = arquivos[0]
        sug = leitura.sugerir(s, conteudo, nome,
                              extracao=preenchimento.instrucao_de_extracao_empresa(),
                              dono_e_novo=True)
        x = sug.get("dados_extraidos") or {}
        campos = {"tipo_pessoa": "PJ", "documento": _digitos(x.get("cnpj")),
                  "nome": x.get("razao_social") or "",
                  **{k: str(x.get(k) or "") for k in
                     ("cep", "logradouro", "numero", "complemento", "bairro",
                      "municipio", "uf", "telefone", "email")}}
        aviso = "" if campos["documento"] else \
            "Não achei o CNPJ — confira se é o Cartão CNPJ."
    else:
        from app.apps.erp.core import documentos_de_pessoa as svc_doc
        r = svc_doc.ler_varios(arquivos)
        x = r.get("campos") or {}
        campos = {"tipo_pessoa": "PF", "documento": _digitos(x.get("cpf")),
                  "nome": x.get("nome") or "",
                  "numero": x.get("numero_endereco") or "",
                  **{k: str(x.get(k) or "") for k in
                     ("cep", "logradouro", "complemento", "bairro", "municipio", "uf")}}
        aviso = "" if campos["documento"] else \
            "Não achei o CPF — confira o documento ou digite."
    # Já cadastrado? Quem decide é a pessoa, mas ela precisa saber.
    ja = None
    if campos["documento"]:
        from app.apps.erp.db.models.cadastros import Fornecedor
        f = s.scalars(select(Fornecedor).where(
            Fornecedor.cnpj_cpf == campos["documento"])).first()
        if f is not None:
            ja = {"id": f.id, "nome": f.razao_social}
    return {"campos": {k: v for k, v in campos.items() if v not in (None,)},
            "ja_cadastrado": ja, "aviso": aviso}


def gravar(s: Session, dados: dict[str, Any], arquivos: list[tuple[bytes, str]],
           usuario) -> dict[str, Any]:
    """Cria o empreiteiro, ou completa o cadastro de quem já existe, com o que a
    pessoa CONFERIU na tela — e arquiva os documentos lidos nele."""
    from app.apps.erp.core.cadastros import fornecedores as svc_forn
    from app.apps.erp.core.cadastros.validadores import cnpj_valido, cpf_valido
    from app.apps.erp.core.documentos.armazenamento import salvar
    from app.apps.erp.db.models.cadastros import Fornecedor
    doc = _digitos(dados.get("documento"))
    pf = len(doc) == 11
    if not ((pf and cpf_valido(doc)) or (len(doc) == 14 and cnpj_valido(doc))):
        raise ErroValidacao("CPF ou CNPJ do empreiteiro inválido — confira.")
    nome = (dados.get("nome") or "").strip()
    if len(nome) < 3:
        raise ErroValidacao("Informe o nome do empreiteiro.")
    f = s.scalars(select(Fornecedor).where(Fornecedor.cnpj_cpf == doc)).first()
    novo = f is None
    if novo:
        f = svc_forn.criar(s, {"tipo_pessoa": "PF" if pf else "PJ", "cnpj_cpf": doc,
                               "razao_social": nome, "origem": "CONTRATO_EMPREITA"}, usuario)
        s.flush()
    mudou = {}
    for campo in ("cep", "logradouro", "numero", "complemento", "bairro", "municipio",
                  "uf", "telefone", "email"):
        valor = str(dados.get(campo) or "").strip()
        if campo in ("cep", "telefone"):
            valor = _digitos(valor)
        if campo == "uf":
            valor = valor.upper()[:2]
        if valor and valor != (getattr(f, campo) or ""):
            mudou[campo] = valor
            setattr(f, campo, valor)
    if not novo and nome and nome != f.razao_social and not f.razao_social_rfb:
        mudou["razao_social"] = nome
        f.razao_social = nome
    arquivados = []
    for conteudo, nome_arq in list(arquivos)[:TETO_DE_ARQUIVOS]:
        if conteudo:
            a = salvar(s, conteudo, nome_arq, entidade_tipo="fornecedor", entidade_id=f.id,
                       categoria="CONTRATO" if not pf else "OUTRO",
                       descricao="Documento do empreiteiro lido no contrato de empreita",
                       usuario=usuario)
            arquivados.append(a.id)
    registrar_evento(s, "fornecedor", f.id,
                     "CADASTRADO_PELO_CONTRATO" if novo else "COMPLETADO_PELO_CONTRATO",
                     {"campos": sorted(mudou), "documentos": arquivados},
                     getattr(usuario, "id", None))
    s.flush()
    return {"fornecedor_id": f.id, "novo": novo, "mudou": sorted(mudou),
            "documentos": len(arquivados)}
