"""O documento que NUNCA foi arquivado vira aviso na agenda — com banco real.

Itens 6 e 7 de `GESTAO_DOCUMENTOS.md`.

O aviso que já existia olhava para documento que VAI VENCER. Faltava o outro
lado, e é o que faz perder licitação e atrasar medição: o documento que nunca
entrou. Certidão vencida pelo menos existe e o sistema sabe de quando é; a
ausência é silêncio — ninguém repara até o dia em que o cliente pede a pasta
da medição e ela sai pela metade.

Com banco porque a conferência do bloco é feita de consultas: qual documento
existe para aquela obra, naquela competência, dentro da validade.

O que se prova:

  1. Obra em execução com a pasta fiscal incompleta vira aviso, e o aviso DIZ
     quais documentos faltam.
  2. Completou a pasta, o aviso SOME sozinho no próximo recálculo — aviso
     deduzido que não some é aviso que ninguém lê depois de um mês.
  3. Obra que não está em execução não gera cobrança. Cobrar documentação de
     obra concluída é ruído.
  4. A habilitação da empresa é cobrada por empresa, não por obra.
  5. A lista de origens do filtro sai do SERVIDOR — foi assim que o aviso do
     certificado digital ficou sem filtro, calado, na entrega anterior.
  6. A ficha do título leva a competência no formato que o bloco precisa.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.agenda import geradores
from app.apps.erp.core.agenda import service as svc_agenda
from app.apps.erp.core.arquivo import blocos, catalogo
from app.apps.erp.core.arquivo import service as arq
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (Categoria, Empresa, Fornecedor, Obra,
                                              PerfilUsuario as P, RegimeTributario,
                                              TipoPessoa, Usuario)
from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                               Parcela, Rateio, StatusParcela,
                                               StatusTitulo, TipoTitulo, Titulo)
from tests.conftest import como

pytestmark = pytest.mark.banco

# O bloco FISCAL cobra estes por competência; a lista está em blocos.PADRAO.
FISCAL_OBRIGATORIOS = ("FOLHA", "RELATORIO-FGTS", "GUIA-FGTS", "COMPROVANTE-FGTS",
                       "DCTFWEB-RECIBO", "DCTFWEB-CREDITOS", "DARF-INSS",
                       "COMPROVANTE-INSS")


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    blocos.aplicar(s)
    admin = Usuario(nome="Admin dos avisos", email="avisos@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    emp = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181", ativo=True)
    s.add_all([admin, emp])
    s.flush()
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto", empresa_id=emp.id,
                fase="EM_EXECUCAO")
    s.add(obra)
    s.flush()
    return {"s": s, "admin": admin, "empresa": emp, "obra": obra}


def _mes_passado(hoje: date) -> date:
    primeiro = hoje.replace(day=1)
    return (primeiro - timedelta(days=1)).replace(day=1)


def _guardar(cenario, tipo, *, competencia=None, validade=None):
    s = cenario["s"]
    t = catalogo.obter(s, tipo)
    dono = ({"obra_id": cenario["obra"].id} if t.dono == "OBRA"
            else {"empresa_id": cenario["empresa"].id})
    return arq.arquivar(
        s, f"conteudo {tipo} {competencia}".encode(), f"{tipo.lower()}.pdf",
        tipo_codigo=tipo, competencia=competencia, validade=validade,
        emissao=date.today(), usuario=cenario["admin"], **dono)


def _avisos_de_documento(s, hoje=None):
    return [e for e in geradores.documentos(s, hoje or date.today())]


# ---------------------------------------------------------------------------
# 1. A PASTA FISCAL INCOMPLETA
# ---------------------------------------------------------------------------
def test_pasta_fiscal_vazia_vira_aviso_dizendo_o_que_falta(cenario):
    avisos = _avisos_de_documento(cenario["s"])
    fiscal = [a for a in avisos if a["chave"].startswith("DOCUMENTO:fiscal=")]
    assert fiscal, "obra em execução sem documentação nenhuma tem de virar aviso"
    a = fiscal[0]
    assert "ESCPLANALTO" in a["titulo"]
    assert "Folha de pagamento" in a["detalhe"]
    assert a["obra_id"] == cenario["obra"].id
    assert a["origem"] == "DOCUMENTO"


def test_pasta_completa_nao_gera_aviso(cenario):
    s = cenario["s"]
    comp = _mes_passado(date.today())
    for tipo in FISCAL_OBRIGATORIOS:
        _guardar(cenario, tipo, competencia=comp)
    s.flush()

    fiscal = [a for a in _avisos_de_documento(s)
              if a["chave"] == f"DOCUMENTO:fiscal={cenario['obra'].id}:{comp:%Y-%m}"]
    assert fiscal == [], "com tudo arquivado não há o que cobrar"


def test_obra_fora_de_execucao_nao_e_cobrada(cenario):
    """Cobrar documentação de obra concluída é ruído, e ruído faz a pessoa
    parar de olhar a agenda."""
    cenario["obra"].fase = "CONCLUIDA"
    cenario["s"].flush()
    fiscal = [a for a in _avisos_de_documento(cenario["s"])
              if a["chave"].startswith("DOCUMENTO:fiscal=")]
    assert fiscal == []


# ---------------------------------------------------------------------------
# 2. O AVISO SOME QUANDO O PROBLEMA ACABA
# ---------------------------------------------------------------------------
def test_o_aviso_some_sozinho_quando_a_pasta_e_completada(cenario):
    s = cenario["s"]
    svc_agenda.sincronizar(s)
    s.flush()
    abertos = svc_agenda.listar(s, situacao="ABERTO", origem="DOCUMENTO")
    assert abertos["eventos"], "o aviso tem de nascer no primeiro recálculo"

    comp = _mes_passado(date.today())
    for tipo in FISCAL_OBRIGATORIOS:
        _guardar(cenario, tipo, competencia=comp)
    # a competência anterior também, senão o aviso dela continua
    anterior = _mes_passado(comp)
    for tipo in FISCAL_OBRIGATORIOS:
        _guardar(cenario, tipo, competencia=anterior)
    s.flush()

    svc_agenda.sincronizar(s)
    s.flush()
    restam = [e for e in svc_agenda.listar(s, situacao="ABERTO",
                                           origem="DOCUMENTO")["eventos"]
              if "fiscal" in (e.get("chave") or "") or "fiscal" in e["titulo"].lower()
              or "Documentação fiscal" in e["titulo"]]
    assert restam == [], "aviso deduzido que não some é aviso que ninguém lê"


# ---------------------------------------------------------------------------
# 3. A HABILITAÇÃO DA EMPRESA
# ---------------------------------------------------------------------------
def test_habilitacao_incompleta_e_cobrada_por_empresa(cenario):
    avisos = [a for a in _avisos_de_documento(cenario["s"])
              if a["chave"].startswith("DOCUMENTO:habilitacao=")]
    assert len(avisos) == 1
    a = avisos[0]
    assert "BWS" in a["titulo"]
    assert a["empresa_id"] == cenario["empresa"].id
    assert a["obra_id"] is None, "habilitação é da empresa, não de uma obra"
    assert "envelope de licitação" in a["detalhe"]


def test_empresa_inativa_nao_e_cobrada(cenario):
    cenario["empresa"].ativo = False
    cenario["s"].flush()
    avisos = [a for a in _avisos_de_documento(cenario["s"])
              if a["chave"].startswith("DOCUMENTO:habilitacao=")]
    assert avisos == []


# ---------------------------------------------------------------------------
# 4. O FILTRO DA TELA VEM DO SERVIDOR
# ---------------------------------------------------------------------------
def test_a_lista_de_origens_sai_do_servidor_e_inclui_as_novas(cenario):
    """Quando o certificado digital virou aviso, a lista escrita na tela ficou
    para trás e o aviso novo não tinha como ser filtrado — e nada quebrou,
    então ninguém viu."""
    d = svc_agenda.listar(cenario["s"])
    codigos = {c for c, _ in d["origens"]}
    assert {"REAJUSTE", "CERTIDAO", "LOCACAO", "CONTRATO",
            "CERTIFICADO", "DOCUMENTO", "MANUAL"} <= codigos


def test_a_rota_da_agenda_devolve_as_origens(app_real, cenario):
    d = como(app_real, cenario["admin"].id).get("/erp/api/agenda").get_json()
    assert any(c == "DOCUMENTO" for c, _ in d["origens"])


# ---------------------------------------------------------------------------
# 5. OS BOTÕES NOS OUTROS LUGARES
# ---------------------------------------------------------------------------
def test_a_ficha_do_titulo_leva_a_competencia_no_formato_do_bloco(app_real, cenario):
    s = cenario["s"]
    cliente = Fornecedor(razao_social="PREFEITURA EXEMPLO", cnpj_cpf="10572071000112",
                         tipo_pessoa=TipoPessoa.PJ, ativo=True,
                         regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="1.1.01", descricao="Receita de obra")
    s.add_all([cliente, cat])
    s.flush()
    t = Titulo(numero_sp="SP-77001", tipo=TipoTitulo.T1_MATERIAL_NFE,
               especie=EspecieTitulo.RECEBER, fornecedor_id=cliente.id,
               descricao="Medição 3", numero_medicao="3",
               valor_bruto=Decimal("1000.00"), valor_liquido=Decimal("1000.00"),
               competencia=date(2026, 8, 1), categoria_id=cat.id,
               forma_pagamento=FormaPagamento.TED, status=StatusTitulo.APROVADO,
               solicitante_id=cenario["admin"].id)
    s.add(t)
    s.flush()
    s.add(Parcela(titulo_id=t.id, numero=1, vencimento=date(2026, 9, 10),
                  valor=Decimal("1000.00"), status=StatusParcela.ABERTA))
    s.add(Rateio(titulo_id=t.id, obra_id=cenario["obra"].id,
                 valor=Decimal("1000.00"), percentual=Decimal("100")))
    s.flush()

    ficha = (como(app_real, cenario["admin"].id)
             .get(f"/erp/api/titulos/{t.id}").get_json()["titulo"]["cabecalho"])
    assert ficha["competencia_iso"] == "2026-08"
    assert ficha["numero_medicao"] == "3"
    assert ficha["obra_ids"] == [cenario["obra"].id]


def test_o_bloco_da_obra_baixa_pelo_endereco_que_o_botao_monta(app_real, cenario):
    """O botão da tela da obra monta este endereço. Se ele mudar de forma sem
    a rota mudar junto, o download vira 404 e ninguém testa isso à mão."""
    r = como(app_real, cenario["admin"].id).get(
        f"/erp/api/arquivo/blocos/OBRA/baixar?obra_id={cenario['obra'].id}")
    assert r.status_code == 200
    assert r.mimetype in ("application/zip", "application/x-zip-compressed")
