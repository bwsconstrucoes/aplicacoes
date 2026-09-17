"""A ÁRVORE DE PASTAS DO GOOGLE DRIVE, e a data do documento corrigida.

Pedido do dono em 17/09/2026, depois de ligar o Drive em produção:

    "A minha ideia é que tivesse tudo no Google Drive, separado numa pasta de
    obra (…) tem a pasta Obras, aí tem as subpastas. E aqueles outros
    documentos (…) salvar em outra pasta, tipo Arquivo (…) é importante, senão
    fica bagunçado. E a gente não ocupa espaço na base de dados, que é o mais
    importante. (…) Só tem que ter cuidado para não excluir."

E, na mesma conversa:

    "Eu vi um contrato que está dando que está vencido, mas na verdade está
    vencido porque eu escrevi a validade errado. Eu queria daqui ir para o
    cadastro e editar isso direto."

O QUE ESTES TESTES SEGURAM

  1. Documento de obra cai em `Obras/<código - nome>`; o resto, em
     `Arquivo/<gaveta>`. Se isso escorregar, a organização morre no primeiro
     mês e ninguém acha nada no computador.
  2. **Pasta que já existe é reusada, não duplicada.** Duas pastas com o mesmo
     nome é o começo de documento sumido.
  3. **Nada é apagado.** Reorganizar move (troca o pai); o arquivo continua o
     mesmo, com o mesmo id.
  4. A data corrigida arruma o aviso de vencimento junto — e a correção fica na
     trilha.

O GOOGLE É DUBLADO aqui: o que precisa de prova é o que o ERP decide, não o
Drive. Bater no Google de dentro da suíte seria lento, caro e frágil.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.apps.erp.core.arquivo import catalogo, service as svc_arq
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.documentos import drive
from app.apps.erp.db.models.cadastros import (Empresa, Obra, Parametro,
                                              PerfilUsuario as P, Usuario)

pytestmark = pytest.mark.banco

PDF = b"%PDF-1.4 documento"
RAIZ = "id-da-raiz"


class DriveFalso:
    """O mínimo do Google Drive que este código usa: listar e criar pasta."""

    def __init__(self):
        self.pastas = {}          # (nome, pai) -> id
        self.criadas = []
        self.movimentos = []
        self.apagados = []

    # --- a cara da biblioteca do Google ---
    def files(self):
        return self

    def list(self, q="", **k):
        nome = q.split("name='", 1)[1].split("'", 1)[0]
        pai = q.split("and '", 1)[1].split("'", 1)[0]
        achado = self.pastas.get((nome, pai))
        return _Executa({"files": [{"id": achado, "name": nome}] if achado else []})

    def create(self, body=None, **k):
        nome, pai = body["name"], body["parents"][0]
        novo = f"id-{nome.lower().replace(' ', '-')}"
        self.pastas[(nome, pai)] = novo
        self.criadas.append((nome, pai))
        return _Executa({"id": novo})

    def get(self, fileId=None, **k):
        return _Executa({"parents": [RAIZ]})

    def update(self, fileId=None, addParents=None, removeParents=None, **k):
        self.movimentos.append((fileId, removeParents, addParents))
        return _Executa({"id": fileId})

    def delete(self, fileId=None, **k):
        self.apagados.append(fileId)
        return _Executa({})


class _Executa:
    def __init__(self, resposta):
        self._r = resposta

    def execute(self):
        return self._r


@pytest.fixture
def cenario(sessao_real, monkeypatch):
    s = sessao_real
    catalogo.aplicar(s)
    s.add(Parametro(chave=drive.CHAVE_PASTA, valor=RAIZ))
    s.add(Parametro(chave=drive.CHAVE_LIGADO, valor="1"))
    admin = Usuario(nome="Admin", email="drive.pastas@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    emp = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181", ativo=True)
    s.add_all([admin, emp])
    s.flush()
    obra = Obra(codigo="ESCPE18", nome="Escola Planalto", empresa_id=emp.id,
                fase="EM_EXECUCAO")
    s.add(obra)
    s.flush()
    falso = DriveFalso()
    monkeypatch.setattr(drive, "_servico", lambda impersonar="": falso)
    # "usável" também olha a credencial no ambiente; aqui ela é de mentira,
    # porque quem fala com o Google está dublado.
    monkeypatch.setenv("GOOGLE_CREDENTIALS_BASE64", "credencial-de-teste")
    return {"s": s, "admin": admin, "obra": obra, "empresa": emp, "drive": falso}


# ---------------------------------------------------------------------------
# 1. CADA COISA NA SUA PASTA
# ---------------------------------------------------------------------------
def test_documento_de_obra_vai_para_a_pasta_da_obra(cenario):
    destino = drive.pasta_de(cenario["s"], "obra", cenario["obra"].id)
    criadas = cenario["drive"].criadas
    assert ("Obras", RAIZ) in criadas, "a pasta Obras nasce na raiz"
    assert ("ESCPE18 - Escola Planalto", "id-obras") in criadas
    assert destino == "id-escpe18---escola-planalto"


def test_o_resto_vai_para_arquivo_na_gaveta_certa(cenario):
    s = cenario["s"]
    assert drive.pasta_de(s, "colaborador", 1) == "id-pessoas"
    assert drive.pasta_de(s, "fornecedor", 1) == "id-fornecedores"
    assert drive.pasta_de(s, "empresa", cenario["empresa"].id) == "id-empresas"
    assert drive.pasta_de(s, "titulo", 1) == "id-financeiro"
    assert ("Arquivo", RAIZ) in cenario["drive"].criadas


def test_tipo_desconhecido_nao_fica_solto_na_raiz(cenario):
    """Cai em Arquivo/Diversos — lugar feio, mas lugar."""
    assert drive.pasta_de(cenario["s"], "coisa_nova", 7) == "id-diversos"


# ---------------------------------------------------------------------------
# 2. PASTA QUE JÁ EXISTE É REUSADA
# ---------------------------------------------------------------------------
def test_nao_cria_a_mesma_pasta_duas_vezes(cenario):
    s = cenario["s"]
    primeiro = drive.pasta_de(s, "obra", cenario["obra"].id)
    quantas = len(cenario["drive"].criadas)
    segundo = drive.pasta_de(s, "obra", cenario["obra"].id)
    assert primeiro == segundo
    assert len(cenario["drive"].criadas) == quantas, "a segunda vez não cria nada"


def test_pasta_criada_a_mao_no_drive_e_aproveitada(cenario):
    """Se o dono já tem uma pasta "Obras" lá, o sistema entra nela."""
    s = cenario["s"]
    cenario["drive"].pastas[("Obras", RAIZ)] = "pasta-que-ja-existia"
    drive.pasta_de(s, "obra", cenario["obra"].id)
    assert ("Obras", RAIZ) not in cenario["drive"].criadas
    assert drive._pasta_guardada(s, "raiz:Obras") == "pasta-que-ja-existia"


def test_a_pasta_lembrada_nao_bate_no_drive_de_novo(cenario):
    s = cenario["s"]
    drive.pasta_de(s, "obra", cenario["obra"].id)
    chave = f"obra:{cenario['obra'].id}"
    assert drive._pasta_guardada(s, chave) != ""


def test_barra_no_nome_da_obra_nao_vira_subpasta(cenario):
    """"Escola A/B" viraria duas pastas no Drive — e o nome some do caminho."""
    s, obra = cenario["s"], cenario["obra"]
    obra.nome = "Escola A/B\nsegunda linha"
    s.flush()
    drive.pasta_de(s, "obra", obra.id)
    nomes = [n for n, _ in cenario["drive"].criadas]
    assert all("/" not in n and "\\n" not in n for n in nomes)


# ---------------------------------------------------------------------------
# 3. SEM PASTA CONFIGURADA, NADA QUEBRA
# ---------------------------------------------------------------------------
def test_sem_pasta_configurada_devolve_vazio(cenario):
    s = cenario["s"]
    s.get(Parametro, drive.CHAVE_PASTA).valor = ""
    s.flush()
    assert drive.pasta_de(s, "obra", cenario["obra"].id) == ""


def test_falha_do_drive_cai_na_raiz_em_vez_de_derrubar(cenario, monkeypatch):
    """Guardar no lugar menos bonito é melhor do que não guardar."""
    def _explode(*a, **k):
        raise drive.ErroDrive("Drive fora do ar")
    monkeypatch.setattr(drive, "garantir_pasta", _explode)
    assert drive.pasta_de(cenario["s"], "obra", cenario["obra"].id) == RAIZ


# ---------------------------------------------------------------------------
# 4. REORGANIZAR MOVE — E NUNCA APAGA
# ---------------------------------------------------------------------------
def test_reorganizar_move_e_nao_apaga(cenario):
    from app.apps.erp.core.documentos.armazenamento import reorganizar_no_drive
    from app.apps.erp.db.models.financeiro import Anexo
    s = cenario["s"]
    s.add(Anexo(entidade_tipo="obra", entidade_id=cenario["obra"].id,
                nome_arquivo="contrato.pdf", guardado_em="DRIVE",
                drive_file_id="arquivo-solto-na-raiz", hash_sha256="abc123"))
    s.flush()

    r = reorganizar_no_drive(s)
    assert r["movidos"] == 1
    alvo, de, para = cenario["drive"].movimentos[0]
    assert alvo == "arquivo-solto-na-raiz"
    assert de == RAIZ and para == "id-escpe18---escola-planalto"
    assert cenario["drive"].apagados == [], "reorganizar nunca apaga"


# ---------------------------------------------------------------------------
# 5. A DATA CORRIGIDA ARRUMA O AVISO
# ---------------------------------------------------------------------------
def _arquivar(cenario, validade):
    return svc_arq.arquivar(
        cenario["s"], PDF, "certidao.pdf", tipo_codigo="CND-FEDERAL",
        empresa_id=cenario["empresa"].id, validade=validade,
        usuario=cenario["admin"])


def test_corrigir_a_validade_tira_o_documento_de_vencido(cenario):
    s = cenario["s"]
    doc = _arquivar(cenario, date.today() - timedelta(days=30))
    assert svc_arq.ler(s, doc)["situacao"] == "VENCIDO"

    svc_arq.corrigir_datas(s, doc.id, validade=(date.today() + timedelta(days=90)
                                                ).isoformat(),
                           usuario=cenario["admin"])
    assert svc_arq.ler(s, doc)["situacao"] != "VENCIDO"


def test_nao_deixa_apagar_a_validade_de_quem_vence(cenario):
    s = cenario["s"]
    doc = _arquivar(cenario, date.today() + timedelta(days=10))
    with pytest.raises(ErroValidacao) as e:
        svc_arq.corrigir_datas(s, doc.id, validade="", usuario=cenario["admin"])
    assert "vence" in str(e.value)


def test_validade_antes_da_emissao_e_recusada(cenario):
    s = cenario["s"]
    doc = _arquivar(cenario, date.today() + timedelta(days=10))
    with pytest.raises(ErroValidacao):
        svc_arq.corrigir_datas(s, doc.id, emissao="2026-05-10",
                               validade="2026-01-10", usuario=cenario["admin"])


def test_a_correcao_fica_na_trilha_com_o_antes_e_o_depois(cenario):
    from app.apps.erp.db.models.financeiro import Evento
    s = cenario["s"]
    doc = _arquivar(cenario, date.today() - timedelta(days=5))
    svc_arq.corrigir_datas(s, doc.id, validade="2027-01-31",
                           usuario=cenario["admin"])
    ev = s.query(Evento).filter_by(acao="DOCUMENTO_DATAS_CORRIGIDAS").first()
    assert ev is not None
    assert ev.detalhe["depois"]["validade"] == "2027-01-31"
    assert ev.detalhe["antes"]["validade"] != "2027-01-31"
